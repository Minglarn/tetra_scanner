#!/usr/bin/env python3
import os
import re
import sys
import math
import cmath
import signal
import threading
from gnuradio import gr
from gnuradio import filter
from gnuradio import analog
from gnuradio import digital
from gnuradio import blocks
from gnuradio.filter import firdes
import osmosdr


def parse_rtl_id(raw_spec, explicit_serial=""):
    spec = str(raw_spec if raw_spec is not None else "0").strip()
    ser = str(explicit_serial or "").strip()
    if ser:
        return ser
    if "serial=" in spec.lower():
        m = re.search(r"serial=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    if "rtl=" in spec.lower():
        m = re.search(r"rtl=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return spec or "0"


class tetra_chan(gr.hier_block2):
    def __init__(self, samp_rate, center_freq, freq_hz, idx):
        gr.hier_block2.__init__(
            self,
            f"tetra_chan_{idx}",
            gr.io_signature(1, 1, gr.sizeof_gr_complex),
            gr.io_signature(0, 0, 0),
        )

        decim = 32
        channel_rate = 36000
        sps = 2

        taps = firdes.low_pass(1.0, samp_rate, 18000, 5000, firdes.WIN_HAMMING)
        self.xlating = filter.freq_xlating_fir_filter_ccc(
            decim, taps, freq_hz - center_freq, samp_rate
        )
        self.resamp = filter.mmse_resampler_cc(
            0, (float(samp_rate) / float(decim)) / float(channel_rate)
        )
        self.agc = analog.feedforward_agc_cc(16, 1.0)
        self.fll = digital.fll_band_edge_cc(sps, 0.35, 45, math.pi / 100.0)

        rrc_taps = firdes.root_raised_cosine(32, 32 * sps, 1.0, 0.35, 11 * sps * 32)
        self.pfb = digital.pfb_clock_sync_ccf(
            sps, 2 * math.pi / 100.0, rrc_taps, 32, 16, 1.5, 1
        )
        self.diff = digital.diff_phasor_cc()

        self.const = digital.constellation_calcdist(
            [
                cmath.exp(1j * math.pi / 4),
                cmath.exp(3j * math.pi / 4),
                cmath.exp(-1j * math.pi / 4),
                cmath.exp(-3j * math.pi / 4),
            ],
            [0, 1, 2, 3],
            4,
            1,
        ).base()

        self.dec = digital.constellation_decoder_cb(self.const)
        self.map_block = digital.map_bb([0, 1, 2, 3])
        self.unpack = blocks.unpack_k_bits_bb(2)
        self.udp = blocks.udp_sink(
            gr.sizeof_char * 1, "127.0.0.1", 42000 + idx, 1472, True
        )

        self.connect(
            self,
            self.xlating,
            self.resamp,
            self.agc,
            self.fll,
            self.pfb,
            self.diff,
            self.dec,
            self.map_block,
            self.unpack,
            self.udp,
        )


class tetra_multi_rtl(gr.top_block):
    def __init__(self):
        gr.top_block.__init__(self, "TETRA Multi-Channel Receiver")

        rtl_id = parse_rtl_id(
            os.environ.get("RTL_DEVICE_INDEX", "0"),
            os.environ.get("RTL_SERIAL", ""),
        )
        gain = float(os.environ.get("RTL_GAIN", "24.0"))
        ppm = int(float(os.environ.get("RTL_PPM", "0")))
        center_freq = int(float(os.environ.get("CENTER_FREQ", "427750000")))
        channels_str = os.environ.get(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        )

        samp_rate = 2000000

        print(
            f"[gnuradio] Initializing RTL-SDR rtl={rtl_id} "
            f"center={center_freq} Hz rate={samp_rate} S/s gain={gain} dB ppm={ppm}",
            flush=True,
        )

        self.src = osmosdr.source(args=f"numchan=1 rtl={rtl_id}")
        self.src.set_sample_rate(samp_rate)
        self.src.set_center_freq(center_freq, 0)
        self.src.set_freq_corr(ppm, 0)
        self.src.set_dc_offset_mode(0, 0)
        self.src.set_iq_balance_mode(0, 0)
        self.src.set_gain_mode(False, 0)
        self.src.set_gain(gain, 0)

        self.channels = []
        raw_entries = [c.strip() for c in channels_str.split(",") if c.strip()]
        for idx, entry in enumerate(raw_entries, start=1):
            parts = entry.split(":")
            freq_hz = int(parts[0])
            la_label = parts[1] if len(parts) > 1 else "?"
            print(
                f"[gnuradio] Channel #{idx}: {freq_hz / 1e6:.4f} MHz (LA {la_label}) -> UDP 127.0.0.1:{42000 + idx}",
                flush=True,
            )
            chan_block = tetra_chan(samp_rate, center_freq, freq_hz, idx)
            self.channels.append(chan_block)
            self.connect(self.src, chan_block)


def main():
    stop_event = threading.Event()
    tb = tetra_multi_rtl()

    def sig_handler(signum, frame):
        if not stop_event.is_set():
            print("[gnuradio] Stopping flowgraph...", flush=True)
            stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    tb.start()
    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        tb.stop()
        tb.wait()


if __name__ == "__main__":
    main()
