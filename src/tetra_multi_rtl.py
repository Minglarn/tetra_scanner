#!/usr/bin/env python3
import os
import re
import gc
import sys
import math
import cmath
import signal
import time
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
    candidate = ser
    if not candidate:
        if "serial=" in spec.lower():
            m = re.search(r"serial=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
            if m:
                candidate = m.group(1).strip()
        elif "rtl=" in spec.lower():
            m = re.search(r"rtl=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
            if m:
                candidate = m.group(1).strip()
        else:
            candidate = spec or "0"

    if candidate.isdigit() and len(candidate) <= 2:
        return candidate

    try:
        import ctypes

        rtlsdr = ctypes.CDLL("librtlsdr.so.0")
        rtlsdr.rtlsdr_get_index_by_serial.argtypes = [ctypes.c_char_p]
        rtlsdr.rtlsdr_get_index_by_serial.restype = ctypes.c_int
        idx = rtlsdr.rtlsdr_get_index_by_serial(candidate.encode("utf-8"))
        if idx >= 0:
            return str(idx)
    except Exception:
        pass

    if spec.isdigit() and len(spec) <= 2:
        return spec
    return "0"


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
        nfilts = 32

        taps = firdes.low_pass(1, samp_rate, 12500, 12500 * 0.2)
        self.xlating = filter.freq_xlating_fir_filter_ccc(
            decim, taps, freq_hz - center_freq, samp_rate
        )
        self.resamp = filter.mmse_resampler_cc(
            0, (float(samp_rate) / float(decim)) / float(channel_rate)
        )
        self.agc = analog.feedforward_agc_cc(8, 1)
        self.fll = digital.fll_band_edge_cc(sps, 0.35, 45, cmath.pi / 100.0)

        rrc_taps = firdes.root_raised_cosine(
            nfilts, nfilts, 1.0 / float(sps), 0.35, 11 * sps * nfilts
        )
        self.pfb = digital.pfb_clock_sync_ccf(
            sps, 2 * cmath.pi / 100.0, rrc_taps, nfilts, nfilts // 2, 1.5, sps
        )
        self.cma = digital.cma_equalizer_cc(15, 1, 10e-3, sps)
        self.diff = digital.diff_phasor_cc()

        self.constel = digital.constellation_dqpsk().base()
        self.constel.gen_soft_dec_lut(8)
        self.dec = digital.constellation_decoder_cb(self.constel)
        self.map_block = digital.map_bb(self.constel.pre_diff_code())
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
            self.cma,
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
        gain = float(os.environ.get("RTL_GAIN", "28.0"))
        ppm = int(float(os.environ.get("RTL_PPM", "0")))
        center_freq = int(float(os.environ.get("CENTER_FREQ", "427750000")))
        channels_str = os.environ.get(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        )

        self.center_freq = center_freq
        self.gain = gain
        self.ppm = ppm

        samp_rate = 2000000
        dev_args = f"numchan=1 rtl={rtl_id}"

        print(
            f"[gnuradio] Initializing RTL-SDR args='{dev_args}' "
            f"center={center_freq} Hz rate={samp_rate} S/s gain={gain} dB ppm={ppm}",
            flush=True,
        )

        self.src = osmosdr.source(args=dev_args)
        self.src.set_sample_rate(samp_rate)
        self.src.set_freq_corr(ppm, 0)
        self.src.set_dc_offset_mode(0, 0)
        self.src.set_iq_balance_mode(0, 0)
        self.src.set_gain_mode(False, 0)
        self.src.set_center_freq(center_freq, 0)
        self.src.set_gain(gain, 0)

        actual_freq = int(round(self.src.get_center_freq(0)))
        actual_gain = self.src.get_gain(0)
        actual_corr = self.src.get_freq_corr(0)
        print(
            f"[gnuradio] R82XX Tuner locked: freq={actual_freq} Hz ({actual_freq / 1e6:.4f} MHz), "
            f"gain={actual_gain:.1f} dB (requested {gain:.1f} dB), "
            f"ppm={actual_corr} (requested {ppm})",
            flush=True,
        )

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
            print(
                f"[gnuradio] Received signal {signum}, stopping flowgraph cleanly...",
                flush=True,
            )
            stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    tb.start()

    def _retune_once():
        time.sleep(1.0)
        if not stop_event.is_set():
            try:
                tb.src.set_center_freq(tb.center_freq, 0)
                tb.src.set_gain(tb.gain, 0)
                print(
                    f"[gnuradio] Post-start retune applied: freq={tb.center_freq} Hz ({tb.center_freq / 1e6:.4f} MHz), gain={tb.gain:.1f} dB",
                    flush=True,
                )
            except Exception as exc:
                print(f"[gnuradio] Post-start retune warning: {exc}", flush=True)

    threading.Thread(target=_retune_once, daemon=True).start()
    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        try:
            tb.stop()
            tb.wait()
        except Exception as exc:
            print(f"[gnuradio] Warning during tb.stop()/wait(): {exc}", flush=True)
        try:
            tb.disconnect_all()
            tb.channels.clear()
            tb.src = None
            del tb
            gc.collect()
            time.sleep(0.20)
        except Exception:
            pass
        print("[gnuradio] Flowgraph stopped and RTL-SDR USB handle released.", flush=True)


if __name__ == "__main__":
    main()
