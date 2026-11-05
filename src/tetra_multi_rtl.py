#!/usr/bin/env python3
import os
import re
import gc
import sys
import math
import cmath
import ctypes
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


def _load_librtlsdr():
    for lib_name in ("librtlsdr.so.0", "librtlsdr.so"):
        try:
            return ctypes.CDLL(lib_name)
        except Exception:
            continue
    return None


def resolve_osmosdr_device_args(raw_device_spec, explicit_serial=""):
    """
    Resolve an RTL-SDR device index, serial number, or full gr-osmosdr string
    (e.g. '0', '1', '00000222', 'rtl=0,serial=00000222') into a deterministic
    osmosdr.source device argument string locked to the target dongle's serial
    (when available) or index.
    """
    spec = str(raw_device_spec or "0").strip()
    serial_cfg = str(explicit_serial or "").strip()

    parsed_index = None
    parsed_serial = serial_cfg if serial_cfg else None

    if "serial=" in spec.lower() or "rtl=" in spec.lower():
        m_ser = re.search(r"serial=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
        if m_ser and not parsed_serial:
            parsed_serial = m_ser.group(1).strip()
        m_rtl = re.search(r"rtl=([A-Za-z0-9_-]+)", spec, re.IGNORECASE)
        if m_rtl:
            rtl_tok = m_rtl.group(1).strip()
            if len(rtl_tok) >= 4 or not rtl_tok.isdigit():
                if not parsed_serial:
                    parsed_serial = rtl_tok
            else:
                parsed_index = int(rtl_tok)
    elif len(spec) >= 4 or not spec.isdigit():
        if not parsed_serial:
            parsed_serial = spec
    else:
        parsed_index = int(spec)

    rtlsdr = _load_librtlsdr()
    resolved_index = parsed_index
    resolved_serial = parsed_serial

    if rtlsdr is not None:
        try:
            dev_count = int(rtlsdr.rtlsdr_get_device_count())
        except Exception:
            dev_count = 0

        if resolved_serial and dev_count > 0:
            try:
                idx = int(
                    rtlsdr.rtlsdr_get_index_by_serial(
                        resolved_serial.encode("utf-8")
                    )
                )
                if idx >= 0:
                    resolved_index = idx
                else:
                    print(
                        f"[gnuradio] Warning: serial '{resolved_serial}' not found by rtlsdr_get_index_by_serial (rc={idx}), scanning {dev_count} device(s)...",
                        flush=True,
                    )
            except Exception:
                pass

        if resolved_index is not None and 0 <= resolved_index < dev_count:
            try:
                mfg = ctypes.create_string_buffer(256)
                prod = ctypes.create_string_buffer(256)
                ser = ctypes.create_string_buffer(256)
                rc = int(
                    rtlsdr.rtlsdr_get_device_usb_strings(
                        int(resolved_index), mfg, prod, ser
                    )
                )
                if rc == 0:
                    hw_serial = ser.value.decode("utf-8", errors="ignore").strip()
                    if hw_serial and not resolved_serial:
                        resolved_serial = hw_serial
            except Exception:
                pass

    # In gr-osmosdr's rtl_source_c.cc, dict["rtl"] is first passed to
    # rtlsdr_get_index_by_serial(val), and if that fails, lexical_cast<uint>(val).
    # Passing the resolved serial (when non-empty and >= 3 chars) or index locks
    # gr-osmosdr deterministically to the target physical stick.
    if resolved_serial and len(resolved_serial) >= 3:
        rtl_selector = resolved_serial
    elif resolved_index is not None:
        rtl_selector = str(resolved_index)
    else:
        rtl_selector = "0"

    # Use 32 USB bulk buffers of 131072 bytes (128 KB = ~32.8 ms per transfer at 2 MS/s,
    # >1.0s total ring buffer) to prevent USB micro-drops that break TDMA/DQPSK sync.
    dev_args = f"numchan=1 rtl={rtl_selector},buffers=32,buflen=131072"
    return dev_args, resolved_index, resolved_serial


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

        # TETRA pi/4-DQPSK (18 ksym/s, alpha=0.35) occupies +/-12.15 kHz.
        # Passband +/-13.5 kHz accommodates +/-1.35 kHz frequency offset while
        # rejecting adjacent 25 kHz channels and out-of-band noise before AGC/FLL.
        taps = firdes.low_pass(1.0, samp_rate, 13500, 3500, firdes.WIN_HAMMING)
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

        raw_device_spec = os.environ.get("RTL_DEVICE_INDEX", "0").strip()
        explicit_serial = os.environ.get("RTL_SERIAL", "").strip()
        gain = float(os.environ.get("RTL_GAIN", "24.0"))
        ppm = int(float(os.environ.get("RTL_PPM", "0")))
        center_freq = int(float(os.environ.get("CENTER_FREQ", "427750000")))
        channels_str = os.environ.get(
            "TETRA_CHANNELS",
            "427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7",
        )

        samp_rate = 2000000
        dev_args, resolved_idx, resolved_ser = resolve_osmosdr_device_args(
            raw_device_spec, explicit_serial
        )

        print(
            f"[gnuradio] Initializing RTL-SDR args='{dev_args}' "
            f"(resolved_index={resolved_idx}, serial={resolved_ser or 'auto'}) "
            f"center={center_freq} Hz rate={samp_rate} S/s gain={gain} dB ppm={ppm}",
            flush=True,
        )

        # 1. Open osmosdr source and allow USB control pipe + I2C repeater to settle
        self.src = osmosdr.source(args=dev_args)
        time.sleep(0.15)

        # 2. Configure baseband sample rate first and let RTL2832U DDC/PLL clock settle
        self.src.set_sample_rate(samp_rate)
        time.sleep(0.10)

        # 3. Tune center frequency and apply PPM correction so librtlsdr applies
        #    PPM to both sample rate and active LO frequency in Scanner and Static modes
        self.src.set_center_freq(center_freq, 0)
        time.sleep(0.10)
        self.src.set_freq_corr(ppm, 0)
        time.sleep(0.05)

        # 4. Disable software DC offset / IQ balance overrides
        self.src.set_dc_offset_mode(0, 0)
        self.src.set_iq_balance_mode(0, 0)

        # 5. Balanced R820T/R828D gain staging:
        #    Set IF/BB VGA stage (16 dB) BEFORE set_gain(gain, 0) so the baseband I/Q
        #    level entering freq_xlating_fir_filter has full dynamic range without
        #    overwriting the calibrated LNA/Mixer gain table.
        self.src.set_gain_mode(False, 0)
        time.sleep(0.05)
        self.src.set_if_gain(16, 0)
        self.src.set_bb_gain(16, 0)
        time.sleep(0.05)
        self.src.set_gain(gain, 0)
        time.sleep(0.10)

        actual_freq = int(round(self.src.get_center_freq(0)))
        actual_gain = self.src.get_gain(0)
        print(
            f"[gnuradio] R82XX Tuner locked: freq={actual_freq} Hz ({actual_freq / 1e6:.4f} MHz), "
            f"gain={actual_gain:.1f} dB (requested {gain:.1f} dB, IF=16 dB), ppm={ppm}",
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

