# TETRA Scanner (MCC 240 / MNC 11)

Realtidsavkodare för 6 parallella TETRA-basstationer (nätverk MCC 240 / MNC 11 inom bandet 427.000-428.500 MHz) med en enda RTL-SDR-mottagare.

Version (CalVer): `2026.11.03.2`

---

## Arkitektur

Systemet fångar upp ett 2.0 MS/s IQ-spektrum runt `427.750 MHz` och kanaliserar 6 TETRA-bärvågor parallellt. Röstpaket (`UPLANE` / `TCH_S`) samt periodiskt bakgrundsbrus (`D-NWRK-BROADCAST`, `SYSINFO`, `D-CK CHANGE DEMAND`) filtreras bort direkt. Enbart datatrafik och signalering parsas och distribueras:

1. **GNU Radio (`src/tetra_multi_rtl.py`)**: Läser 2.0 MS/s från RTL-SDR och demodulerar 6 parallella pi/4-DQPSK-kanaler till individuella lokala UDP-portar (`42001..42006`).
2. **tetra-kit (`decoder`)**: En instans per kanal avkodar de råa symbolerna och skickar TETRA-protokollramar som JSON till lokala UDP-portar (`42101..42106`).
3. **TETRA Parser (`src/tetra_parser.py`)**:
   - Slår samman tvådelade `D-SDS-DATA`-paket (metadata + hex-payload inom 2.0 sekunder).
   - Avkodar **Protocol 10 (0x0A, LIP GPS)** Short Location Report (25-bit signed lon, 24-bit signed lat) till decimalkoordinater.
   - Avkodar **Protocol 130 (0x82, SDS-TL Text)** (exempelvis TrionaGateway-meddelanden).
   - Avkodar **Protocol 245 (0xF5, Fordonstelemetri/AVL)** och separerar ASCII-prefix (t.ex. `666:D`) från binär svans.
   - Distribuerar varje unik händelse via **MQTT** (med Home Assistant Auto-Discovery), **UDP JSON Stream** samt lokal **JSONL-loggfil** (`/app/logs/tetra_events.jsonl`).

---

## Frekvensplan (MCC 240 / MNC 11)

Standardkonfigurationen övervakar följande 6 basstationer (Location Areas) med `CENTER_FREQ=427750000`:

| Kanal # | Frekvens (MHz) | Frekvens (Hz) | Location Area (LA) | Intern UDP (Demod -> Decoder -> Parser) |
|---------|----------------|---------------|--------------------|-----------------------------------------|
| 1       | 427.0000       | 427000000     | LA 71              | 42001 -> 42101                          |
| 2       | 427.3000       | 427300000     | LA 17              | 42002 -> 42102                          |
| 3       | 427.5500       | 427550000     | LA 23              | 42003 -> 42103                          |
| 4       | 427.8500       | 427850000     | LA 1               | 42004 -> 42104                          |
| 5       | 427.9500       | 427950000     | LA 16              | 42005 -> 42105                          |
| 6       | 428.5000       | 428500000     | LA 7               | 42006 -> 42106                          |

---

## SSI-nummerserier och Trafiktyper

| Kategorier / Protokoll | Typisk användning i nätet (240-11) |
|------------------------|------------------------------------|
| **LIP GPS (`0x0A`)**   | Fordons- och enhetspositionering (Short Location Report med latitud/longitud). |
| **SDS-TL (`0x82`)**    | Textmeddelanden och TrionaGateway-status. |
| **AVL (`0xF5`)**       | Fordonstelemetri med linje/fordonsprefix (t.ex. `666:D`) följt av binär telemetrisvans. |
| **D-STATUS**           | Statuskod `65024` = Heartbeat / ACK, Statuskod `0` = NÖDLARM / Emergency. |
| **CALL / ROAMING**     | Samtalsuppkoppling (`D-SETUP`, `D-TX GRANTED`, `D-RELEASE`) och cellbyte (`D-LOCATION UPDATE` med SSI/GSSI). |

---

## MQTT Topics och Home Assistant Auto-Discovery

När `MQTT_ENABLED=true` publiceras data under `MQTT_BASE_TOPIC` (standard `tetra/240_11`):

- `tetra/240_11/status` - `online` / `offline` (LWT, retained)
- `tetra/240_11/event` - Alla händelser i realtid (ej retained)
- `tetra/240_11/SDS` - Senaste SDS-meddelande (retained)
- `tetra/240_11/CALL` - Senaste samtalsmetadata (retained)
- `tetra/240_11/STATUS` - Senaste statuskod (retained)
- `tetra/240_11/ROAMING` - Senaste Location Update (retained)
- `tetra/240_11/heartbeat` - 60-sekunders kanalstatistik (retained)

Vid `HA_DISCOVERY_ENABLED=true` skapas automatiskt enheten **TETRA Scanner (240-11)** i Home Assistant med 5 sensorer (`last_sds`, `last_call`, `last_status`, `last_roaming`, `packets_per_min`).

### Exempel på JSON-payload (SDS LIP GPS)

```json
{
  "timestamp": "2026-11-03T20:15:30.123456+00:00",
  "host": "iridium",
  "category": "SDS",
  "service": "MLE",
  "pdu": "D-SDS-DATA",
  "freq_mhz": "427.8500",
  "la": 1,
  "ssi": 100234,
  "dest_ssi": 900100,
  "protocol_id": 10,
  "message_reference": 42,
  "sds_pdu": "SDS-TRANSFER",
  "sds_type": "LIP_GPS",
  "sds_text": "GPS 59.329323, 18.068581",
  "sds_hex": "0a01100f1a2b3c4d",
  "sds_binary_tail": null,
  "gps_lat": 59.329323,
  "gps_lon": 18.068581
}
```

### Exempel på JSON-payload (SDS AVL Telemetri `0xF5`)

```json
{
  "timestamp": "2026-11-03T20:16:02.654321+00:00",
  "host": "iridium",
  "category": "SDS",
  "service": "MLE",
  "pdu": "D-SDS-DATA",
  "freq_mhz": "427.0000",
  "la": 71,
  "ssi": 204512,
  "dest_ssi": 900200,
  "protocol_id": 245,
  "message_reference": 19,
  "sds_pdu": "SDS-TRANSFER",
  "sds_type": "AVL_TELEMETRY",
  "sds_text": "666:D",
  "sds_hex": "f501003636363a440a01020304",
  "sds_binary_tail": "01020304",
  "gps_lat": null,
  "gps_lon": null
}
```

---

## UDP JSON Stream

För extern realtidsmatning av händelser via UDP, aktivera i `.env`:

```ini
UDP_OUT_ENABLED=true
UDP_OUT_TARGETS=192.168.1.10:5555,192.168.1.20:5555
UDP_OUT_INCLUDE_RAW=false
```

Varje händelse samt 60-sekunders heartbeat skickas som ett enradigt JSON-datagram via UDP till samtliga konfigurerade mottagare.

---

## Installation och Drift

All kompilering och allt Docker-byggande sker automatiskt i molnet via GitHub Actions vid push till GitHub och publiceras till `ghcr.io/minglarn/tetra_scanner:latest`.

På målservern:

```bash
git clone https://github.com/Minglarn/tetra_scanner.git
cd tetra_scanner
cp .env.example .env
nano .env

docker compose pull && docker compose up -d
```

För att följa loggarna i realtid:

```bash
docker compose logs -f
```

