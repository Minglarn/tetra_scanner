# TETRA Scanner (MCC 240 / MNC 11 & Auto-Discovery)

Realtidsavkodare och frekvensskanner för TETRA-basstationer med en enda RTL-SDR-mottagare. Systemet kan köras i två lägen:
- **Static Mode (`MODE=static`)**: Lyssnar kontinuerligt på upp till 6 fasta TETRA-bärvågor inom ett 2.0 MHz-fönster (standard: MCC 240 / MNC 11 på `427.000-428.500 MHz`).
- **Auto-Discovery Scanner Mode (`MODE=scanner`)**: Sveper automatiskt valfritt frekvensband (t.ex. `410-430 MHz`) i 2 MHz-block, hittar aktiva TETRA-bärvågor och avkodar både nätverksidentitet (`MCC`, `MNC`, `LA`) och datatrafik.

Version (CalVer): `2026.11.04.3`

---

## Arkitektur

1. **GNU Radio (`src/tetra_multi_rtl.py`)**: Läser 2.0 MS/s från RTL-SDR och demodulerar upp till 6 parallella pi/4-DQPSK-kanaler till lokala UDP-portar (`42001..42006`).
2. **tetra-kit (`decoder`)**: En instans per kanal avkodar de råa symbolerna och skickar TETRA-protokollramar som JSON till lokala UDP-portar (`42101..42106`).
3. **TETRA Parser (`src/tetra_parser.py`)**:
   - Identifierar nätverk via `SYSINFO` och `D-NWRK-BROADCAST` (`MCC`, `MNC`, `Location Area`) och sparar unika nätverk till `/app/logs/discovered_networks.jsonl` samt MQTT `{MQTT_BASE_TOPIC}/NETWORK` (med 1 timmes dedupliceringsspärr per nätverk).
   - Slår samman tvådelade `D-SDS-DATA`-paket (metadata + hex-payload inom 2.0 sekunder).
   - Avkodar **Protocol 10 (`0x0A`, LIP GPS)**, **NMEA GPS (`$GPRMC`, `$GPGGA`, m.fl.)** samt dolda koordinater i binära hex-strängar.
   - Avkodar **Protocol 130 (`0x82`, SDS-TL Text)** inklusive **TrionaGateway (`*901*TrionaGateway*...#`)** och **Tåg-/Omloppssynkronisering (`A_A_,3670205134,4515134-1-240,2`)**.
   - Avkodar **Protocol 245 (`0xF5`, Fordonstelemetri/AVL)** och separerar ASCII-prefix från binär svans.
   - Håller ett enhetsregister i minnet (`unit_registry`) som kopplar ihop radio-SSI med aktivt tåg-/linjenummer, körriktning och hårdvaru-MAC.
   - Distribuerar varje unik händelse med läsbar `human_text` via **MQTT** (med Home Assistant Auto-Discovery), **UDP JSON Stream** samt lokal **JSONL-loggfil** (`/app/logs/tetra_events.jsonl`).
4. **Scanner Orchestrator (`src/scanner_orchestrator.py`)**: Styr spektrumsvep (`rtl_power`) och processlivscykel när `MODE=scanner` är valt.

---

## Auto-Discovery Scanner (`MODE=scanner`)

När `MODE=scanner` anges i `.env` startar `entrypoint.sh` automatiskt `src/scanner_orchestrator.py` istället för den statiska kanallistan.

### Hur svepet fungerar steg för steg
1. **Indelning i 2 MHz-block**: Bandet mellan `SCAN_START_MHZ` (t.ex. `410`) och `SCAN_END_MHZ` (t.ex. `430`) delas upp i 2 MHz-block med centerfrekvenser på `411 MHz`, `413 MHz`, `415 MHz` osv.
2. **Snabbt spektrumsvep (`rtl_power`)**: För varje 2 MHz-block körs `rtl_power` i 5 sekunder med `12.5 kHz` upplösning till `/tmp/scan.csv`.
3. **Toppdetektering**:
   - Beräknar brusgolvet (medianvärdet i dBm över 2 MHz-blocket).
   - Filtrerar bort DC-spiken i mitten av svepet (`+-25 kHz` runt centerfrekvensen) samt bandkanterna.
   - Väljer ut de **upp till 6 starkaste bärvågorna** som ligger minst `6 dB` över brusgolvet och är separerade med minst `25 kHz`.
   - Om inga aktiva bärvågor hittas i blocket hoppar skannern direkt vidare till nästa 2 MHz-block utan fördröjning.
4. **Avkodning under `SCAN_DWELL_TIME`**:
   - Om bärvågor hittas startas `tetra-kit/decoder`, `tetra_multi_rtl.py` och `tetra_parser.py` dynamiskt för just dessa frekvenser under `SCAN_DWELL_TIME` sekunder (standard `120` sekunder).
   - Alla upptäckta nätverk (`MCC`, `MNC`, `LA`) loggas i `/app/logs/discovered_networks.jsonl` och publiceras på `{MQTT_BASE_TOPIC}/NETWORK`. All samtidig trafik (`SDS`, `CALL`, `STATUS`, `ROAMING`) loggas precis som vanligt i `/app/logs/tetra_events.jsonl`.
5. **Ren nedstängning och rotation**: Efter `SCAN_DWELL_TIME` avslutas underprocesserna rent och skannern går vidare till nästa 2 MHz-block i en evig loop.

### Konfiguration i `.env` för Auto-Discovery

```ini
MODE=scanner
SCAN_START_MHZ=410
SCAN_END_MHZ=430
SCAN_DWELL_TIME=120
```

### Exempel på JSON-payload i `/app/logs/discovered_networks.jsonl` och `{MQTT_BASE_TOPIC}/NETWORK`

```json
{
  "timestamp": "2026-11-04T04:50:12.104512+00:00",
  "seen_epoch": 1793767812.105,
  "host": "iridium",
  "category": "NETWORK",
  "pdu": "SYSINFO",
  "network_id": "427.8500_240_11_1",
  "freq_mhz": "427.8500",
  "freq_hz": 427850000,
  "mcc": 240,
  "mnc": 11,
  "la": 1,
  "dl_freq": 427850000,
  "ul_freq": 417850000,
  "human_text": "Discovered TETRA Network on 427.8500 MHz: MCC=240 MNC=11 LA=1 (via SYSINFO)"
}
```

---

## Statisk Frekvensplan (MCC 240 / MNC 11 vid `MODE=static`)

Standardkonfigurationen i statiskt läge övervakar följande 6 basstationer (Location Areas) med `CENTER_FREQ=427750000`:

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

| Kategorier / Protokoll | Typisk användning i nätet |
|------------------------|---------------------------|
| **SSI `1-999` (t.ex. `590`)** | Central gateway / Trafikledning / Dispatchserver. |
| **GSSI `3000000-3999999`** | Talgrupper (Talkgroups) vid gruppsamtal (`D-SETUP`, `D-TX GRANTED`, `D-TX CEASED`, `D-RELEASE`). |
| **ISSI `4500000-4599999`** | Fordons- och tågradioenheter. |
| **Train/Route Sync (`A_A_,...`)** | Kopplar tåg-/fordonsradio till aktuellt tåg-/linjenummer och körriktning (t.ex. `4515134-1-240` = Tåg 240, riktning 1). |
| **TrionaGateway (`*901*...#`)** | Handskakning och konfiguration mellan Triona-gateway och fordonsenhetens hårdvaru-ID/MAC. |
| **LIP GPS (`0x0A`) & NMEA** | Fordons- och enhetspositionering (`$GPRMC`, `$GPGGA`, Short Location Report). |
| **AVL (`0xF5`)**       | Fordonstelemetri med linje-/fordonsprefix (t.ex. `666:D`) följt av binär telemetrisvans. |
| **D-STATUS**           | Statuskod `65024` = Heartbeat / ACK, Statuskod `0` = NÖDLARM / Emergency, `1..12` = Driftstatus. |

---

## MQTT Topics och Home Assistant Auto-Discovery

När `MQTT_ENABLED=true` publiceras data under `MQTT_BASE_TOPIC` (standard `tetra/240_11`):

- `tetra/240_11/status` - `online` / `offline` (LWT, retained)
- `tetra/240_11/event` - Alla händelser i realtid (ej retained)
- `tetra/240_11/NETWORK` - Senast upptäckta TETRA-nätverk med MCC/MNC/LA (retained)
- `tetra/240_11/SDS` - Senaste SDS-meddelande (retained)
- `tetra/240_11/CALL` - Senaste samtalsmetadata inkl. taltid i sekunder (retained)
- `tetra/240_11/STATUS` - Senaste statuskod (retained)
- `tetra/240_11/ROAMING` - Senaste Location Update (retained)
- `tetra/240_11/heartbeat` - 60-sekunders kanalstatistik (retained)

Vid `HA_DISCOVERY_ENABLED=true` skapas automatiskt enheten **TETRA Scanner (240-11)** i Home Assistant med 5 sensorer (`last_sds`, `last_call`, `last_status`, `last_roaming`, `packets_per_min`).

---

## UDP JSON Stream

För extern realtidsmatning av händelser via UDP, aktivera i `.env`:

```ini
UDP_OUT_ENABLED=true
UDP_OUT_TARGETS=192.168.1.10:5555,192.168.1.20:5555
UDP_OUT_INCLUDE_RAW=false
```

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
