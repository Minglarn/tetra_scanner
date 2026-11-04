# TETRA Scanner (MCC 240 / MNC 11 & Smart Auto-Tune Scanner)

Realtidsavkodare, nätverksskanner och WebUI för TETRA-basstationer med en enda RTL-SDR-mottagare. Systemet kan köras i två lägen:
- **Smart Auto-Tune Scanner (`MODE=scanner`)**: Gör ett högupplöst spektrumsvep (`2 kHz` steg, `25 kHz` PTS-raster), väljer ut det bästa 2 MHz-fönstret med flest och starkast TETRA-bärvågor, ställer in mottagaren och övervakar trafiken. Om ingen faktisk nyttolast (`SDS`, `CALL`, `STATUS`, `ROAMING`) avkodas inom `IDLE_TIMEOUT_SEC` (standard 900s / 15 min) startas ett nytt svep automatiskt.
- **Static Mode (`MODE=static`)**: Lyssnar kontinuerligt på upp till 6 fasta TETRA-bärvågor inom ett 2.0 MHz-fönster (standard: MCC 240 / MNC 11 på `427.000-428.500 MHz`).

Version (CalVer): `2026.11.04.7`

---

## Integrerat WebUI (FastAPI + WebSockets)

Vid uppstart startar containern automatiskt en FastAPI-server på port `WEB_PORT` (standard `8080`):
- **Dashboard (`http://<server-ip>:8080/`)**:
  - **Realtidsström via WebSockets (`/ws/events`)**: Tailar `/app/logs/tetra_events.jsonl` icke-blockerande och visar alla händelser direkt i webbläsaren (inklusive de 100 senaste händelserna vid sidladdning).
  - **Färgkodade kategorier**:
    - `[SDS]`: Blå badge/text (inklusive direktlänk till karta vid GPS-koordinater).
    - `[CALL]`: Grön badge/text (inklusive talgrupp, sändande SSI och taltid i sekunder).
    - `[ROAMING]`: Gul/Orange badge/text.
    - `[STATUS]`: Lila badge/text.
  - **Interaktiva filter**:
    - Kryssrutor för **Show TrionaGateway** och **Show Control Frames** som direkt visar/döljer valda meddelandetyper i DOM:en.
    - Fritextsökning på SSI, GSSI, frekvens eller meddelandetext samt **Auto-Scroll**.
  - **Discovered Networks (`/api/networks`)**: Sidopanel som listar alla upptäckta TETRA-basstationer från `/app/logs/discovered_networks.jsonl` med frekvens, `MCC`, `MNC` och `Location Area (LA)`.

---

## Arkitektur

1. **GNU Radio (`src/tetra_multi_rtl.py`)**: Läser 2.0 MS/s från RTL-SDR och demodulerar upp till 6 parallella pi/4-DQPSK-kanaler till lokala UDP-portar (`42001..42006`).
2. **tetra-kit (`decoder`)**: En instans per kanal avkodar de råa symbolerna och skickar TETRA-protokollramar som JSON till lokala UDP-portar (`42101..42106`).
3. **TETRA Parser (`src/tetra_parser.py`)**:
   - Identifierar nätverk via `SYSINFO` och `D-NWRK-BROADCAST` (`MCC`, `MNC`, `Location Area`) och sparar unika nätverk till `/app/logs/discovered_networks.jsonl` samt MQTT `{MQTT_BASE_TOPIC}/NETWORK` (med 1 timmes dedupliceringsspärr).
   - Slår samman tvådelade `D-SDS-DATA`-paket (metadata + hex-payload inom 2.0 sekunder).
   - Avkodar **Protocol 10 (`0x0A`, LIP GPS)**, **NMEA GPS (`$GPRMC`, `$GPGGA`, m.fl.)** samt dolda koordinater i binära hex-strängar.
   - Avkodar **Protocol 130 (`0x82`, SDS-TL Text)** inklusive **TrionaGateway (`*901*TrionaGateway*...#`)** och **Tåg-/Omloppssynkronisering (`A_A_,3670205134,4515134-1-240,2`)**.
   - Avkodar **Protocol 245 (`0xF5`, Fordonstelemetri/AVL)** och separerar ASCII-prefix från binär svans.
   - Håller ett enhetsregister i minnet (`unit_registry`) som kopplar ihop radio-SSI med aktivt tåg-/linjenummer, körriktning och hårdvaru-MAC.
4. **Smart Auto-Tune Orchestrator (`src/scanner_orchestrator.py`)**:
   - **Fas 1 (SWEEP)**: Sveper `SCAN_START_MHZ` till `SCAN_END_MHZ` i 2 MHz-block med `rtl_power` (`2 kHz` steg, `10s` integrering), hittar lokala spektrumtoppar minst `+3 dB` över brusgolvet och snäpper dem till **25 kHz PTS-raster**.
   - **Fas 2 (TOP LIST)**: Väljer ut det 2 MHz-fönster som rymmer flest och starkast TETRA-bärvågor (max 6 st inom `CENTER_FREQ +/- 1 MHz`) och skriver ut en topplista i loggen.
   - **Fas 3 (MONITOR & TIMEOUT)**: Övervakar det valda fönstret i realtid. Om ingen nyttolast (`[SDS]`, `[CALL]`, `[STATUS]`, `[ROAMING]`) ses inom `IDLE_TIMEOUT_SEC` startas ett nytt svep.
5. **FastAPI WebUI (`src/web_api.py` & `src/templates/index.html`)**: Serverar realtidspanelen och REST/WebSocket-API:erna på port `8080`.

---

## Konfiguration (`.env`)

```ini
MODE=scanner
SCAN_START_MHZ=420
SCAN_END_MHZ=430
IDLE_TIMEOUT_SEC=900

WEB_PORT=8080

RTL_DEVICE_INDEX=1
RTL_GAIN=24.0
RTL_PPM=0
CENTER_FREQ=427750000
TETRA_CHANNELS=427000000:71,427300000:17,427550000:23,427850000:1,427950000:16,428500000:7
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

Öppna sedan WebUI i webbläsaren på:
`http://<server-ip>:8080`
