# Analys av cosmo

En granskning av repot med tre frågeställningar: vad det gör, hur det kan användas på andra sätt, och vad som kan komplettera det.

---

## 1. Vad gör cosmo?

Cosmo är ett **AI-drivet dataanalys- och visualiseringsverktyg** där två LLM-agenter autonomt analyserar, debatterar och visualiserar godtycklig CSV/JSON-data i realtid. Huvudkonceptet är att två modeller med olika personligheter ("teman") ifrågasätter varandras slutsatser och manipulerar en levande GPU-accelererad graf samt inline-diagram för att bevisa sina poänger.

### Arkitektur i korthet

| Lager | Teknik | Nyckelfiler |
|-------|--------|-------------|
| Frontend | Vite 6 + Cosmograph 2.0 (WebGL + DuckDB-WASM) + Chart.js 4 + marked.js | `index.html`, `vite.config.js` |
| Backend | FastAPI + Pandas/NumPy på Python 3.11+ | `backend/server.py` (multi-agent-loop, endpoints), `backend/analyzer.py` (Pandas-motor, plugins, multi-dataset) |
| AI | OpenRouter: Claude Sonnet 4.6 (analyst) + Gemini 3.1 Pro (companion) | `MODEL_ANALYST`, `MODEL_COMPANION` i `backend/server.py` |
| Extra | Filesystem-scanner som producerar graf-data | `scan.js` → `data/filesystem.json` |

### Huvudflöde

1. Användaren drar in en CSV/JSON-fil → `/api/upload`.
2. Varje rad blir en nod i Cosmograph; schemat autodetekteras.
3. Användaren ställer en fråga; `/api/chat` triggar analyst-agenten.
4. Analyst skriver egen Pandas-kod via verktyg, skapar Chart.js-diagram inline och styr grafen (zoom, filter, select).
5. Om "Companion" är aktiverad tar en andra agent vid, utmanar eller fördjupar — de växlar i upp till 4 rundor.
6. Dashboard- och report-tabbarna genererar KPI-widgets respektive en Markdown-rapport från hela sessionen.

### Agent-verktyg

`query_dataframe`, `run_pandas_code`, `create_chart`, `control_graph`, `add_column`, `create_links`, `find_connections`, `join_datasets`, `save_plugin`, `run_plugin`, `list_plugins`, `list_datasets`, `save_snapshot`, `data_quality_report`.

### Companion-teman

`Default`, `Devil's Advocate`, `Anomaly Hunter`, `Optimizer`, `Connector`, `Storyteller` — varje tema ger genuint olika analyser av samma data.

### Det som gör cosmo distinkt

- **Multi-agent-debatt** som standard, inte som experimentell feature.
- **Självbyggande analytik**: agenten skriver Pandas/NumPy på flygande fot och kan spara resultaten som återanvändbara plugins.
- **Grafen är ett instrument, inte dekoration** — AI:n navigerar den aktivt som en del av svaret.
- **Full transparens**: varje verktygsanrop och Pandas-snutt visas ovanför AI-svaret.

### Referenser

`README.md`, `ABOUT.md`, `backend/server.py`, `backend/analyzer.py`, `index.html`, `scan.js`.

---

## 2. Tre andra användningsområden

### A. Säkerhets- och loggforensik-explorer

Mata in auth-loggar, netflow, audit trails eller SIEM-export som CSV.

- **Anomaly Hunter**-teman plockar upp ovanliga tider, IP-adresser och användarbeteenden.
- **Connector**-temat bygger grafkanter mellan entiteter (user ↔ host ↔ resurs) med `create_links`, så mönster som lateral movement, brute-force-kluster eller privilege escalation blir visuellt uppenbara i Cosmograph.
- **Plugin-systemet** låter SOC-teamet lagra återanvändbara detektorer (`impossible_travel`, `credential_spray`, `new_admin_baseline`) som körs på nästa incident.
- **Snapshots** fungerar som bevissäkring av ett visst utredningstillstånd.

### B. FinOps- och molnkostnadsanalys

Ladda in exporter från AWS Cost Explorer, GCP Billing, Azure Cost Management eller Kubecost.

- **Optimizer**-temat är i princip skräddarsytt för "var kan vi skära?".
- **Cross-dataset-join** (`find_connections`, `join_datasets`) knyter ihop kostnadsposter med taggar, team, projekt och lastdata.
- **Grafen** visualiserar vilka workloads/tjänster som står för lejonparten av kostnaden; `zoom_to` flyger kameran till de dyraste klustren.
- **Dashboard-tabben** blir en auto-genererad månadsrapport med KPI:er (spend delta, top cost centers, unused reservations).

### C. Kvalitativ/kvantitativ forskning och enkätanalys

Passar användarintervjuer kodade som CSV, likert-skalor, A/B-testdata eller akademiska datamängder.

- **Storyteller + Devil's Advocate** i tandem ger både narrativ och stresstest — reducerar enskild-AI-bias vid hypotesgenerering.
- **Snapshots** + **report-tabben** blir metodologidokumentation: varje hypotes har ett spårbart tillstånd som kan citeras.
- **`data_quality_report`** före analys hjälper forskaren upptäcka systematiskt bortfall eller skevheter innan slutsatser dras.
- Passar särskilt bra i utforskningsfasen innan man commit:ar till en formell statistisk metod.

---

## 3. Kompletteringar att överväga

### Persistens och samarbete
- Persistent storage för snapshots, plugins och uppladdade dataset (idag allt in-memory i `analyzer.py`) — SQLite räcker långt, Postgres för flera användare.
- Multi-user-sessioner med delbara URL:er till ett specifikt debatt- eller snapshot-läge.
- Export av hela debatten som självstående PDF/HTML-rapport med inbäddade charts.

### Dataconnectors (utöver CSV/JSON)
- Parquet, Arrow, Excel och Google Sheets-import.
- Direktkopplingar mot BigQuery, Snowflake och Postgres så stora dataset inte behöver laddas som fil.
- S3/GCS-läsning för filer som överstiger browserminnet.
- DuckDB-vyer för streaming/near-realtime (DuckDB finns redan i Cosmograph-stacken).

### AI- och agentkapabilitet
- **Moderator-agent** som sammanfattar debatten och markerar konsensus vs. kvarstående oenighet.
- SQL-tool utöver Pandas (DuckDB är redan laddad i frontend).
- "Evidence-based"-läge med explicit hypotes → falsifieringsrunda.
- Lokal modell-backend (Ollama / llama.cpp) för känslig data som inte får lämna organisationen.
- Model routing per verktyg (billig liten modell för triviala queries, Opus för reasoning-tunga steg).

### Säkerhet och drift
- **Hårdare sandbox på `run_pandas_code`** — nuvarande blacklist bör ersättas med AST-whitelist eller körning i subprocess med resource limits. Detta är den viktigaste härdningen.
- Autentisering på API-endpoints (CORS står på `*` idag).
- Rate limiting och per-användare-hantering av OpenRouter-nycklar.
- Audit log på samtliga tool-calls.
- Dockerfile + docker-compose för enkel deploy.

### Visualisering och UX
- Geospatial layer (kartvy) för data med lat/lng-kolumner.
- Tidsserie-scrubbing i grafen (animera noder/kanter över tid).
- Inbäddade Jupyter-celler för power users som vill gå djupare än chatten tillåter.
- Mobile-responsiv layout.
- Tillgänglighet: tangentbordsnavigering, ARIA-attribut, färgblindsäkra paletter.

### Tester och kvalitet
- Enhetstester för `analyzer.py` — repot verkar sakna tester helt.
- Integrationstester som mockar OpenRouter för reproducerbara agent-flöden.
- CI-pipeline (GitHub Actions) med lint, typcheck och test.
- Typning med mypy/ruff för Python samt TypeScript för frontend.

---

## Sammanfattning

Cosmo är mer än ett "chat-med-din-CSV"-verktyg: kombinationen av **multi-agent-debatt**, **självbyggande Pandas-analytik**, **återanvändbara plugins** och en **levande GPU-graf** gör det till en generell explorationsplattform för godtyckliga tabulära data. De starkaste utvecklingsriktningarna är (i) persistens + samarbete, (ii) fler dataconnectors och (iii) hårdare sandboxning av kod-exekveringen — de tre tillsammans flyttar produkten från hackdemo till seriöst analysverktyg.
