# AI Smart Parser for Semiconductor Tool Logs

A hybrid deterministic + AI-assisted parser that converts heterogeneous semiconductor tool logs into:

- **machine-readable relational tables** in MySQL
- **human-readable outputs** such as processed JSON results, dashboard tables/charts, and AI summaries

The project is designed for mixed vendor environments where equipment logs may arrive as structured, semi-structured, unstructured, binary-like, or previously unseen file types.

## What the project does

The pipeline ingests a raw tool log, detects its structure, routes it into the correct parsing lane, extracts meaningful records, standardizes field names, normalizes engineering units, validates the records, and writes accepted rows into domain-specific MySQL tables.

It also supports a read-only dashboard that loads the database contents and provides chart-based views and optional AI-assisted commentary.

## Supported input categories

### Structured
- JSON
- CSV
- XML

### Semi-structured
- Syslog-like logs
- Key-value logs
- telemetry-style text lines with embedded engineering measurements

### Unstructured
- free-form plain text event logs
- maintenance notes / low-structure technical text

### Binary-like / non-human-readable
- binary / hex fallback lane
- unknown binary-like files are safely isolated instead of being fake-parsed as text
- Parquet is treated as **structured binary** and safely identified, but not fully decoded in the current prototype

### Unknown file types
The project includes content-based sniffing for unknown file types:
- readable unknown text is routed into existing text parsing lanes when possible
- binary-like unknown files are handled safely through a non-text fallback path

## End-to-end architecture

1. **Ingestion**  
   File is copied into a managed raw-data location and assigned metadata such as filename, extension, hash, and ingestion time.

2. **Detection**  
   The system performs format detection using both extension hints and content sniffing.

3. **Support check + routing**  
   Files are routed into one of the available lanes:
   - structured parser
   - semi-structured parser
   - free-form text parser
   - binary / hex fallback
   - safe binary unknown handler

4. **Parsing**  
   - Structured logs use dedicated readers and structure builders
   - Semi-structured logs use rule-based and AI-assisted spec generation
   - Free-form logs use chunking and LLM-assisted extraction
   - Binary/hex inputs use a controlled fallback path

5. **Canonicalization**  
   Parsed records are converted into a canonical batch representation.

6. **Adapter application**  
   Adapter specs map source/vendor record types and field names into the project’s canonical schema.

7. **Standardization**  
   Common aliases are harmonized into standard field names such as `tool_id`, `timestamp`, `parameter`, `value`, `unit`, `recipe`, `step`, `fault_code`, and `fault_summary`.

8. **Normalization**  
   Engineering units are normalized into canonical units where supported. Examples include:
   - temperature → `degC`
   - pressure → `Pa`
   - time → `s`
   - power → `W`
   - thickness → `nm`
   - gas flow → `sccm`

9. **Validation**  
   Accepted and rejected records are separated based on downstream business rules.

10. **Routing to tables**  
    Accepted records are routed into domain-specific relational tables.

11. **Database storage**  
    Rows are inserted into MySQL.

12. **Query, dashboard, and AI analysis**  
    The dashboard loads the parsed tables, renders charts, and can call OpenAI for compact analytical summaries.

## Repository layout

```text
app/
  adapters/              Adapter fingerprinting, spec generation, spec storage, spec application
  adapter_specs/         Saved adapter specs keyed by schema fingerprint
  ai/                    Structured file AI config generation / validation helpers
  binary_hex/            Binary and hex fallback handling
  db/                    MySQL schema creation and write helpers
  free_form/             Free-form text chunking, extraction, validation, post-processing
  ingestion/             Ingestion, detection, support registry, routing
  parser/                Generic parser entry points and helpers
  pipeline/              Canonicalize → standardize → normalize → validate → route
  readers/               Shared readers (e.g. JSON reader)
  semi_structured/       Semi-structured text family detection, parsing, spec building
  structured/            CSV / JSON / XML structure building and readers
  transform/             Legacy canonicalization helper(s)
  main.py                Main pipeline orchestration entry point

dashboard/
  server.js              Express dashboard server
  views/dashboard.ejs    Main dashboard view
  .env.example           Dashboard environment template
```

## Database tables

The backend writes to the following tables:

- `files`
- `equipment_states`
- `process_parameters_recipes`
- `sensor_readings`
- `fault_events`
- `wafer_processing_sequences`
- `generic_observations_staging`
- `rejected_records`

These tables allow the project to preserve both:
- accepted, queryable machine-readable records
- rejected or weakly-supported records for traceability and future parser improvement

## Runtime requirements

### Python backend
- Python **3.10+** recommended
- MySQL server
- Optional OpenAI API key for AI-assisted features

### Dashboard
- Node.js **18+** recommended
- npm
- MySQL server reachable from the dashboard
- Optional OpenAI API key for `/analyze-data`

## Python dependencies

See `requirements.txt`.

Install them with:

```bash
pip install -r requirements.txt
```

## Dashboard dependencies

The dashboard uses Node.js dependencies defined in `dashboard/package.json`:
- `express`
- `ejs`
- `mysql2`
- `dotenv`
- `openai`

Install them with:

```bash
cd dashboard
npm install
```

## Configuration

### Backend `.env`
The root `.env.example` currently contains:

```env
OPENAI_API_KEY=
OPENAI_MODEL=
```

AI-assisted parser features will fall back when no API key is available.

### Dashboard `.env`
Use `dashboard/.env.example` as the starting point:

```env
PORT=3000
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=YOUR_PASSWORD
MYSQL_DATABASE=semicon_parser
OPENAI_API_KEY=
```

### Important note about backend DB config
In the current codebase, the backend MySQL schema helper stores DB connection settings in `app/db/schema.py` via `DB_CONFIG`.  
Before running the parser, update that file so the backend points to your local MySQL instance and target database.

For a future cleanup, this DB config should be externalized into environment variables to match the dashboard style.

## Running the parser

From the project root:

```bash
python app/main.py
```

`app/main.py` currently calls `run_pipeline(...)` with a sample file path in the `__main__` block.  
Change that file path to the log you want to test, or import and call `run_pipeline()` from your own script.

The parser writes processed result JSON files into:

```text
data/processed/
```

## Running the dashboard

```bash
cd dashboard
npm install
npm start
```

Then open:

```text
http://localhost:3000
```

Useful endpoints:
- `GET /` – dashboard
- `GET /health` – health check
- `POST /analyze-data` – AI analysis for selected dashboard rows

## Typical demo flow

1. Prepare a raw log file (JSON / XML / CSV / syslog / KV / free-form text / binary-like sample)
2. Run the parser from `app/main.py`
3. Confirm accepted rows are written into MySQL
4. Open the dashboard and inspect tables/charts
5. Optionally trigger AI analysis for a selected dashboard tab

## AI-assisted components

The project uses the OpenAI API for several targeted tasks:
- structured file structure detection and repair
- semi-structured spec generation
- free-form text extraction
- adapter spec generation / enrichment
- dashboard-side summarization of selected data

The pipeline is intentionally hybrid:
- deterministic parsing where structure is reliable
- AI assistance where pattern discovery or semantic mapping is needed

## Development tools and technologies

### Backend / parser
- Python
- regex- and rule-based parsing
- modular pipeline design
- `python-dotenv`
- `pydantic`
- `mysql-connector-python`
- OpenAI Python SDK

### Dashboard
- Node.js
- Express
- EJS
- Chart.js (CDN in the template)
- `mysql2`
- OpenAI Node SDK

### Data / assets used
- synthetic and vendor-style semiconductor log samples
- adapter spec JSON files
- processed JSON result artifacts
- dashboard view templates

## Current limitations

- Unknown binary-like formats are safely isolated, not universally decoded
- Parquet is recognized as structured binary but is **not** fully parsed without a dedicated format-aware reader
- Some vendor-specific JSON schemas may still require richer adapter mappings to reduce rejected records
- Free-form extraction is AI-assisted and may require human review for high-stakes use cases
- Unit normalization covers many common engineering units, but not every possible vendor-specific representation
- Some DB configuration is still code-based instead of fully environment-driven

## Suggested next improvements

- Externalize backend DB configuration into `.env`
- Improve vendor-specific adapter coverage for JSON families with high rejection counts
- Add optional dedicated Parquet decoding path if required
- Expand normalization coverage for additional engineering units and field families
- Add automated regression tests for representative log families

