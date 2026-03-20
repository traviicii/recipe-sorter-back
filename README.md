# Recipe Sorter Backend

Recipe Sorter Backend is a FastAPI service for storing recipe PDF collections, parsing macro-first recipe cards, and exposing a library-oriented API for the frontend.

This repo powers the `v2.0.0-beta.1` backend release.

## What Changed In V2 Beta
- Shifted from one-off document parsing to persisted collection parsing
- Added library-level collection and recipe endpoints
- Added selective OCR and structured parsing helpers
- Added collection reuse via PDF hash + parser version
- Added per-collection progress and durable local storage
- Added clear-library support
- Added tests and a CLI preview harness for local review

See `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/RELEASE_NOTES.md` for the beta release notes.

## Tech Stack
- FastAPI
- Python 3
- pdfplumber / pdfminer
- pdf2image + Tesseract OCR
- OpenAI API for targeted fallback extraction
- Local JSON/file persistence under `data/`

## Local Setup
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install OCR dependencies on macOS:
   ```bash
   brew install tesseract poppler
   ```

4. Create your env file:
   ```bash
   cp .env.example .env
   ```

5. Start the API:
   ```bash
   fastapi dev main.py
   ```

## Environment Variables
### Required
- `OPENAI_API_KEY`

### Model / request settings
- `OPENAI_MODEL` — default: `gpt-4o`
- `OPENAI_MODEL_FAST` — default: `gpt-4o-mini`
- `OPENAI_TIMEOUT_SECONDS` — default: `60`
- `OPENAI_MAX_RETRIES` — default: `1`
- `LLM_MAX_TOKENS` — optional response cap
- `LLM_MAX_CHARS` — default: `12000`
- `LLM_MAX_CHARS_FAST` — default: `12000`
- `INCLUDE_INSTRUCTIONS` — default: `true`

### OCR / parsing settings
- `OCR_MIN_WORDS` — default: `40`
- `OCR_MIN_ALPHA_RATIO` — default: `0.6`
- `OCR_MAX_PAGES` — default: `4`
- `OCR_SKIP_AVG_SCORE` — default: `0.55`
- `FAST_MAX_PAGES` — optional
- `MAX_PAGES` — optional
- `TESSERACT_LANG` — default: `eng`
- `TESSERACT_CMD` — optional path override
- `USE_LLM_FALLBACK` — default: `false`
- `ENRICH_INGREDIENTS` — default: `false`
- `SKIP_OCR` — default: `false`

### Local dev / CORS
- `CORS_ALLOW_ALL` — default: `false`
- `CORS_ORIGINS` — comma-separated allowlist for the frontend

## API
### Core endpoints
- `POST /collections`
  - Upload a PDF collection
  - Reuses a previous parse when the same PDF hash and parser version already exist
- `GET /collections`
  - Returns all saved collections in reverse update order
- `GET /collections/{collectionId}`
  - Returns one collection and its latest job metadata
- `GET /recipes`
  - Returns recipes across the whole library
- `GET /recipes?collectionIds=id1,id2`
  - Returns recipes only from selected collections
- `GET /collections/{collectionId}/recipes`
  - Returns recipes for a single collection
- `DELETE /collections`
  - Clears persisted local library data
  - Blocked while parsing is actively running

### Secondary/debug endpoints
- `POST /collections/{collectionId}/enrich`
  - Optional ingredient enrichment pass
  - Kept for debugging / secondary workflows
- `POST /parse-recipes`
  - Legacy parsing endpoint
  - Useful for low-level debugging and extraction diagnostics

## Parsing Pipeline
The collection pipeline is structured as:
1. Page extraction
2. Recipe segmentation
3. Macro extraction
4. Targeted OCR for unresolved low-quality blocks
5. Optional LLM fallback for missing macro fields
6. Optional ingredient enrichment

The primary goal is to return usable macro cards quickly, not to reconstruct every recipe perfectly.

## Persistence Model
Data is stored locally under `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/data`.

Generated files:
- `data/collections/<collection-id>/collection.json`
- `data/collections/<collection-id>/recipes.json`
- `data/collections/<collection-id>/blocks.json`
- `data/collections/<collection-id>/source.pdf`
- `data/jobs/<job-id>.json`
- `data/hash_index.json`

These files are intentionally ignored by Git.

## Project Structure
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/main.py`
  - FastAPI app and HTTP endpoints
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/collection_service.py`
  - Collection lifecycle, segmentation, parsing workflow, progress updates
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/parser.py`
  - PDF text extraction, OCR helpers, cleanup, and targeted LLM parsing support
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/storage.py`
  - Local storage helpers for collections, jobs, recipes, and hashes
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/scripts/preview_parse.py`
  - CLI preview harness for manual QA
- `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/tests/`
  - Unit tests for parser, collection service, and storage

## Manual Review Harness
Drop PDFs into `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/samples/` and run:
```bash
python3 scripts/preview_parse.py
```

Useful env toggles for the harness:
- `USE_LLM_FALLBACK=true`
- `ENRICH_INGREDIENTS=true`
- `SKIP_OCR=true`
- `PARSE_MODE=fast`

Output:
- JSON artifacts are written to `/Users/travispeck/Documents/coding_projects/recipe-sorter/recipe-sorter-back/artifacts/`
- A compact terminal summary is printed for sanity checks

## Testing
Run the backend unit tests:
```bash
python3 -m unittest tests/test_parser.py tests/test_collection_service.py tests/test_storage.py
```

Compile-check key files:
```bash
python3 -m py_compile main.py storage.py collection_service.py parser.py
```

## Known Beta Limitations
- OCR and LLM fallback are still heuristics and can produce partial cards
- Ingredients are not the main product path yet
- Local persistence is file-based and intended for this app’s current scale, not a multi-user production deployment
- Clearing the library while parsing is active is intentionally blocked

## Versioning
This repo is currently tagged:
- `v2.0.0-beta.1`
