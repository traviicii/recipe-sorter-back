# Recipe Sorter Backend

## Overview
FastAPI backend that stores uploaded recipe collections, parses macro-oriented recipe cards first, and optionally enriches ingredient lists afterward.

## Local Setup
1. Create a virtual environment and install requirements:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Install OCR dependencies (macOS):
   ```bash
   brew install tesseract poppler
   ```

3. Configure environment variables:
   ```bash
   cp .env.example .env
   ```

4. Run the API:
   ```bash
   fastapi dev main.py
   ```

## Environment Variables
- `OPENAI_API_KEY` (required)
- `OPENAI_MODEL` (default: `gpt-4o`)
- `OPENAI_MODEL_FAST` (default: `gpt-4o-mini`)
- `INCLUDE_INSTRUCTIONS` (default: `true`)
- `OPENAI_TIMEOUT_SECONDS` (default: `60`)
- `OPENAI_MAX_RETRIES` (default: `1`)
- `LLM_MAX_TOKENS` (optional; cap output tokens per request)
- `LLM_MAX_CHARS` (default: `12000`; chunk size for fallback)
- `LLM_MAX_CHARS_FAST` (default: `12000`; chunk size for fast mode)
- `OCR_MIN_WORDS` (default: `40`)
- `OCR_MIN_ALPHA_RATIO` (default: `0.6`)
- `OCR_MAX_PAGES` (default: `4`)
- `OCR_SKIP_AVG_SCORE` (default: `0.55`)
- `FAST_MAX_PAGES` (optional; cap pages in fast mode)
- `MAX_PAGES` (optional; cap pages in accurate mode)
- `TESSERACT_LANG` (default: `eng`)
- `TESSERACT_CMD` (optional path override)

## Collection API
- `POST /collections`
  - Upload a PDF collection.
  - Reuses the existing parsed collection when the same PDF hash and parser version already exist.
- `GET /collections/{collectionId}`
  - Returns collection metadata, progress, parse status, and latest job info.
- `GET /collections/{collectionId}/recipes`
  - Returns the parsed recipe cards available so far.
- `POST /collections/{collectionId}/enrich`
  - Runs the optional ingredient enrichment pass after macro parsing is complete.

## Legacy Debugging
Add `?debug=true` to `POST /parse-recipes` to include low-level page diagnostics:
- `page_sources`
- `page_quality_scores`
- `page_snippets`
- `ocr_pages`

## Manual Review Harness
Drop PDFs into `samples/` and run:
```bash
python3 scripts/preview_parse.py
```
Outputs JSON to `artifacts/` and prints a compact summary.
Useful options:
- `USE_LLM_FALLBACK=true` to let the per-recipe LLM fill missing macro fields.
- `ENRICH_INGREDIENTS=true` to include ingredient lists in the preview output.
- `SKIP_OCR=true` to force digital-text-only extraction for faster smoke tests.
