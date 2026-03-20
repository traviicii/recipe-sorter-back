# Release Notes

## v2.0.0-beta.1
Date: 2026-03-19

### Summary
This beta introduces the new collection-based parsing backend for Recipe Sorter. Instead of treating each PDF as a one-off request, the backend now persists collections, exposes library-oriented endpoints, and returns macro-first recipe cards that can be reused across sessions.

### Highlights
- Added persisted collection parsing pipeline
- Added library-level collection and recipe endpoints
- Added PDF hash reuse to avoid reparsing identical uploads
- Added selective OCR support for low-quality extraction
- Added macro-first parsing flow with optional enrichment
- Added local job tracking and progress messaging
- Added clear-library endpoint for local resets
- Added preview harness and unit tests

### New API Surface
- `POST /collections`
- `GET /collections`
- `GET /collections/{collectionId}`
- `GET /recipes`
- `GET /recipes?collectionIds=id1,id2`
- `GET /collections/{collectionId}/recipes`
- `DELETE /collections`

### Parsing Strategy
The backend now prioritizes:
1. Deterministic macro extraction
2. Targeted OCR only when needed
3. LLM fallback only for missing macro fields

### Developer Notes
- Storage is local and file-based under `data/`
- Sample PDFs under `samples/*.pdf` are ignored by Git
- The legacy `POST /parse-recipes` endpoint remains available for debugging

### Known Limitations
- OCR/LLM fallback still depends on PDF quality
- Ingredient enrichment is secondary and not the primary product path
- The current storage model is intended for local/single-user workflows
