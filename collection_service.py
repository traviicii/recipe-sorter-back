import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

from parser import (
    clean_text,
    extract_pdf_text_pages,
    get_openai_client,
    infer_cooking_method,
    normalize_cooking_method,
    normalize_numeric_value,
    ocr_selected_pages,
    should_ocr_page,
)
from storage import (
    compute_pdf_hash,
    create_collection_record,
    create_job,
    find_collection_by_hash,
    load_blocks,
    load_collection,
    load_job,
    load_pdf_bytes,
    load_recipes,
    register_collection_hash,
    save_blocks,
    save_collection,
    save_job,
    save_pdf_bytes,
    save_recipes,
    upsert_recipe,
)


PARSER_VERSION = "collection-macro-v2"
HEADER_PATTERNS = {
    "https://bevictoriouscoaching.com/",
}
ACTION_PREFIXES = (
    "add ",
    "combine ",
    "heat ",
    "place ",
    "stir ",
    "cook ",
    "drizzle ",
    "divide ",
    "mix ",
    "remove ",
    "fold ",
    "transfer ",
    "serve ",
    "top ",
    "refrigerate ",
    "best enjoyed",
    "use ",
    "scoop ",
    "flip ",
)
LABEL_PREFIXES = (
    "saturated",
    "trans",
    "polyunsaturated",
    "monounsaturated",
    "fiber",
    "sugar",
    "sodium",
)
GRAM_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*g\b", re.IGNORECASE)
MG_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*mg\b", re.IGNORECASE)
CALORIE_RE = re.compile(r"^\d{2,4}$")
TIME_RE = re.compile(r"(\d+)\s*(?:to|-)?\s*\d*\s*(minutes?|minute|hours?|hour)", re.IGNORECASE)
NUTRITION_CUE_RE = re.compile(r"(saturated|fiber|sodium|protein|calories?)", re.IGNORECASE)
SEGMENT_TIME_RE = re.compile(
    r"^\d+\s*(?:hours?|hour|minutes?|minute)(?:\s+\d+\s*(?:minutes?|minute))?$",
    re.IGNORECASE,
)
LOWERCASE_FILLERS = {"and", "or", "with", "of", "in", "the", "a", "&", "to", "for", "optional"}
SERVICE_LOCK = Lock()
ProgressCallback = Optional[Callable[[str, int], None]]
UNSET = object()


@dataclass
class RecipeBlock:
    id: str
    title: str
    lines: List[str]
    text: str
    pageNumbers: List[int]


def public_collection(collection: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": collection.get("id"),
        "name": collection.get("name"),
        "sourceFilename": collection.get("sourceFilename"),
        "pdfHash": collection.get("pdfHash"),
        "parserVersion": collection.get("parserVersion"),
        "status": collection.get("status", "queued"),
        "step": collection.get("step", "queued"),
        "progress": collection.get("progress", 0),
        "macroStatus": collection.get("macroStatus", "queued"),
        "ingredientStatus": collection.get("ingredientStatus", "idle"),
        "pageCount": collection.get("pageCount", 0),
        "totalBlocks": collection.get("totalBlocks", 0),
        "totalRecipes": collection.get("totalRecipes", 0),
        "parsedRecipes": collection.get("parsedRecipes", 0),
        "completeRecipes": collection.get("completeRecipes", 0),
        "partialRecipes": collection.get("partialRecipes", 0),
        "failedRecipes": collection.get("failedRecipes", 0),
        "ocrRecipes": collection.get("ocrRecipes", 0),
        "enrichedRecipes": collection.get("enrichedRecipes", 0),
        "reused": collection.get("reused", False),
        "lastJobId": collection.get("lastJobId"),
        "message": collection.get("message", "Queued"),
        "error": collection.get("error"),
        "createdAt": collection.get("createdAt"),
        "updatedAt": collection.get("updatedAt"),
    }


def public_recipe(recipe: Dict[str, Any], collection: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {
        key: value
        for key, value in recipe.items()
        if not key.startswith("_") and key not in {"blockText", "blockHash", "macroFinalized"}
    }
    if collection is not None:
        payload["collectionId"] = collection.get("id")
        payload["collectionName"] = collection.get("name")
        payload["collectionSourceFilename"] = collection.get("sourceFilename")
    return payload


def looks_like_time(line: str) -> bool:
    return bool(TIME_RE.search(line))


def looks_like_duration_line(line: str) -> bool:
    return bool(SEGMENT_TIME_RE.fullmatch(line.strip()))


def has_nutrition_cue(lines: List[str], start_index: int, window: int = 12) -> bool:
    search_window = lines[start_index + 1:start_index + 1 + window]
    for line in search_window:
        stripped = line.strip()
        if not stripped:
            continue
        if NUTRITION_CUE_RE.search(stripped):
            return True
        if CALORIE_RE.fullmatch(stripped):
            return True
    return False


def looks_like_title(line: str) -> bool:
    stripped = line.strip()
    if len(stripped) < 3 or len(stripped) > 120:
        return False
    if stripped.lower() in HEADER_PATTERNS:
        return False
    if CALORIE_RE.fullmatch(stripped):
        return False
    if looks_like_time(stripped):
        return False
    if stripped.lower().startswith(ACTION_PREFIXES):
        return False
    return True


def is_confirmed_recipe_start(lines: List[str], index: int) -> bool:
    if not looks_like_title(lines[index]):
        return False
    if index + 1 < len(lines) and looks_like_duration_line(lines[index + 1]):
        return True
    if index == 0:
        return has_nutrition_cue(lines, index)
    return False


def normalize_page_lines(text: str) -> List[str]:
    lines = [line.strip() for line in clean_text(text).split("\n") if line.strip()]
    return [line for line in lines if line.lower() not in HEADER_PATTERNS]


def find_segment_starts(lines: List[str]) -> List[int]:
    starts: List[int] = []
    for index in range(max(0, len(lines) - 1)):
        if is_confirmed_recipe_start(lines, index):
            starts.append(index)
    if not starts and lines and is_confirmed_recipe_start(lines, 0):
        starts.append(0)
    return starts


def compute_block_hash(text: str) -> str:
    normalized = clean_text(text)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def split_page_segments(lines: List[str]) -> Tuple[List[str], List[List[str]]]:
    starts = find_segment_starts(lines)
    if not starts:
        return lines, []

    prefix = lines[:starts[0]]
    segments: List[List[str]] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(lines)
        segments.append(lines[start:end])
    return prefix, segments


def build_block(lines: List[str], page_number: int) -> RecipeBlock:
    title = lines[0] if lines else f"Recipe {page_number}"
    return RecipeBlock(
        id=str(uuid4()),
        title=title,
        lines=list(lines),
        text="\n".join(lines),
        pageNumbers=[page_number],
    )


def assign_stable_block_ids(blocks: List[RecipeBlock]) -> List[RecipeBlock]:
    for index, block in enumerate(blocks, start=1):
        if not block.id:
            block.id = f"block-{index:04d}"
            continue
        if re.fullmatch(r"[0-9a-fA-F-]{32,36}", block.id):
            block.id = f"block-{index:04d}"
    return blocks


def deserialize_blocks(raw_blocks: List[Dict[str, Any]]) -> List[RecipeBlock]:
    blocks: List[RecipeBlock] = []
    for index, raw in enumerate(raw_blocks, start=1):
        blocks.append(
            RecipeBlock(
                id=raw.get("id") or f"block-{index:04d}",
                title=raw.get("title") or f"Recipe {index}",
                lines=list(raw.get("lines") or []),
                text=raw.get("text") or "\n".join(raw.get("lines") or []),
                pageNumbers=list(raw.get("pageNumbers") or []),
            )
        )
    return blocks


def segment_recipe_blocks(pages: List[Any], progress_cb: ProgressCallback = None) -> List[RecipeBlock]:
    blocks: List[RecipeBlock] = []
    current: Optional[RecipeBlock] = None
    total_pages = len(pages)

    for page_index, page in enumerate(pages, start=1):
        lines = normalize_page_lines(page.text)
        if progress_cb:
            progress_cb(f"Segmenting page {page_index}/{max(1, total_pages)}", 25 + int(10 * (page_index / max(1, total_pages))))
        if not lines:
            continue

        prefix, segments = split_page_segments(lines)
        if not segments:
            if current is None:
                current = build_block(lines, page_index)
            else:
                current.lines.extend(lines)
                current.text = "\n".join(current.lines)
                if page_index not in current.pageNumbers:
                    current.pageNumbers.append(page_index)
            continue

        if prefix and current is not None:
            current.lines.extend(prefix)
            current.text = "\n".join(current.lines)
            if page_index not in current.pageNumbers:
                current.pageNumbers.append(page_index)

        for segment_index, segment in enumerate(segments):
            if current is not None:
                blocks.append(current)
                current = None

            current = build_block(segment, page_index)
            if segment_index < len(segments) - 1:
                blocks.append(current)
                current = None

    if current is not None:
        blocks.append(current)

    return assign_stable_block_ids(blocks)


def parse_time_minutes(lines: List[str]) -> Optional[int]:
    for line in lines[:5]:
        match = TIME_RE.search(line)
        if not match:
            continue
        value = normalize_numeric_value(match.group(1))
        if value is None:
            continue
        if "hour" in line.lower():
            return value * 60
        return value
    return None


def find_calorie_index(lines: List[str]) -> Optional[int]:
    for index, line in enumerate(lines[2:], start=2):
        stripped = line.strip()
        if CALORIE_RE.fullmatch(stripped):
            return index
        if re.match(r"^\d{2,4}\b", stripped) and not GRAM_RE.search(stripped):
            return index
    return None


def extract_labeled_grams(lines: List[str], label: str) -> Optional[int]:
    pattern = re.compile(rf"{label}\s+(-?\d+(?:\.\d+)?)\s*g\b", re.IGNORECASE)
    for line in lines:
        match = pattern.search(line)
        if match:
            return normalize_numeric_value(match.group(1))
    return None


def extract_first_grams(line: str) -> Optional[int]:
    match = GRAM_RE.search(line)
    if not match:
        return None
    return normalize_numeric_value(match.group(1))


def find_sodium_index(lines: List[str], start_index: int) -> Optional[int]:
    for index in range(start_index, len(lines)):
        if "sodium" in lines[index].lower():
            return index
    return None


def extract_unlabeled_gram_values(lines: List[str], start_index: int, end_index: int) -> List[int]:
    values: List[int] = []
    for line in lines[start_index:end_index]:
        lower = line.lower()
        if any(label in lower for label in LABEL_PREFIXES):
            continue
        value = extract_first_grams(line)
        if value is not None:
            values.append(value)
    return values


def split_ingredient_prefix(line: str) -> str:
    lower = line.lower()
    for prefix in ACTION_PREFIXES:
        marker = f" {prefix.strip()} "
        index = lower.find(marker)
        if index > 0:
            return line[:index].strip(" ,.-")
    return line.strip()


def is_probable_ingredient(line: str) -> bool:
    candidate = split_ingredient_prefix(line)
    lower = candidate.lower()
    if not candidate:
        return False
    if "." in candidate:
        return False
    if CALORIE_RE.fullmatch(candidate) or looks_like_time(candidate):
        return False
    if lower.startswith(ACTION_PREFIXES):
        return False
    if any(label in lower for label in LABEL_PREFIXES):
        return False
    if "approximately" in lower or "airtight" in lower or "container" in lower or "enjoy" in lower:
        return False
    words = candidate.replace(",", " ").split()
    if len(words) > 8:
        return False
    if "(" in candidate and candidate[0].isupper():
        return True
    capitalized_words = 0
    for word in words:
        cleaned = word.strip("()").strip()
        if not cleaned:
            continue
        if cleaned.lower() in LOWERCASE_FILLERS:
            capitalized_words += 1
            continue
        if cleaned[0].isupper():
            capitalized_words += 1
    return capitalized_words >= max(1, len(words) - 1)


def extract_ingredient_candidates(lines: List[str], calorie_index: Optional[int]) -> List[str]:
    end_index = calorie_index if calorie_index is not None else len(lines)
    candidates: List[str] = []
    for line in lines[2:end_index]:
        candidate = split_ingredient_prefix(line)
        if not is_probable_ingredient(candidate):
            continue
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def compute_macro_status(recipe: Dict[str, Any]) -> str:
    required_fields = ["title", "calories", "protein", "fat", "fiber"]
    present = sum(1 for field in required_fields if recipe.get(field) not in (None, "", []))
    if present == len(required_fields):
        return "complete"
    if present >= 2:
        return "partial"
    return "failed"


def infer_block_source(block: RecipeBlock, pages_by_number: Optional[Dict[int, Any]] = None) -> str:
    if not pages_by_number:
        return "pdf_text"
    for page_number in block.pageNumbers:
        page = pages_by_number.get(page_number)
        if page is not None and getattr(page, "source", "pdf_text") == "ocr":
            return "ocr"
    return "pdf_text"


def build_confidence_map(recipe: Dict[str, Any], ingredient_candidates: List[str]) -> Dict[str, Optional[float]]:
    confidence: Dict[str, Optional[float]] = {
        "title": 0.98 if recipe.get("title") else None,
        "prepTime": 0.9 if recipe.get("prepTime") is not None else None,
        "ingredients": None,
        "ingredientCount": 0.75 if ingredient_candidates else None,
        "calories": 0.95 if recipe.get("calories") is not None else None,
        "protein": 0.9 if recipe.get("protein") is not None else None,
        "fat": 0.9 if recipe.get("fat") is not None else None,
        "saturatedFat": 0.9 if recipe.get("saturatedFat") is not None else None,
        "fiber": 0.9 if recipe.get("fiber") is not None else None,
        "instructions": None,
        "cookingMethod": 0.7 if recipe.get("cookingMethod") else None,
    }
    return confidence


def build_sources_map(recipe: Dict[str, Any], ingredient_candidates: List[str], block_source: str) -> Dict[str, str]:
    return {
        "title": block_source if recipe.get("title") else "unknown",
        "prepTime": block_source if recipe.get("prepTime") is not None else "unknown",
        "ingredients": "unknown",
        "ingredientCount": "computed" if ingredient_candidates else "unknown",
        "calories": block_source if recipe.get("calories") is not None else "unknown",
        "protein": block_source if recipe.get("protein") is not None else "unknown",
        "fat": block_source if recipe.get("fat") is not None else "unknown",
        "saturatedFat": block_source if recipe.get("saturatedFat") is not None else "unknown",
        "fiber": block_source if recipe.get("fiber") is not None else "unknown",
        "instructions": "unknown",
        "cookingMethod": "computed" if recipe.get("cookingMethod") else "unknown",
    }


def llm_fill_missing_macro_fields(recipe: Dict[str, Any], block: RecipeBlock) -> Dict[str, Any]:
    if recipe["macroStatus"] == "complete":
        return recipe
    if not os.getenv("OPENAI_API_KEY"):
        return recipe

    client = get_openai_client()
    model = os.getenv("OPENAI_MODEL_FAST", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    missing_fields = [
        field
        for field in ["title", "prepTime", "calories", "protein", "fat", "saturatedFat", "fiber", "cookingMethod"]
        if recipe.get(field) is None or (field == "cookingMethod" and recipe.get(field) == "Other")
    ]
    if not missing_fields:
        return recipe
    prompt = (
        "Extract only the missing macro-oriented recipe data from this single recipe block. "
        f"Only fill these keys if explicit or strongly implied: {', '.join(missing_fields)}. "
        "Return only JSON with keys: title, prepTime, calories, protein, fat, saturatedFat, fiber, cookingMethod. "
        "Use null for missing values. Do not include extra keys.\n\n"
        f"{block.text}"
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
        )
        payload = json.loads(response.choices[0].message.content or "{}")
    except Exception:
        return recipe

    for field in ["title", "prepTime", "calories", "protein", "fat", "saturatedFat", "fiber"]:
        if recipe.get(field) is None and payload.get(field) is not None:
            recipe[field] = normalize_numeric_value(payload.get(field)) if field != "title" else str(payload.get(field)).strip()
            recipe["sources"][field] = "inferred"
            recipe["confidence"][field] = 0.6

    if recipe.get("cookingMethod") in (None, "", "Other") and payload.get("cookingMethod"):
        recipe["cookingMethod"] = normalize_cooking_method(str(payload.get("cookingMethod")), block.text)
        recipe["sources"]["cookingMethod"] = "inferred"
        recipe["confidence"]["cookingMethod"] = 0.55

    recipe["macroStatus"] = compute_macro_status(recipe)
    return recipe


def parse_macro_recipe_block(
    block: RecipeBlock,
    use_llm_fallback: bool = True,
    pages_by_number: Optional[Dict[int, Any]] = None,
) -> Dict[str, Any]:
    lines = block.lines
    calorie_index = find_calorie_index(lines)
    ingredient_candidates = extract_ingredient_candidates(lines, calorie_index)
    block_source = infer_block_source(block, pages_by_number)
    block_hash = compute_block_hash(block.text)

    calories = normalize_numeric_value(lines[calorie_index]) if calorie_index is not None else None
    sodium_index = find_sodium_index(lines, calorie_index or 0)
    unlabeled_values = extract_unlabeled_gram_values(lines, (calorie_index or 0) + 1, sodium_index or len(lines))
    fat = unlabeled_values[0] if unlabeled_values else None
    protein = unlabeled_values[-1] if len(unlabeled_values) >= 2 else None

    recipe: Dict[str, Any] = {
        "id": f"recipe-{block.id}",
        "title": lines[0] if lines else block.title,
        "prepTime": parse_time_minutes(lines),
        "ingredients": [],
        "ingredientCount": len(ingredient_candidates) if ingredient_candidates else None,
        "calories": calories,
        "protein": protein,
        "fat": fat,
        "saturatedFat": extract_labeled_grams(lines, "Saturated"),
        "fiber": extract_labeled_grams(lines, "Fiber"),
        "instructions": None,
        "cookingMethod": infer_cooking_method(block.text),
        "confidence": {},
        "sources": {},
        "macroStatus": "queued",
        "ingredientStatus": "idle",
        "pageNumbers": block.pageNumbers,
        "blockId": block.id,
        "blockHash": block_hash,
        "blockText": block.text,
        "ocrUsed": block_source == "ocr",
        "macroFinalized": False,
        "_ingredientCandidates": ingredient_candidates,
    }

    recipe["confidence"] = build_confidence_map(recipe, ingredient_candidates)
    recipe["sources"] = build_sources_map(recipe, ingredient_candidates, block_source)
    recipe["macroStatus"] = compute_macro_status(recipe)

    if use_llm_fallback:
        recipe = llm_fill_missing_macro_fields(recipe, block)

    recipe["cookingMethod"] = normalize_cooking_method(recipe.get("cookingMethod"), block.text)
    return recipe


def enrich_recipe_ingredients(recipe: Dict[str, Any]) -> Dict[str, Any]:
    candidates = recipe.get("_ingredientCandidates") or []
    recipe["ingredients"] = candidates
    recipe["ingredientCount"] = len(candidates) if candidates else recipe.get("ingredientCount")
    recipe["ingredientStatus"] = "complete"
    recipe["sources"]["ingredients"] = "computed" if candidates else "unknown"
    recipe["confidence"]["ingredients"] = 0.7 if candidates else None
    if candidates:
        recipe["sources"]["ingredientCount"] = "computed"
        recipe["confidence"]["ingredientCount"] = 0.8
    return recipe


def summarize_recipes(recipes: List[Dict[str, Any]]) -> Dict[str, int]:
    complete = sum(1 for recipe in recipes if recipe.get("macroStatus") == "complete")
    partial = sum(1 for recipe in recipes if recipe.get("macroStatus") == "partial")
    failed = sum(1 for recipe in recipes if recipe.get("macroStatus") == "failed")
    ocr_recipes = sum(1 for recipe in recipes if recipe.get("ocrUsed"))
    return {
        "parsedRecipes": complete + partial,
        "completeRecipes": complete,
        "partialRecipes": partial,
        "failedRecipes": failed,
        "ocrRecipes": ocr_recipes,
    }


def refresh_collection_summary(collection_id: str, collection: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    current_collection = load_collection(collection_id) or collection
    if current_collection is None:
        raise ValueError(f"Collection {collection_id} not found")
    summary = summarize_recipes(load_recipes(collection_id))
    current_collection.update(summary)
    save_collection(current_collection)
    return current_collection


def should_skip_cached_recipe(cached_recipe: Optional[Dict[str, Any]], block: RecipeBlock) -> bool:
    if not cached_recipe:
        return False
    return (
        cached_recipe.get("macroFinalized") is True
        and cached_recipe.get("macroStatus") in {"complete", "partial"}
        and cached_recipe.get("blockHash") == compute_block_hash(block.text)
    )


def finalize_macro_results(collection_id: str) -> None:
    recipes = load_recipes(collection_id)
    updated = False
    for recipe in recipes:
        if not recipe.get("macroFinalized"):
            recipe["macroFinalized"] = True
            updated = True
    if updated:
        save_recipes(collection_id, recipes)


def pages_by_number(pages: List[Any]) -> Dict[int, Any]:
    return {page.index + 1: page for page in pages}


def select_targeted_ocr_page_indexes(
    blocks: List[RecipeBlock],
    recipes_by_block: Dict[str, Dict[str, Any]],
    page_lookup: Dict[int, Any],
) -> List[int]:
    min_words = int(os.getenv("OCR_MIN_WORDS", "40"))
    min_alpha_ratio = float(os.getenv("OCR_MIN_ALPHA_RATIO", "0.6"))
    page_indexes: List[int] = []
    seen: set[int] = set()
    for block in blocks:
        recipe = recipes_by_block.get(block.id)
        if recipe is None or recipe.get("macroStatus") == "complete" or recipe.get("macroFinalized"):
            continue
        for page_number in block.pageNumbers:
            page = page_lookup.get(page_number)
            if page is None or page.index in seen:
                continue
            if should_ocr_page(page.score, page.word_count, page.alpha_ratio, min_words, min_alpha_ratio):
                seen.add(page.index)
                page_indexes.append(page.index)
    return page_indexes


def update_job_and_collection(
    collection: Dict[str, Any],
    job: Dict[str, Any],
    *,
    job_status: Optional[str] = None,
    step: Optional[str] = None,
    progress: Optional[int] = None,
    message: Optional[str] = None,
    collection_status: Optional[str] = None,
    macro_status: Optional[str] = None,
    ingredient_status: Optional[str] = None,
    error: Any = UNSET,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if job_status is not None:
        job["status"] = job_status
    if step is not None:
        job["step"] = step
        collection["step"] = step
    if progress is not None:
        job["progress"] = progress
        collection["progress"] = progress
    if message is not None:
        job["message"] = message
        collection["message"] = message
    if collection_status is not None:
        collection["status"] = collection_status
    if macro_status is not None:
        collection["macroStatus"] = macro_status
    if ingredient_status is not None:
        collection["ingredientStatus"] = ingredient_status
    if error is not UNSET:
        job["error"] = error
        collection["error"] = error
    save_job(job)
    save_collection(collection)
    return collection, job


def create_or_reuse_collection(source_filename: str, pdf_bytes: bytes) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], bool]:
    pdf_hash = compute_pdf_hash(pdf_bytes)
    existing = find_collection_by_hash(pdf_hash, PARSER_VERSION)
    if existing is not None:
        existing_job = load_job(existing["lastJobId"]) if existing.get("lastJobId") else None
        if existing.get("status") != "failed":
            existing["reused"] = True
            save_collection(existing)
            return existing, existing_job, True

        retry_job = create_job(existing["id"], "macro_parse")
        existing["status"] = "queued"
        existing["step"] = "queued"
        existing["progress"] = 0
        existing["macroStatus"] = "queued"
        existing["error"] = None
        existing["reused"] = False
        existing["lastJobId"] = retry_job["id"]
        save_collection(existing)
        return existing, retry_job, False

    collection = create_collection_record(source_filename, pdf_hash, PARSER_VERSION)
    collection["pdfPath"] = save_pdf_bytes(collection["id"], pdf_bytes)
    save_collection(collection)
    register_collection_hash(collection["id"], pdf_hash, PARSER_VERSION)

    job = create_job(collection["id"], "macro_parse")
    collection["lastJobId"] = job["id"]
    save_collection(collection)
    return collection, job, False


def parse_collection_job(collection_id: str, job_id: str) -> None:
    with SERVICE_LOCK:
        collection = load_collection(collection_id)
        job = load_job(job_id)
        if collection is None or job is None:
            return

        update_job_and_collection(
            collection,
            job,
            job_status="processing",
            step="extracting",
            progress=5,
            message="Extracting pages",
            collection_status="processing",
            macro_status="processing",
            error=None,
        )

        def progress_cb(message: str, percent: int) -> None:
            current_collection = load_collection(collection_id) or collection
            current_job = load_job(job_id) or job
            step = "extracting"
            if percent >= 70:
                step = "reprocessing"
            elif percent >= 25:
                step = "segmenting"
            update_job_and_collection(
                current_collection,
                current_job,
                job_status="processing",
                step=step,
                progress=percent,
                message=message,
                collection_status="processing",
                macro_status="processing",
            )

        try:
            pdf_bytes = load_pdf_bytes(collection_id)
            pages = extract_pdf_text_pages(pdf_bytes, progress_cb=progress_cb, skip_ocr=True)
            page_lookup = pages_by_number(pages)

            collection = load_collection(collection_id) or collection
            job = load_job(job_id) or job
            collection["pageCount"] = len(pages)
            save_collection(collection)

            cached_blocks = deserialize_blocks(load_blocks(collection_id))
            if cached_blocks:
                blocks = cached_blocks
                update_job_and_collection(
                    collection,
                    job,
                    job_status="processing",
                    step="segmenting",
                    progress=30,
                    message="Using cached recipe segments",
                    collection_status="processing",
                    macro_status="processing",
                )
            else:
                update_job_and_collection(
                    collection,
                    job,
                    job_status="processing",
                    step="segmenting",
                    progress=30,
                    message="Segmenting recipes",
                    collection_status="processing",
                    macro_status="processing",
                )
                blocks = segment_recipe_blocks(pages, progress_cb=progress_cb)
                save_blocks(collection_id, [asdict(block) for block in blocks])

            collection = load_collection(collection_id) or collection
            job = load_job(job_id) or job
            collection["totalBlocks"] = len(blocks)
            collection["totalRecipes"] = len(blocks)
            save_collection(collection)

            existing_recipes = {recipe.get("blockId"): recipe for recipe in load_recipes(collection_id) if recipe.get("blockId")}

            for index, block in enumerate(blocks, start=1):
                progress = 36 + int(34 * (index / max(1, len(blocks))))
                update_job_and_collection(
                    collection,
                    job,
                    job_status="processing",
                    step="parsing_macros",
                    progress=progress,
                    message=f"Parsing macros {index}/{max(1, len(blocks))} recipes",
                    collection_status="processing",
                    macro_status="processing",
                )
                cached_recipe = existing_recipes.get(block.id)
                if should_skip_cached_recipe(cached_recipe, block):
                    continue
                try:
                    recipe = parse_macro_recipe_block(block, use_llm_fallback=False, pages_by_number=page_lookup)
                except Exception as exc:
                    recipe = {
                        "id": f"recipe-{block.id}",
                        "title": block.title,
                        "prepTime": None,
                        "ingredients": [],
                        "ingredientCount": None,
                        "calories": None,
                        "protein": None,
                        "fat": None,
                        "saturatedFat": None,
                        "fiber": None,
                        "instructions": None,
                        "cookingMethod": "Other",
                        "confidence": {},
                        "sources": {},
                        "macroStatus": "failed",
                        "ingredientStatus": "idle",
                        "pageNumbers": block.pageNumbers,
                        "blockId": block.id,
                        "blockHash": compute_block_hash(block.text),
                        "blockText": block.text,
                        "ocrUsed": False,
                        "macroFinalized": False,
                        "_ingredientCandidates": [],
                        "error": str(exc),
                    }
                upsert_recipe(collection_id, recipe)
                collection = refresh_collection_summary(collection_id, load_collection(collection_id) or collection)

            recipes_by_block = {recipe.get("blockId"): recipe for recipe in load_recipes(collection_id) if recipe.get("blockId")}
            ocr_page_indexes = select_targeted_ocr_page_indexes(blocks, recipes_by_block, page_lookup)

            if ocr_page_indexes:
                update_job_and_collection(
                    collection,
                    job,
                    job_status="processing",
                    step="reprocessing",
                    progress=72,
                    message="Reprocessing low-quality pages",
                    collection_status="processing",
                    macro_status="processing",
                )
                ocr_selected_pages(
                    pdf_bytes,
                    pages,
                    ocr_page_indexes,
                    progress_cb=progress_cb,
                    message_template="Reprocessing low-quality pages {current}/{total}",
                    progress_start=73,
                    progress_span=10,
                )
                page_lookup = pages_by_number(pages)
                blocks = segment_recipe_blocks(pages)
                save_blocks(collection_id, [asdict(block) for block in blocks])
                collection = load_collection(collection_id) or collection
                collection["totalBlocks"] = len(blocks)
                collection["totalRecipes"] = len(blocks)
                save_collection(collection)

                for index, block in enumerate(blocks, start=1):
                    cached_recipe = recipes_by_block.get(block.id)
                    if cached_recipe and cached_recipe.get("macroStatus") == "complete" and cached_recipe.get("blockHash") == compute_block_hash(block.text):
                        continue
                    progress = 84 + int(6 * (index / max(1, len(blocks))))
                    update_job_and_collection(
                        collection,
                        job,
                        job_status="processing",
                        step="parsing_macros",
                        progress=progress,
                        message=f"Parsing macros {index}/{max(1, len(blocks))} recipes",
                        collection_status="processing",
                        macro_status="processing",
                    )
                    try:
                        recipe = parse_macro_recipe_block(block, use_llm_fallback=False, pages_by_number=page_lookup)
                    except Exception as exc:
                        recipe = {
                            "id": f"recipe-{block.id}",
                            "title": block.title,
                            "prepTime": None,
                            "ingredients": [],
                            "ingredientCount": None,
                            "calories": None,
                            "protein": None,
                            "fat": None,
                            "saturatedFat": None,
                            "fiber": None,
                            "instructions": None,
                            "cookingMethod": "Other",
                            "confidence": {},
                            "sources": {},
                            "macroStatus": "failed",
                            "ingredientStatus": "idle",
                            "pageNumbers": block.pageNumbers,
                            "blockId": block.id,
                            "blockHash": compute_block_hash(block.text),
                            "blockText": block.text,
                            "ocrUsed": any(page_lookup.get(page_number) and page_lookup[page_number].source == "ocr" for page_number in block.pageNumbers),
                            "macroFinalized": False,
                            "_ingredientCandidates": [],
                            "error": str(exc),
                        }
                    upsert_recipe(collection_id, recipe)
                    collection = refresh_collection_summary(collection_id, load_collection(collection_id) or collection)

            recipes_by_block = {recipe.get("blockId"): recipe for recipe in load_recipes(collection_id) if recipe.get("blockId")}
            unresolved = [
                block for block in blocks
                if recipes_by_block.get(block.id, {}).get("macroStatus") in {"partial", "failed"}
                and not recipes_by_block.get(block.id, {}).get("macroFinalized")
            ]

            for index, block in enumerate(unresolved, start=1):
                update_job_and_collection(
                    collection,
                    job,
                    job_status="processing",
                    step="parsing_macros",
                    progress=93 + int(4 * (index / max(1, len(unresolved)))),
                    message=f"Filling missing macro fields {index}/{max(1, len(unresolved))} recipes",
                    collection_status="processing",
                    macro_status="processing",
                )
                try:
                    recipe = parse_macro_recipe_block(block, use_llm_fallback=True, pages_by_number=page_lookup)
                except Exception as exc:
                    recipe = recipes_by_block.get(block.id) or {
                        "id": f"recipe-{block.id}",
                        "title": block.title,
                        "prepTime": None,
                        "ingredients": [],
                        "ingredientCount": None,
                        "calories": None,
                        "protein": None,
                        "fat": None,
                        "saturatedFat": None,
                        "fiber": None,
                        "instructions": None,
                        "cookingMethod": "Other",
                        "confidence": {},
                        "sources": {},
                        "macroStatus": "failed",
                        "ingredientStatus": "idle",
                        "pageNumbers": block.pageNumbers,
                        "blockId": block.id,
                        "blockHash": compute_block_hash(block.text),
                        "blockText": block.text,
                        "ocrUsed": any(page_lookup.get(page_number) and page_lookup[page_number].source == "ocr" for page_number in block.pageNumbers),
                        "macroFinalized": False,
                        "_ingredientCandidates": [],
                    }
                    recipe["error"] = str(exc)
                upsert_recipe(collection_id, recipe)

            finalize_macro_results(collection_id)
            collection = refresh_collection_summary(collection_id, load_collection(collection_id) or collection)
            job = load_job(job_id) or job
            summary = {
                "complete": collection.get("completeRecipes", 0),
                "partial": collection.get("partialRecipes", 0),
                "failed": collection.get("failedRecipes", 0),
            }
            if collection.get("parsedRecipes", 0) == 0:
                update_job_and_collection(
                    collection,
                    job,
                    job_status="failed",
                    step="failed",
                    progress=100,
                    message="No recipes recovered from this collection",
                    collection_status="failed",
                    macro_status="failed",
                    ingredient_status="idle",
                    error="No recipes recovered from this collection",
                )
            else:
                update_job_and_collection(
                    collection,
                    job,
                    job_status="complete",
                    step="complete",
                    progress=100,
                    message=f"Done: {summary['complete']} complete, {summary['partial']} partial, {summary['failed']} failed",
                    collection_status="complete",
                    macro_status="complete",
                    ingredient_status="idle",
                    error=None,
                )
        except Exception as exc:
            collection = load_collection(collection_id) or collection
            job = load_job(job_id) or job
            update_job_and_collection(
                collection,
                job,
                job_status="failed",
                step="failed",
                progress=100,
                message="Parsing failed",
                collection_status="failed",
                macro_status="failed",
                error=str(exc),
            )


def enrich_collection_job(collection_id: str, job_id: str) -> None:
    with SERVICE_LOCK:
        collection = load_collection(collection_id)
        job = load_job(job_id)
        if collection is None or job is None:
            return

        recipes = load_recipes(collection_id)
        if not recipes:
            update_job_and_collection(
                collection,
                job,
                job_status="failed",
                step="failed",
                progress=100,
                message="No recipes to enrich",
                collection_status=collection.get("status", "complete"),
                ingredient_status="failed",
                error="No recipes to enrich",
            )
            return

        update_job_and_collection(
            collection,
            job,
            job_status="processing",
            step="enriching",
            progress=10,
            message="Enriching ingredients",
            collection_status=collection.get("status", "complete"),
            ingredient_status="processing",
            error=None,
        )

        for index, recipe in enumerate(recipes, start=1):
            recipe = enrich_recipe_ingredients(recipe)
            upsert_recipe(collection_id, recipe)
            collection = load_collection(collection_id) or collection
            job = load_job(job_id) or job
            collection["enrichedRecipes"] = index
            progress = 10 + int(90 * (index / max(1, len(recipes))))
            update_job_and_collection(
                collection,
                job,
                job_status="processing" if index < len(recipes) else "complete",
                step="enriching" if index < len(recipes) else "complete",
                progress=progress,
                message=f"Enriching recipe {index}/{len(recipes)}" if index < len(recipes) else "Ingredients ready",
                collection_status=collection.get("status", "complete"),
                ingredient_status="processing" if index < len(recipes) else "complete",
            )


def start_failed_recipe_retry(collection_id: str) -> Optional[Dict[str, Any]]:
    collection = load_collection(collection_id)
    if collection is None:
        return None

    recipes = load_recipes(collection_id)
    if not any(recipe.get("macroStatus") == "failed" for recipe in recipes):
        return None

    job = create_job(collection_id, "macro_parse_retry")
    collection["status"] = "queued"
    collection["step"] = "queued"
    collection["progress"] = 0
    collection["macroStatus"] = "queued"
    collection["error"] = None
    collection["reused"] = False
    collection["lastJobId"] = job["id"]
    save_collection(collection)
    return job


def start_enrichment(collection_id: str) -> Optional[Dict[str, Any]]:
    collection = load_collection(collection_id)
    if collection is None:
        return None
    job = create_job(collection_id, "ingredient_enrichment")
    collection["lastJobId"] = job["id"]
    save_collection(collection)
    return job
