import io
import json
import os
import re
import math
import logging
from pathlib import Path
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract
from openai import OpenAI
try:
    from openai import APITimeoutError, APIError, RateLimitError
    RETRYABLE_ERRORS = (APITimeoutError, APIError, RateLimitError)
except Exception:
    RETRYABLE_ERRORS = tuple()
from pydantic import BaseModel, Field, ValidationError, ConfigDict
from dotenv import load_dotenv

SourceType = Literal["pdf_text", "ocr", "inferred", "computed", "unknown"]
ProgressCallback = Optional[Callable[[str, int], None]]

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.getLogger("pdfminer").setLevel(logging.ERROR)

FIELD_NAMES = [
    "title",
    "prepTime",
    "ingredients",
    "ingredientCount",
    "calories",
    "protein",
    "fat",
    "saturatedFat",
    "fiber",
    "instructions",
    "cookingMethod",
]


class Recipe(BaseModel):
    title: Optional[str] = None
    prepTime: Optional[int] = None
    ingredients: List[str] = Field(default_factory=list)
    ingredientCount: Optional[int] = None
    calories: Optional[int] = None
    protein: Optional[int] = None
    fat: Optional[int] = None
    saturatedFat: Optional[int] = None
    fiber: Optional[int] = None
    instructions: Optional[str] = None
    cookingMethod: Optional[str] = None
    confidence: Optional[Dict[str, Optional[float]]] = None
    sources: Optional[Dict[str, Optional[SourceType]]] = None

    model_config = ConfigDict(extra="ignore")


class RecipeExtraction(BaseModel):
    recipes: List[Recipe] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")


@dataclass
class PageText:
    index: int
    text: str
    word_count: int
    alpha_ratio: float
    score: float
    source: SourceType


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required")
    timeout_env = os.getenv("OPENAI_TIMEOUT_SECONDS")
    max_retries_env = os.getenv("OPENAI_MAX_RETRIES")
    kwargs: Dict[str, Any] = {"api_key": api_key}
    if timeout_env:
        try:
            kwargs["timeout"] = float(timeout_env)
        except ValueError:
            pass
    if max_retries_env:
        try:
            kwargs["max_retries"] = int(max_retries_env)
        except ValueError:
            pass
    try:
        return OpenAI(**kwargs)
    except TypeError:
        return OpenAI(api_key=api_key)


def is_retryable_exception(err: Exception) -> bool:
    if RETRYABLE_ERRORS and isinstance(err, RETRYABLE_ERRORS):
        return True
    message = str(err).lower()
    return "timeout" in message or "timed out" in message or "rate limit" in message


def normalize_numeric_value(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        cleaned = value.strip().lower()
        cleaned = cleaned.replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if not match:
            return None
        try:
            return int(round(float(match.group(0))))
        except ValueError:
            return None
    return None


def infer_cooking_method(text: str) -> str:
    if not text:
        return "Other"
    lower_text = text.lower()
    if "oven" in lower_text or "bake" in lower_text or "roast" in lower_text:
        return "Oven"
    if "air fryer" in lower_text:
        return "Air Fryer"
    if (
        "stovetop" in lower_text
        or "skillet" in lower_text
        or "pan" in lower_text
        or "fry" in lower_text
        or "saute" in lower_text
        or "boil" in lower_text
        or "simmer" in lower_text
    ):
        return "Stovetop"
    if "blender" in lower_text or "blend" in lower_text or "smoothie" in lower_text:
        return "Blender"
    if (
        "no cook" in lower_text
        or "raw" in lower_text
        or "refrigerate" in lower_text
        or "chill" in lower_text
        or "overnight" in lower_text
    ):
        return "No Cook"
    return "Other"


def normalize_cooking_method(method: Optional[str], instructions: Optional[str]) -> str:
    if method:
        normalized = method.strip().lower()
        if normalized in {"oven", "baked", "baking", "roasting", "roast"}:
            return "Oven"
        if normalized in {"stovetop", "skillet", "pan", "frying", "fry", "saute", "boiling", "boil", "simmer"}:
            return "Stovetop"
        if normalized in {"air fryer", "air-fryer"}:
            return "Air Fryer"
        if normalized in {"blender", "blending", "blend", "smoothie"}:
            return "Blender"
        if normalized in {"no cook", "nocook", "raw", "refrigeration", "chilling", "overnight"}:
            return "No Cook"
        if normalized in {"other"}:
            return "Other"
    return infer_cooking_method(instructions or "")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "\n")
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(line for line in lines if line)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def score_page_text(text: str, word_count: Optional[int] = None, alpha_ratio: Optional[float] = None) -> float:
    if word_count is None:
        word_count = len(re.findall(r"\b\w+\b", text))
    if alpha_ratio is None:
        alpha_ratio = compute_alpha_ratio(text)
    word_score = min(1.0, word_count / 200.0)
    score = 0.6 * alpha_ratio + 0.4 * word_score
    return round(score, 3)


def compute_alpha_ratio(text: str) -> float:
    if not text:
        return 0.0
    total = len(text)
    if total == 0:
        return 0.0
    alpha = sum(1 for ch in text if ch.isalpha())
    return round(alpha / total, 3)


def should_ocr_page(
    score: float,
    word_count: int,
    alpha_ratio: float,
    min_words: int,
    min_alpha_ratio: float,
) -> bool:
    return word_count < min_words or alpha_ratio < min_alpha_ratio or score < 0.35


def ocr_page_image(image) -> str:
    tesseract_cmd = os.getenv("TESSERACT_CMD")
    if tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
    lang = os.getenv("TESSERACT_LANG", "eng")
    return pytesseract.image_to_string(image, lang=lang, config="--oem 1 --psm 6")


def render_pdf_page_image(pdf_bytes: bytes, page_index: int, dpi: int = 200):
    images = convert_from_bytes(
        pdf_bytes,
        dpi=dpi,
        first_page=page_index + 1,
        last_page=page_index + 1,
    )
    return images[0] if images else None


def strip_repeated_lines(pages: List[PageText], min_ratio: float = 0.7) -> None:
    if not pages or len(pages) < 2:
        return
    line_counts: Counter[str] = Counter()
    page_lines: List[List[str]] = []

    for page in pages:
        lines = [line.strip().lower() for line in page.text.split("\n") if line.strip()]
        page_lines.append(lines)
        for line in set(lines):
            if 0 < len(line) <= 120:
                line_counts[line] += 1

    threshold = max(2, math.ceil(len(pages) * min_ratio))
    repeated = {line for line, count in line_counts.items() if count >= threshold}

    for idx, page in enumerate(pages):
        filtered_lines = [
            line for line in page.text.split("\n")
            if line.strip().lower() not in repeated
        ]
        page.text = clean_text("\n".join(filtered_lines))
        pages[idx] = page


def _notify(progress_cb: ProgressCallback, message: str, percent: int) -> None:
    if progress_cb:
        progress_cb(message, percent)


def extract_pdf_text_pages(
    pdf_bytes: bytes,
    progress_cb: ProgressCallback = None,
    skip_ocr: bool = False,
    max_ocr_pages_override: Optional[int] = None,
) -> List[PageText]:
    min_words = int(os.getenv("OCR_MIN_WORDS", "40"))
    min_alpha_ratio = float(os.getenv("OCR_MIN_ALPHA_RATIO", "0.6"))
    max_ocr_pages = int(os.getenv("OCR_MAX_PAGES", "4"))
    skip_avg_score = float(os.getenv("OCR_SKIP_AVG_SCORE", "0.55"))
    if max_ocr_pages_override is not None:
        max_ocr_pages = max_ocr_pages_override

    pages: List[PageText] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        for idx, page in enumerate(pdf.pages):
            raw_text = page.extract_text() or ""
            cleaned = clean_text(raw_text)
            word_count = len(re.findall(r"\b\w+\b", cleaned))
            alpha_ratio = compute_alpha_ratio(cleaned)
            score = score_page_text(cleaned, word_count, alpha_ratio)
            pages.append(
                PageText(
                    index=idx,
                    text=cleaned,
                    word_count=word_count,
                    alpha_ratio=alpha_ratio,
                    score=score,
                    source="pdf_text",
                )
            )
            _notify(progress_cb, f"Reading page {idx + 1}/{total_pages}", 10 + int(15 * ((idx + 1) / max(1, total_pages))))

    if not pages:
        return pages

    avg_score = sum(page.score for page in pages) / len(pages)
    if skip_ocr or avg_score >= skip_avg_score or max_ocr_pages <= 0:
        return pages

    ocr_selected_pages(
        pdf_bytes,
        pages,
        [page.index for page in pages],
        progress_cb=progress_cb,
        message_template="OCR page {current}/{total}",
        progress_start=25,
        progress_span=10,
        max_ocr_pages_override=max_ocr_pages_override,
    )
    return pages


def ocr_selected_pages(
    pdf_bytes: bytes,
    pages: List[PageText],
    page_indexes: List[int],
    progress_cb: ProgressCallback = None,
    message_template: str = "OCR page {current}/{total}",
    progress_start: int = 25,
    progress_span: int = 10,
    max_ocr_pages_override: Optional[int] = None,
) -> int:
    min_words = int(os.getenv("OCR_MIN_WORDS", "40"))
    min_alpha_ratio = float(os.getenv("OCR_MIN_ALPHA_RATIO", "0.6"))
    max_ocr_pages = int(os.getenv("OCR_MAX_PAGES", "4"))
    if max_ocr_pages_override is not None:
        max_ocr_pages = max_ocr_pages_override

    selected_pages: List[PageText] = []
    seen: set[int] = set()
    for page_index in page_indexes:
        if page_index in seen or page_index < 0 or page_index >= len(pages):
            continue
        seen.add(page_index)
        page = pages[page_index]
        if should_ocr_page(page.score, page.word_count, page.alpha_ratio, min_words, min_alpha_ratio):
            selected_pages.append(page)

    selected_pages.sort(key=lambda page: page.score)
    selected_pages = selected_pages[:max_ocr_pages]
    if not selected_pages:
        return 0

    improved_pages = 0
    for idx, page in enumerate(selected_pages, start=1):
        percent = progress_start + int(progress_span * (idx / max(1, len(selected_pages))))
        _notify(progress_cb, message_template.format(current=idx, total=len(selected_pages), page=page.index + 1), percent)
        try:
            image = render_pdf_page_image(pdf_bytes, page.index)
            if image is None:
                continue
            ocr_text = clean_text(ocr_page_image(image))
            ocr_word_count = len(re.findall(r"\b\w+\b", ocr_text))
            ocr_alpha_ratio = compute_alpha_ratio(ocr_text)
            ocr_score = score_page_text(ocr_text, ocr_word_count, ocr_alpha_ratio)
            if ocr_score > page.score:
                page.text = ocr_text
                page.word_count = ocr_word_count
                page.alpha_ratio = ocr_alpha_ratio
                page.score = ocr_score
                page.source = "ocr"
                improved_pages += 1
        except Exception:
            continue

    if improved_pages:
        strip_repeated_lines(pages)
    return improved_pages


def build_diagnostics(pages: List[PageText]) -> Dict[str, Any]:
    return {
        "page_sources": [page.source for page in pages],
        "page_quality_scores": [page.score for page in pages],
        "page_snippets": [page.text[:300] for page in pages],
        "ocr_pages": [page.index for page in pages if page.source == "ocr"],
    }


def build_recipe_schema() -> Dict[str, Any]:
    return {
        "name": "recipe_extraction",
        "schema": RecipeExtraction.model_json_schema(),
    }

def _extract_json(raw_content: str) -> Dict[str, Any]:
    if not raw_content:
        raise ValueError("Empty LLM response")
    try:
        return json.loads(raw_content)
    except json.JSONDecodeError:
        start = raw_content.find("{")
        end = raw_content.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw_content[start:end + 1])
        raise


ALLOWED_SOURCES = {"pdf_text", "ocr", "inferred", "computed", "unknown"}


def sanitize_llm_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return data
    recipes = data.get("recipes")
    if not isinstance(recipes, list):
        return data

    for recipe in recipes:
        if not isinstance(recipe, dict):
            continue
        confidence = recipe.get("confidence")
        if isinstance(confidence, dict):
            for key, value in list(confidence.items()):
                if not isinstance(value, (int, float)):
                    confidence[key] = None
        else:
            recipe["confidence"] = None

        sources = recipe.get("sources")
        if isinstance(sources, dict):
            for key, value in list(sources.items()):
                if not isinstance(value, str) or value not in ALLOWED_SOURCES:
                    sources[key] = None
        else:
            recipe["sources"] = None

    return data


def _call_openai_for_text(
    text: str,
    model: str,
    include_instructions: bool,
    max_tokens: Optional[int],
) -> RecipeExtraction:
    client = get_openai_client()

    system_prompt = (
        "You are a data extraction engine for nutrition recipes. "
        "Return JSON matching the provided schema. "
        "If a value is not explicit in the text, use null. "
        "Do not guess missing numeric values. "
        "Provide confidence (0-1) and sources for each field."
    )

    user_prompt = (
        "Extract all recipes from the text below. "
        "Return a JSON object with a 'recipes' array. "
        "Each recipe should include confidence and sources maps keyed by field name. "
        "Use sources: pdf_text, ocr, inferred, computed, unknown. "
        + ("Do not include instructions; set instructions to null. " if not include_instructions else "")
        + "Text:\n\n" + text
    )

    response_format = {
        "type": "json_schema",
        "json_schema": build_recipe_schema(),
    }

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format=response_format,
            temperature=0,
            max_tokens=max_tokens,
        )
        raw_content = response.choices[0].message.content
    except Exception:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=max_tokens,
        )
        raw_content = response.choices[0].message.content

    try:
        data = sanitize_llm_payload(_extract_json(raw_content))
        return RecipeExtraction.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as err:
        repair_prompt = (
            "Fix the JSON to match the schema exactly. "
            "Return only valid JSON. "
            f"Error: {err}\n\nRaw JSON:\n{raw_content}"
        )
        repair_response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": repair_prompt},
            ],
            response_format=response_format,
            temperature=0,
            max_tokens=max_tokens,
        )
        repair_raw = repair_response.choices[0].message.content
        repair_data = sanitize_llm_payload(_extract_json(repair_raw))
        return RecipeExtraction.model_validate(repair_data)


def get_structured_recipes_from_openai(
    text: str,
    model: str,
    include_instructions: bool,
    max_tokens: Optional[int],
) -> RecipeExtraction:
    return _call_openai_for_text(text, model, include_instructions, max_tokens)


def chunk_pages_by_chars(pages: List[str], max_chars: int) -> List[List[str]]:
    chunks: List[List[str]] = []
    current: List[str] = []
    current_len = 0

    for page_text in pages:
        page_len = len(page_text)
        if current_len + page_len > max_chars and current:
            chunks.append(current)
            current = []
            current_len = 0
        current.append(page_text)
        current_len += page_len

    if current:
        chunks.append(current)

    return chunks


def split_text_mid(text: str) -> List[str]:
    parts = [part for part in text.split("\n\n") if part.strip()]
    if len(parts) >= 2:
        mid = len(parts) // 2
        left = "\n\n".join(parts[:mid]).strip()
        right = "\n\n".join(parts[mid:]).strip()
        return [left, right]
    mid = len(text) // 2
    left = text[:mid].strip()
    right = text[mid:].strip()
    return [left, right]


def merge_recipe_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    index: Dict[str, int] = {}

    for recipe in records:
        title = (recipe.get("title") or "").strip().lower()
        if not title:
            merged.append(recipe)
            continue
        if title not in index:
            index[title] = len(merged)
            merged.append(recipe)
            continue

        existing = merged[index[title]]
        for field in FIELD_NAMES:
            if existing.get(field) in (None, "", []):
                if recipe.get(field) not in (None, "", []):
                    existing[field] = recipe.get(field)

        existing_conf = existing.get("confidence") if isinstance(existing.get("confidence"), dict) else {}
        new_conf = recipe.get("confidence") if isinstance(recipe.get("confidence"), dict) else {}
        for key, value in new_conf.items():
            if key not in existing_conf or (value is not None and (existing_conf.get(key) or 0) < value):
                existing_conf[key] = value
        existing["confidence"] = existing_conf

        existing_src = existing.get("sources") if isinstance(existing.get("sources"), dict) else {}
        new_src = recipe.get("sources") if isinstance(recipe.get("sources"), dict) else {}
        for key, value in new_src.items():
            if existing_src.get(key) in (None, "unknown") and value:
                existing_src[key] = value
        existing["sources"] = existing_src

        merged[index[title]] = existing

    return merged


def get_structured_recipes_from_openai_chunked(
    pages: List[str],
    model: str,
    include_instructions: bool,
    max_tokens: Optional[int],
    max_chars: int,
    progress_cb: ProgressCallback = None,
) -> RecipeExtraction:
    chunks = chunk_pages_by_chars(pages, max_chars)
    all_recipes: List[Dict[str, Any]] = []

    idx = 0
    while idx < len(chunks):
        chunk_pages = chunks[idx]
        chunk_text = "\n\n".join(chunk_pages).strip()
        if progress_cb:
            progress = 40 + int(40 * ((idx + 1) / max(1, len(chunks))))
            progress_cb(f"Parsing chunk {idx + 1}/{len(chunks)}", progress)
        try:
            extraction = _call_openai_for_text(chunk_text, model, include_instructions, max_tokens)
            all_recipes.extend([recipe.model_dump() for recipe in extraction.recipes])
            idx += 1
        except Exception as err:
            if not is_retryable_exception(err):
                raise
            if progress_cb:
                progress_cb("Retrying with smaller chunk", 40 + int(40 * ((idx + 1) / max(1, len(chunks)))))
            if len(chunk_pages) > 1:
                mid = len(chunk_pages) // 2
                chunks[idx:idx + 1] = [chunk_pages[:mid], chunk_pages[mid:]]
                continue
            if len(chunk_text) <= 2000:
                raise
            left, right = split_text_mid(chunk_text)
            if not left or not right or left == chunk_text or right == chunk_text:
                raise
            chunks[idx:idx + 1] = [[left], [right]]
            continue

    merged = merge_recipe_records(all_recipes)
    return RecipeExtraction.model_validate({"recipes": merged})


def ensure_meta(recipe: Dict[str, Any]) -> None:
    confidence = recipe.get("confidence")
    sources = recipe.get("sources")
    if not isinstance(confidence, dict):
        confidence = {}
    if not isinstance(sources, dict):
        sources = {}
    for field in FIELD_NAMES:
        confidence.setdefault(field, None)
        sources.setdefault(field, "unknown")
    recipe["confidence"] = confidence
    recipe["sources"] = sources


def apply_bounds(recipe: Dict[str, Any], field: str, value: Optional[int], max_value: int) -> Optional[int]:
    if value is None:
        return None
    if value < 0 or value > max_value:
        recipe["confidence"][field] = min(recipe["confidence"].get(field) or 0.2, 0.2)
        recipe["sources"][field] = "unknown"
        return None
    return value


def post_process_recipes(recipes: List[Dict[str, Any]], include_instructions: bool = True) -> List[Dict[str, Any]]:
    processed: List[Dict[str, Any]] = []
    bounds = {
        "calories": 2000,
        "protein": 300,
        "fat": 300,
        "saturatedFat": 200,
        "fiber": 200,
        "prepTime": 600,
        "ingredientCount": 200,
    }

    for recipe in recipes:
        ensure_meta(recipe)

        ingredients = recipe.get("ingredients")
        if not isinstance(ingredients, list):
            ingredients = []
        recipe["ingredients"] = [str(item).strip() for item in ingredients if str(item).strip()]

        for field in [
            "prepTime",
            "calories",
            "protein",
            "fat",
            "saturatedFat",
            "fiber",
        ]:
            recipe[field] = normalize_numeric_value(recipe.get(field))

        if recipe.get("ingredientCount") is None:
            recipe["ingredientCount"] = len(recipe["ingredients"])
            recipe["sources"]["ingredientCount"] = "computed"
            recipe["confidence"]["ingredientCount"] = 0.7
        else:
            recipe["ingredientCount"] = normalize_numeric_value(recipe.get("ingredientCount"))

        instructions = recipe.get("instructions") or ""
        if not include_instructions:
            recipe["instructions"] = None

        recipe["cookingMethod"] = normalize_cooking_method(recipe.get("cookingMethod"), instructions)
        if recipe.get("sources"):
            if recipe["sources"].get("cookingMethod") in (None, "unknown"):
                recipe["sources"]["cookingMethod"] = "computed"
        if recipe.get("confidence"):
            if recipe["confidence"].get("cookingMethod") is None:
                recipe["confidence"]["cookingMethod"] = 0.6

        for field, max_value in bounds.items():
            recipe[field] = apply_bounds(recipe, field, recipe.get(field), max_value)

        processed.append(recipe)

    return processed


def parse_recipes_pdf(
    pdf_bytes: bytes,
    debug: bool = False,
    mode: str = "accurate",
    progress_cb: ProgressCallback = None,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    mode = (mode or "accurate").lower()
    fast_mode = mode == "fast"
    model_env = "OPENAI_MODEL_FAST" if fast_mode else "OPENAI_MODEL"
    model = os.getenv(model_env, os.getenv("OPENAI_MODEL", "gpt-4o"))
    max_tokens_env = os.getenv("LLM_MAX_TOKENS")
    max_tokens = int(max_tokens_env) if max_tokens_env else None
    include_instructions = os.getenv("INCLUDE_INSTRUCTIONS", "true").lower() == "true"
    if fast_mode:
        include_instructions = False
    max_chars = int(os.getenv("LLM_MAX_CHARS", "12000"))
    if fast_mode:
        max_chars = int(os.getenv("LLM_MAX_CHARS_FAST", str(max_chars)))
    max_pages_env = os.getenv("FAST_MAX_PAGES") if fast_mode else os.getenv("MAX_PAGES")
    max_pages = int(max_pages_env) if max_pages_env else None

    _notify(progress_cb, "Extracting text", 10)
    pages = extract_pdf_text_pages(
        pdf_bytes,
        progress_cb=progress_cb,
        skip_ocr=fast_mode,
        max_ocr_pages_override=2 if fast_mode else None,
    )
    if max_pages and max_pages > 0:
        pages = pages[:max_pages]
    combined_text = "\n\n".join(page.text for page in pages)

    _notify(progress_cb, "Parsing recipes", 40)
    try:
        extraction = get_structured_recipes_from_openai(combined_text, model, include_instructions, max_tokens)
    except Exception:
        extraction = get_structured_recipes_from_openai_chunked(
            [page.text for page in pages],
            model,
            include_instructions,
            max_tokens,
            max_chars,
            progress_cb=progress_cb,
        )
    recipes_raw = [recipe.model_dump() for recipe in extraction.recipes]
    _notify(progress_cb, "Post-processing", 90)
    recipes = post_process_recipes(recipes_raw, include_instructions=include_instructions)

    diagnostics = build_diagnostics(pages) if debug else None
    _notify(progress_cb, "Done", 100)
    return recipes, diagnostics
