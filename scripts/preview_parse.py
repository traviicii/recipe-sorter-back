import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

base_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(base_dir))
load_dotenv(base_dir / ".env")

from collection_service import (
    enrich_recipe_ingredients,
    parse_macro_recipe_block,
    public_recipe,
    segment_recipe_blocks,
)
from parser import extract_pdf_text_pages

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "samples"
ARTIFACTS_DIR = Path(__file__).resolve().parents[1] / "artifacts"


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    pdf_files = sorted(SAMPLES_DIR.glob("*.pdf"))

    if not pdf_files:
        print(f"No PDFs found in {SAMPLES_DIR}")
        return

    last_progress = {"percent": -1, "message": ""}

    def progress_cb(message: str, percent: int) -> None:
        if percent != last_progress["percent"] or message != last_progress["message"]:
            print(f"[{percent:3d}%] {message}")
            last_progress["percent"] = percent
            last_progress["message"] = message

    for pdf_path in pdf_files:
        data = pdf_path.read_bytes()
        skip_ocr = env_flag("SKIP_OCR", False)
        enrich_ingredients = env_flag("ENRICH_INGREDIENTS", False)
        use_llm_fallback = env_flag("USE_LLM_FALLBACK", False)

        progress_cb("Extracting pages", 5)
        pages = extract_pdf_text_pages(data, progress_cb=progress_cb, skip_ocr=skip_ocr)
        progress_cb("Segmenting recipe blocks", 30)
        blocks = segment_recipe_blocks(pages, progress_cb=progress_cb)

        recipes = []
        for index, block in enumerate(blocks, start=1):
            percent = 40 + int(55 * (index / max(1, len(blocks))))
            progress_cb(f"Parsing recipe {index}/{max(1, len(blocks))}", percent)
            recipe = parse_macro_recipe_block(block, use_llm_fallback=use_llm_fallback)
            if enrich_ingredients:
                recipe = enrich_recipe_ingredients(recipe)
            recipes.append(public_recipe(recipe))

        progress_cb("Writing artifacts", 98)
        output = {
            "pageCount": len(pages),
            "blockCount": len(blocks),
            "recipes": recipes,
        }

        out_path = ARTIFACTS_DIR / f"{pdf_path.stem}.json"
        out_path.write_text(json.dumps(output, indent=2))
        progress_cb("Done", 100)

        print(f"{pdf_path.name}: {len(recipes)} recipes")
        for recipe in recipes[:5]:
            title = recipe.get("title") or "(untitled)"
            calories = recipe.get("calories")
            protein = recipe.get("protein")
            method = recipe.get("cookingMethod")
            print(f"- {title} | calories: {calories} | protein: {protein} | method: {method}")
        if len(recipes) > 5:
            print(f"  ... {len(recipes) - 5} more")


if __name__ == "__main__":
    main()
