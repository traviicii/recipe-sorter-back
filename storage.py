import hashlib
import json
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
COLLECTIONS_DIR = DATA_DIR / "collections"
JOBS_DIR = DATA_DIR / "jobs"
HASH_INDEX_FILE = DATA_DIR / "hash_index.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_storage_dirs() -> None:
    COLLECTIONS_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _clone_default(default: Any) -> Any:
    if isinstance(default, (dict, list)):
        return deepcopy(default)
    return default


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return _clone_default(default)
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return _clone_default(default)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp_path.replace(path)


def compute_pdf_hash(pdf_bytes: bytes) -> str:
    return hashlib.sha256(pdf_bytes).hexdigest()


def collection_hash_key(pdf_hash: str, parser_version: str) -> str:
    return f"{pdf_hash}:{parser_version}"


def load_hash_index() -> Dict[str, str]:
    ensure_storage_dirs()
    return read_json(HASH_INDEX_FILE, {})


def save_hash_index(index: Dict[str, str]) -> None:
    ensure_storage_dirs()
    write_json(HASH_INDEX_FILE, index)


def collection_dir(collection_id: str) -> Path:
    return COLLECTIONS_DIR / collection_id


def collection_file(collection_id: str) -> Path:
    return collection_dir(collection_id) / "collection.json"


def recipes_file(collection_id: str) -> Path:
    return collection_dir(collection_id) / "recipes.json"


def blocks_file(collection_id: str) -> Path:
    return collection_dir(collection_id) / "blocks.json"


def pdf_file(collection_id: str) -> Path:
    return collection_dir(collection_id) / "source.pdf"


def job_file(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def create_collection_record(source_filename: str, pdf_hash: str, parser_version: str) -> Dict[str, Any]:
    collection_id = str(uuid4())
    timestamp = utc_now_iso()
    return {
        "id": collection_id,
        "name": Path(source_filename).stem,
        "sourceFilename": source_filename,
        "pdfHash": pdf_hash,
        "parserVersion": parser_version,
        "status": "queued",
        "step": "queued",
        "progress": 0,
        "macroStatus": "queued",
        "ingredientStatus": "idle",
        "pageCount": 0,
        "totalBlocks": 0,
        "totalRecipes": 0,
        "parsedRecipes": 0,
        "completeRecipes": 0,
        "partialRecipes": 0,
        "failedRecipes": 0,
        "ocrRecipes": 0,
        "enrichedRecipes": 0,
        "reused": False,
        "lastJobId": None,
        "message": "Queued",
        "error": None,
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }


def save_collection(collection: Dict[str, Any]) -> Dict[str, Any]:
    ensure_storage_dirs()
    collection["updatedAt"] = utc_now_iso()
    write_json(collection_file(collection["id"]), collection)
    return collection


def load_collection(collection_id: str) -> Optional[Dict[str, Any]]:
    ensure_storage_dirs()
    path = collection_file(collection_id)
    if not path.exists():
        return None
    return read_json(path, None)


def load_collections() -> List[Dict[str, Any]]:
    ensure_storage_dirs()
    collections: List[Dict[str, Any]] = []
    for path in COLLECTIONS_DIR.glob("*/collection.json"):
        record = read_json(path, None)
        if record:
            collections.append(record)
    collections.sort(key=lambda item: item.get("updatedAt", ""), reverse=True)
    return collections


def save_pdf_bytes(collection_id: str, pdf_bytes: bytes) -> str:
    ensure_storage_dirs()
    path = pdf_file(collection_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pdf_bytes)
    return str(path)


def load_pdf_bytes(collection_id: str) -> bytes:
    return pdf_file(collection_id).read_bytes()


def save_blocks(collection_id: str, blocks: List[Dict[str, Any]]) -> None:
    write_json(blocks_file(collection_id), blocks)


def load_blocks(collection_id: str) -> List[Dict[str, Any]]:
    return read_json(blocks_file(collection_id), [])


def load_recipes(collection_id: str) -> List[Dict[str, Any]]:
    return read_json(recipes_file(collection_id), [])


def save_recipes(collection_id: str, recipes: List[Dict[str, Any]]) -> None:
    write_json(recipes_file(collection_id), recipes)


def upsert_recipe(collection_id: str, recipe: Dict[str, Any]) -> None:
    recipes = load_recipes(collection_id)
    for index, existing in enumerate(recipes):
        if existing.get("id") == recipe.get("id") or (
            existing.get("blockId") and existing.get("blockId") == recipe.get("blockId")
        ):
            recipes[index] = recipe
            save_recipes(collection_id, recipes)
            return
    recipes.append(recipe)
    save_recipes(collection_id, recipes)


def create_job(collection_id: str, job_type: str) -> Dict[str, Any]:
    ensure_storage_dirs()
    job = {
        "id": str(uuid4()),
        "collectionId": collection_id,
        "type": job_type,
        "status": "queued",
        "step": "queued",
        "progress": 0,
        "message": "Queued",
        "error": None,
        "createdAt": utc_now_iso(),
        "updatedAt": utc_now_iso(),
    }
    save_job(job)
    return job


def save_job(job: Dict[str, Any]) -> Dict[str, Any]:
    ensure_storage_dirs()
    job["updatedAt"] = utc_now_iso()
    write_json(job_file(job["id"]), job)
    return job


def load_job(job_id: str) -> Optional[Dict[str, Any]]:
    path = job_file(job_id)
    if not path.exists():
        return None
    return read_json(path, None)


def find_collection_by_hash(pdf_hash: str, parser_version: str) -> Optional[Dict[str, Any]]:
    index = load_hash_index()
    collection_id = index.get(collection_hash_key(pdf_hash, parser_version))
    if not collection_id:
        return None
    return load_collection(collection_id)


def register_collection_hash(collection_id: str, pdf_hash: str, parser_version: str) -> None:
    index = load_hash_index()
    index[collection_hash_key(pdf_hash, parser_version)] = collection_id
    save_hash_index(index)


def clear_library_storage() -> None:
    if COLLECTIONS_DIR.exists():
        shutil.rmtree(COLLECTIONS_DIR)
    if JOBS_DIR.exists():
        shutil.rmtree(JOBS_DIR)
    if HASH_INDEX_FILE.exists():
        HASH_INDEX_FILE.unlink()
    ensure_storage_dirs()
