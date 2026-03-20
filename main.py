# Server command
# fastapi dev main.py
import os
import traceback
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from collection_service import (
    create_or_reuse_collection,
    enrich_collection_job,
    parse_collection_job,
    public_collection,
    public_recipe,
    start_enrichment,
)
from parser import parse_recipes_pdf
from storage import clear_library_storage, load_collection, load_collections, load_job, load_recipes, save_collection


load_dotenv()
app = FastAPI()


DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
    "https://recipe-sorter-front.onrender.com",
]


cors_allow_all = os.getenv("CORS_ALLOW_ALL", "false").lower() == "true"
cors_origins_env = os.getenv(
    "CORS_ORIGINS",
    ",".join(DEFAULT_CORS_ORIGINS),
)
cors_origins = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]
if cors_allow_all:
    cors_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def public_job(job: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if job is None:
        return None
    return {
        "id": job["id"],
        "collectionId": job["collectionId"],
        "type": job["type"],
        "status": job["status"],
        "step": job["step"],
        "progress": job["progress"],
        "message": job["message"],
        "error": job["error"],
        "createdAt": job["createdAt"],
        "updatedAt": job["updatedAt"],
    }


@app.get("/")
async def home() -> JSONResponse:
    return JSONResponse(content={"status": "ok"})


@app.post("/parse-recipes")
async def parse_recipes_endpoint(
    file: UploadFile = File(...),
    debug: bool = Query(False, description="Include extraction diagnostics"),
    mode: str = Query("accurate", description="Legacy parsing mode"),
) -> JSONResponse:
    if not file.content_type or not file.content_type.endswith("pdf"):
        raise HTTPException(status_code=400, detail="PDF required.")

    try:
        file_data = await file.read()
        recipes, diagnostics = parse_recipes_pdf(file_data, debug=debug, mode=mode)
        payload: Dict[str, Any] = {"recipes": recipes}
        if debug and diagnostics is not None:
            payload["diagnostics"] = diagnostics
        return JSONResponse(content=payload)
    except Exception as exc:
        print("\n\n--- LEGACY PARSE ERROR ---")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Parsing failed: {exc}")


@app.post("/collections")
async def create_collection(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
) -> JSONResponse:
    if not file.content_type or not file.content_type.endswith("pdf"):
        raise HTTPException(status_code=400, detail="PDF required.")

    try:
        file_data = await file.read()
        collection, job, reused = create_or_reuse_collection(file.filename or "recipe-collection.pdf", file_data)
        collection_data = public_collection(collection)

        if not reused and job is not None:
            background_tasks.add_task(parse_collection_job, collection["id"], job["id"])

        return JSONResponse(
            content={
                "collection": collection_data,
                "job": public_job(job),
                "reused": reused,
            }
        )
    except Exception as exc:
        print("\n\n--- COLLECTION CREATE ERROR ---")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Collection creation failed: {exc}")


@app.get("/collections")
async def list_all_collections() -> JSONResponse:
    collections = [public_collection(collection) for collection in load_collections()]
    return JSONResponse(content={"collections": collections})


@app.delete("/collections")
async def clear_collections() -> JSONResponse:
    collections = load_collections()
    active = [
        collection
        for collection in collections
        if collection.get("status") in {"queued", "processing"}
    ]
    if active:
        raise HTTPException(
            status_code=409,
            detail="Wait for active parsing to finish before clearing the library.",
        )

    clear_library_storage()
    return JSONResponse(content={"cleared": True})


@app.get("/collections/{collection_id}")
async def get_collection(collection_id: str) -> JSONResponse:
    collection = load_collection(collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found.")
    job = load_job(collection.get("lastJobId")) if collection.get("lastJobId") else None
    return JSONResponse(content={"collection": public_collection(collection), "job": public_job(job)})


@app.get("/recipes")
async def get_library_recipes(
    collection_ids: str | None = Query(None, alias="collectionIds"),
) -> JSONResponse:
    selected_ids = {
        item.strip()
        for item in (collection_ids or "").split(",")
        if item.strip()
    }
    collections = load_collections()
    if selected_ids:
        collections = [collection for collection in collections if collection.get("id") in selected_ids]

    recipes = []
    for collection in collections:
        recipes.extend(public_recipe(recipe, collection) for recipe in load_recipes(collection["id"]))
    return JSONResponse(content={"recipes": recipes})


@app.get("/collections/{collection_id}/recipes")
async def get_collection_recipes(collection_id: str) -> JSONResponse:
    collection = load_collection(collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found.")
    recipes = [public_recipe(recipe, collection) for recipe in load_recipes(collection_id)]
    return JSONResponse(content={"recipes": recipes})


@app.post("/collections/{collection_id}/enrich")
async def enrich_collection(collection_id: str, background_tasks: BackgroundTasks) -> JSONResponse:
    collection = load_collection(collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found.")
    if collection.get("macroStatus") != "complete":
        raise HTTPException(status_code=409, detail="Macro parsing is not complete yet.")

    if collection.get("ingredientStatus") == "complete":
        job = load_job(collection.get("lastJobId")) if collection.get("lastJobId") else None
        return JSONResponse(
            content={
                "collection": public_collection(collection),
                "job": public_job(job),
                "started": False,
            }
        )

    job = start_enrichment(collection_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Collection not found.")

    collection["ingredientStatus"] = "queued"
    collection["lastJobId"] = job["id"]
    save_collection(collection)
    background_tasks.add_task(enrich_collection_job, collection_id, job["id"])

    return JSONResponse(
        content={
            "collection": public_collection(collection),
            "job": public_job(job),
            "started": True,
        }
    )
