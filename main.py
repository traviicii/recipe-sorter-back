# Server command
# fastapi dev main.py
import io
import os
import re
import json
import pdfplumber
from openai import OpenAI
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import traceback

from dotenv import load_dotenv
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://recipe-sorter.vercel.app", "https://recipe-sorter-front.onrender.com"],  # You can restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def convert_pdf_to_text(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        raise RuntimeError(f"Failed to extract text from PDF: {e}")

def get_structured_recipes_from_openai(pdf_text: str) -> list:
    prompt = f"""
You are a nutritionist and software engineer helping to extract structured data from recipe PDFs.

Please parse the following raw text into a JSON array of recipe objects with this structure:

[
  {{
    "title": "Recipe Name",
    "prepTime": 10,
    "ingredients": ["item 1", "item 2"],
    "ingredientCount": 2,
    "calories": 300,
    "protein": 25,
    "fat": 10,
    "saturatedFat": 3,
    "fiber": 5,
    "instructions": "Full cooking instructions...",
    "cookingMethod": "Oven"
  }}
]
Within the raw text, there may be keywords missing like "protein" or "fat"- Do your best to reason which numbers belong to which macro nutrient values. If you see a recipe for overnight oats, make the prep time value 0.
Return only the JSON array. Here's the raw text from the PDF:
---
{pdf_text}
---
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )

    raw_content = response.choices[0].message.content.strip()

    # Extract JSON array using regex (robust to markdown wrapping or notes)
    match = re.search(r"\[\s*{.*?}\s*\]", raw_content, re.DOTALL)
    if not match:
        print("\n--- RAW RESPONSE FROM OPENAI ---\n", raw_content)
        raise ValueError("OpenAI output did not contain a valid JSON array.")

    json_text = match.group(0)

    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"OpenAI returned invalid JSON: {e}")

@app.get("/")
async def Home():
    return JSONResponse(content={"status": "ok"})

@app.post("/parse-recipes")
async def parse_recipes_endpoint(file: UploadFile = File(...)):
    if not file.content_type or not file.content_type.endswith("pdf"):
        raise HTTPException(status_code=400, detail="PDF required.")

    try:
        file_data = await file.read()
        pdf_text = convert_pdf_to_text(file_data)

        print("\n\n--- Extracted PDF Text Preview ---\n", pdf_text[:500], "\n--------------------\n")

        structured_recipes = get_structured_recipes_from_openai(pdf_text)

        return JSONResponse(content={"recipes": structured_recipes})

    except Exception as e:
        print("\n\n--- ERROR CAUGHT ---")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Parsing failed: {e}")
