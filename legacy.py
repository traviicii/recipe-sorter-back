# from typing import Union

# import re
# import io
# import PyPDF2
# from fastapi import FastAPI, File, UploadFile, HTTPException
# from fastapi.responses import JSONResponse
# from fastapi.middleware.cors import CORSMiddleware

# # Server command
# # fastapi dev main.py

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # can restrict to frontend domain specifically ["https://your-frontend-domain.com"]
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# def extract_text_from_pdf(file_data: bytes) -> str:
#     pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_data))
#     full_text = ""
#     for page in pdf_reader.pages:
#         text = page.extract_text()
#         if text:
#             full_text += text + "\n"
#     return full_text

# def determine_cooking_method(instructions: str) -> str:
#     lower_text = instructions.lower()
#     if "oven" in lower_text:
#         return "Oven"
#     elif "stovetop" in lower_text or "skillet" in lower_text:
#         return "Stovetop"
#     elif "air fryer" in lower_text:
#         return "Air Fryer"
#     elif "blender" in lower_text:
#         return "Blender"
#     elif "no cook" in lower_text or "raw" in lower_text:
#         return "No Cook"
#     else:
#         # If none match, could either return a default or "Other"
#         return "Other"

# def parse_recipes(full_text: str) -> list:
#     recipes = []
#     # Split on the URL delimiter.
#     blocks = full_text.split("https://bevictoriouscoaching.com/")
#     blocks = [block.strip() for block in blocks if block.strip()]
    
#     for block in blocks:
#         lines = [line.strip() for line in block.splitlines() if line.strip()]
#         if len(lines) < 6:
#             continue

#         title = lines[0]
#         prep_time_str = lines[1]
#         try:
#             prepTime = int(prep_time_str.split()[0])
#         except ValueError:
#             prepTime = 0

#         ingredient_lines = []
#         i = 2
#         while i < len(lines) and not lines[i].isdigit():
#             ingredient_lines.append(lines[i])
#             i += 1

#         if i >= len(lines):
#             continue

#         try:
#             calories = int(lines[i])
#         except ValueError:
#             calories = 0
#         i += 1

#         protein_str = lines[i].replace("g", "").strip() if i < len(lines) else "0"
#         try:
#             protein = int(protein_str)
#         except ValueError:
#             protein = 0
#         i += 1

#         def get_nutrition(label: str, idx: int):
#             if idx < len(lines) and lines[idx].lower().startswith(label.lower()):
#                 if idx + 1 < len(lines):
#                     val_str = lines[idx + 1].replace("g", "").strip()
#                     try:
#                         return int(val_str), idx + 2
#                     except ValueError:
#                         return 0, idx + 2
#             return 0, idx

#         saturatedFat, i = get_nutrition("Saturated", i)
#         transFat, i = get_nutrition("Trans", i)
#         polyFat, i = get_nutrition("Polyunsaturated", i)
#         monoFat, i = get_nutrition("Monounsaturated", i)

#         fat = 0
#         if i < len(lines) and re.match(r"^\d+g$", lines[i]):
#             try:
#                 fat = int(lines[i].replace("g", ""))
#             except ValueError:
#                 fat = 0
#             i += 1

#         fiber, i = get_nutrition("Fiber", i)
        
#         nutritionLabels = {"Sugar", "Sodium", "Calcium", "Iron", "Folate", "Zinc", "Selenium"}
#         numeric_pattern = re.compile(r"^\d+(?:g|mg|µg)$")
#         while i < len(lines) and (lines[i] in nutritionLabels or numeric_pattern.match(lines[i])):
#             i += 1

#         full_instructions = "\n".join(lines[i:]) if i < len(lines) else ""
#         simple_method = determine_cooking_method(full_instructions)
        
#         recipe = {
#             "title": title,
#             "prepTime": prepTime,
#             "ingredients": ingredient_lines,
#             "ingredientCount": len(ingredient_lines),
#             "calories": calories,
#             "protein": protein,
#             "saturatedFat": saturatedFat,
#             "transFat": transFat,
#             "polyFat": polyFat,
#             "monoFat": monoFat,
#             "fat": fat,
#             "fiber": fiber,
#             "instructions": full_instructions,
#             "cookingMethod": simple_method,  # This field now matches your filtering options.
#         }
#         recipes.append(recipe)
    
#     return recipes

# @app.post("/parse-recipes")
# async def parse_recipes_endpoint(file: UploadFile = File(...)):
#     print("Received file:", file, type(file))
#     if not hasattr(file, "content_type"):
#         raise HTTPException(status_code=400, detail="Uploaded file is not valid.")
#     if file.content_type != "application/pdf":
#         raise HTTPException(status_code=400, detail="Invalid file type. PDF required.")
    
#     try:
#         file_data = await file.read()
#         full_text = extract_text_from_pdf(file_data)
#         recipes_data = parse_recipes(full_text)
#         # print("Full Text: ", full_text)
#         # print("Recipe Data: ", recipes_data)
#         if not recipes_data:
#             raise ValueError("No recipes found; please check the PDF format.")
#     except Exception as e:
#         print("Error processing PDF:", e)
#         raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")
    
#     return JSONResponse(content={"recipes": recipes_data})