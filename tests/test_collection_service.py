import unittest
from types import SimpleNamespace

from collection_service import (
    enrich_recipe_ingredients,
    parse_macro_recipe_block,
    segment_recipe_blocks,
    summarize_recipes,
)


PAGE_TEXT = """https://bevictoriouscoaching.com/
Blackberry Kefir Smoothie
5 minutes
Plain Kefir
Add all of the ingredients to a blender and blend until smooth. Enjoy!
Blackberries (fresh or frozen)
Vanilla Protein Powder
Hemp Seeds
Best enjoyed immediately.
One serving is approximately 1 1/2 cups.
Add a handful of baby spinach or kale.
411
10g
Saturated 3g
Trans 0g
Polyunsaturated 4g
Monounsaturated 1g
42g
Fiber 9g
Sugar 33g
42g
Sodium 295mg
"""

PAGE_WITH_NUTRITION_CUES = """https://bevictoriouscoaching.com/
Vanilla Yogurt Bowl
Greek Yogurt
Blueberries
Almond Butter
325
14g
Saturated 2g
Fiber 6g
28g
Sodium 180mg
"""


class TestCollectionService(unittest.TestCase):
    def test_segments_single_recipe_page(self):
        pages = [
            SimpleNamespace(text="https://bevictoriouscoaching.com/"),
            SimpleNamespace(text=PAGE_TEXT),
        ]

        blocks = segment_recipe_blocks(pages)

        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].title, "Blackberry Kefir Smoothie")

    def test_parses_macro_first_recipe_block(self):
        block = segment_recipe_blocks([SimpleNamespace(text=PAGE_TEXT)])[0]

        recipe = parse_macro_recipe_block(block, use_llm_fallback=False)

        self.assertEqual(recipe["title"], "Blackberry Kefir Smoothie")
        self.assertEqual(recipe["prepTime"], 5)
        self.assertEqual(recipe["calories"], 411)
        self.assertEqual(recipe["fat"], 10)
        self.assertEqual(recipe["saturatedFat"], 3)
        self.assertEqual(recipe["fiber"], 9)
        self.assertEqual(recipe["protein"], 42)
        self.assertEqual(recipe["ingredientCount"], 4)
        self.assertEqual(recipe["macroStatus"], "complete")

    def test_enriches_ingredients_from_candidates(self):
        block = segment_recipe_blocks([SimpleNamespace(text=PAGE_TEXT)])[0]
        recipe = parse_macro_recipe_block(block, use_llm_fallback=False)

        enriched = enrich_recipe_ingredients(recipe)

        self.assertEqual(enriched["ingredientStatus"], "complete")
        self.assertEqual(
            enriched["ingredients"],
            [
                "Plain Kefir",
                "Blackberries (fresh or frozen)",
                "Vanilla Protein Powder",
                "Hemp Seeds",
            ],
        )

    def test_segments_recipe_with_nutrition_cues_without_duration(self):
        blocks = segment_recipe_blocks([SimpleNamespace(text=PAGE_WITH_NUTRITION_CUES)])

        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].title, "Vanilla Yogurt Bowl")

    def test_summarizes_recipe_status_counts(self):
        summary = summarize_recipes(
            [
                {"macroStatus": "complete", "ocrUsed": False},
                {"macroStatus": "partial", "ocrUsed": True},
                {"macroStatus": "failed", "ocrUsed": False},
            ]
        )

        self.assertEqual(summary["parsedRecipes"], 2)
        self.assertEqual(summary["completeRecipes"], 1)
        self.assertEqual(summary["partialRecipes"], 1)
        self.assertEqual(summary["failedRecipes"], 1)
        self.assertEqual(summary["ocrRecipes"], 1)


if __name__ == "__main__":
    unittest.main()
