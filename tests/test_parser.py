import unittest

from parser import (
    infer_cooking_method,
    normalize_numeric_value,
    score_page_text,
    should_ocr_page,
)


class TestParserUtils(unittest.TestCase):
    def test_should_ocr_page_thresholds(self):
        score = score_page_text("short text", word_count=2, alpha_ratio=0.9)
        self.assertTrue(should_ocr_page(score, 2, 0.9, min_words=40, min_alpha_ratio=0.6))

        score_ok = score_page_text("word " * 60, word_count=60, alpha_ratio=0.8)
        self.assertFalse(should_ocr_page(score_ok, 60, 0.8, min_words=40, min_alpha_ratio=0.6))

    def test_infer_cooking_method(self):
        self.assertEqual(infer_cooking_method("Bake in the oven"), "Oven")
        self.assertEqual(infer_cooking_method("Cook in a skillet"), "Stovetop")
        self.assertEqual(infer_cooking_method("Use the air fryer"), "Air Fryer")
        self.assertEqual(infer_cooking_method("Blend until smooth"), "Blender")
        self.assertEqual(infer_cooking_method("No cook option"), "No Cook")
        self.assertEqual(infer_cooking_method("Serve chilled"), "No Cook")

    def test_normalize_numeric_value(self):
        self.assertEqual(normalize_numeric_value("12g"), 12)
        self.assertEqual(normalize_numeric_value("1,234"), 1234)
        self.assertEqual(normalize_numeric_value(10.6), 11)
        self.assertIsNone(normalize_numeric_value("abc"))
        self.assertIsNone(normalize_numeric_value(None))


if __name__ == "__main__":
    unittest.main()
