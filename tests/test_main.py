import unittest

from fastapi.testclient import TestClient

from main import app


class TestCors(unittest.TestCase):
    def test_delete_preflight_allows_hosted_frontend_origin(self):
        client = TestClient(app)

        response = client.options(
            "/collections",
            headers={
                "Origin": "https://recipe-sorter-front.onrender.com",
                "Access-Control-Request-Method": "DELETE",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            response.headers.get("access-control-allow-origin"),
            {"*", "https://recipe-sorter-front.onrender.com"},
        )
        self.assertIn("DELETE", response.headers.get("access-control-allow-methods", ""))


if __name__ == "__main__":
    unittest.main()
