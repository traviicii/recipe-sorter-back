import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import storage


class TestStorage(unittest.TestCase):
    def test_clear_library_storage_removes_saved_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            data_dir = base_dir / "data"
            collections_dir = data_dir / "collections"
            jobs_dir = data_dir / "jobs"
            hash_index_file = data_dir / "hash_index.json"

            with (
                patch.object(storage, "DATA_DIR", data_dir),
                patch.object(storage, "COLLECTIONS_DIR", collections_dir),
                patch.object(storage, "JOBS_DIR", jobs_dir),
                patch.object(storage, "HASH_INDEX_FILE", hash_index_file),
            ):
                storage.ensure_storage_dirs()
                (collections_dir / "abc").mkdir(parents=True, exist_ok=True)
                (collections_dir / "abc" / "collection.json").write_text("{}")
                (jobs_dir / "job.json").write_text("{}")
                hash_index_file.write_text("{}")

                storage.clear_library_storage()

                self.assertTrue(collections_dir.exists())
                self.assertTrue(jobs_dir.exists())
                self.assertEqual(list(collections_dir.iterdir()), [])
                self.assertEqual(list(jobs_dir.iterdir()), [])
                self.assertFalse(hash_index_file.exists())


if __name__ == "__main__":
    unittest.main()
