# Unit test for each function in the module main.py

import unittest
import os

from main import create_database, create_table, export_table

 # test the creation of a database
class TestCreateDatabase(unittest.TestCase):
		def test_create_database(self):
				path = "test.db"
				db = create_database(path)
				self.assertTrue(os.path.exists(path))
				os.remove(path)

 # test the creation of a table from a list of parquet files
class TestCreateTable(unittest.TestCase):
		def test_create_table(self):
				path = "test.db"
				db = create_database(path)
				files = ["test.parquet"]
				create_table(db, "test", files)
				self.assertTrue(os.path.exists(path))
				os.remove(path)

 # test the export of a table as a parquet file to a path 
class TestExportTable(unittest.TestCase):
		def test_export_table(self):
				path = "test.db"
				db = create_database(path)
				files = ["test.parquet"]
				create_table(db, "test", files)
				export_table(db, "test", "test.parquet", 1, 1)
				self.assertTrue(os.path.exists(path))
				os.remove(path)
				


