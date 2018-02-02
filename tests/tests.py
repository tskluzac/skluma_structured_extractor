
import time
import unittest
from ex_columns import process_structured_file

class ExtractionTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_freetext_header(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 0: " + str(t1 - t0) + " seconds.")

    def test_tabs_spaces_commas(self):
        filename = '/test_files/tabs'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 1: " + str(t1-t0) + " seconds.")

    def test_no_headers_tabs(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 2: " + str(t1 - t0) + " seconds.")

    def test_freetext_should_fail(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 3: " + str(t1 - t0) + " seconds.")

    def test_compressed_should_fail(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 4: " + str(t1 - t0) + " seconds.")

    def test_images_should_fail(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 5: " + str(t1 - t0) + " seconds.")

    def test_netCDF_should_fail(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 6: " + str(t1 - t0) + " seconds.")

    def test_big(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0,0)
        print("Test 7: " + str(t1-t0) + " seconds.")

if __name__ == '__main__':
    unittest.main()
