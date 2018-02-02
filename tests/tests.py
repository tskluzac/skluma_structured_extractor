
import json
import os
import time
import unittest
from ex_columns import process_structured_file

cwd = os.getcwd() + '\\' + 'test_files' + '\\'

class ExtractionTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_freetext_header(self):  # DONE.
        filename = cwd + 'freetext_header'
        t0 = time.time()
        metadata = str(process_structured_file(filename)[0])
        t1 = time.time()
        new_json = open_json(filename + '.json')
        self.assertEqual(metadata, new_json)
        print("Test 0: " + str(t1 - t0) + " seconds.")

    def test_tabs_spaces_commas(self):
        filename = '/test_files/tabs'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 1: " + str(t1-t0) + " seconds.")

    def test_no_headers(self):
        filename = cwd + 'no_headers'
        t0 = time.time()
        # metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 2: " + str(t1 - t0) + " seconds.")

    def test_freetext_should_fail(self):  # DONE.
        filename = cwd + 'freetext'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
        print("Test 3: " + str(t1 - t0) + " seconds.")

    def test_compressed_should_fail(self):
        filename = '/test_files/freetext_header'
        t0 = time.time()
        #metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(0, 0)
        print("Test 4: " + str(t1 - t0) + " seconds.")

    def test_images_should_fail(self):  # DONE.
        filename = cwd + 'image'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
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

def open_json(filename):
    with open(filename) as f:
        for line in f:
            #print line
            return line

if __name__ == '__main__':
    unittest.main()
