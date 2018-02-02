import unittest

class ExtractionTests(unittest.TestCase):

    def setUp(self):
        pass


    def tabs_spaces_commas(self):
        self.assertEqual(0,0)
        print(1)

    def test_no_headers_tabs(self):
        self.assertEqual(0,0)
        print(2)

    def should_fail(self):
        self.assertEqual(0,0)
        print(3)


if __name__ == '__main__':
    unittest.main()
