
import difflib
import json
import os

from ex_columns import process_structured_file

def open_json(filename):
    with open(filename) as f:
        for line in f:
            return line

cwd = os.getcwd() + '\\' + 'test_files' + '\\'

json_thing = cwd + 'freetext_header.json'

string1 = open_json(json_thing)
file_metadata = str(process_structured_file(cwd + 'freetext')[0])

print(file_metadata)

if string1 == file_metadata:
    print "WOO"
