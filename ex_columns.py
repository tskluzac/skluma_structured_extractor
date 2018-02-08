
"""
    This module extracts metadata from samples deemed 'structured' by the file sampling module. One should be able to
    input any file and this module will return a bunch of structured metdata, including null inference* as a dictionary.  

    Authors: Paul Beckman and Tyler J. Skluzacek 
    Last Edited: 01/21/2018
"""

from multiprocessing import Pool
import os
import re
# import StringIO #TODO: Make work in Python3.

from decimal import Decimal
from heapq import nsmallest, nlargest
import pandas as pd
import pickle as pkl
import struct_utils


class ExtractionFailed(Exception):
    """Basic error to throw when an extractor fails"""


class ExtractionPassed(Exception):
    """Indicator to throw when extractor passes for fast file classification"""


NULL_EPSILON = 1


def extract_columnar_metadata(data, pass_fail=False, lda_preamble=False, null_inference=False, nulls=None):
    """Get metadata from column-formatted file.
            :param file_handle: (file) open file
            :param pass_fail: (bool) whether to exit after ascertaining file class
            :param lda_preamble: (bool) whether to collect the free-text preamble at the start of the file
            :param null_inference: (bool) whether to use the null inference model to remove nulls
            :param nulls: (list(int)) list of null indices
            :returns: (dict) ascertained metadata
            :raises: (ExtractionFailed) if the file cannot be read as a columnar file"""

    # pool = Pool(processes = 2)
    # result = pool.map(doubler, numbers)

    # for m_item in result:
    #   print(m_item)

    try:

        with open(data, 'rU') as data2:

            header_info = get_header_info(data2, delim=',')  #TODO: use max-fields of ',', ' ', or '\t'???
            freetext_offset = header_info[0]
            header_col_labels = header_info[1]
            line_count = header_info[2]


            # TODO: TYLER -- start here.
            if header_col_labels != None:
                dataframes = get_dataframes(data2, header=header_col_labels)

            else:  # elif header_col_labels == None.
                dataframes = get_dataframes(data2, header=None)


        return _extract_columnar_metadata(
            data, ",",
            pass_fail=pass_fail, lda_preamble=lda_preamble, null_inference=null_inference, nulls=nulls
        )
    except ExtractionFailed:
        try:
            return _extract_columnar_metadata(
                data, "\t",
                pass_fail=pass_fail, lda_preamble=lda_preamble, null_inference=null_inference, nulls=nulls
            )
        except ExtractionFailed:
            try:
                return _extract_columnar_metadata(
                    data, " ",
                    pass_fail=pass_fail, lda_preamble=lda_preamble, null_inference=null_inference, nulls=nulls
                )

            except:
                pass


def _extract_columnar_metadata(data, delimiter, pass_fail=False, lda_preamble=False,
                               null_inference=False, nulls=None):
    """helper method for extract_columnar_metadata that uses a specific delimiter."""
    reverse_reader = [struct_utils.fields(line, delimiter) for line in data] [::-1]  #reversed(file_handle.readlines())
    # base dictionary in which to store all the metadata
    metadata = {"columns": {}}

    # minimum number of rows to be considered an extractable table
    min_rows = 5
    # number of rows used to generate aggregates for the null inference model
    ni_rows = 100
    # number of rows to skip at the end of the file before reading
    end_rows = 5

    headers = []
    col_types = []
    col_aliases = []
    num_rows = 0
    # used to check if all rows are the same length, if not, this is not a valid columnar file
    row_length = 0
    is_first_row = True
    # fully_parsed = True

    # save the last `end_rows` rows to try to parse them later
    # if there are less than `end_rows` rows, you must catch the StopIteration exception
    last_rows = reverse_reader[0:end_rows]
    #try:
    #    last_rows = [reverse_reader.next() for i in range(0, end_rows)]
    #except StopIteration:
    #    pass
    # now we try to extract a table from the remaining n-`end_rows` rows
    for row in reverse_reader[end_rows:] :
        # if row is not the same length as previous row, raise an error showing this is not a valid columnar file
        if not is_first_row and row_length != len(row):
            # tables are not worth extracting if under this row threshold
            if num_rows < min_rows:
                raise ExtractionFailed
            else:
                # show that extract failed before we reached the beginning of the file
                fully_parsed = False
                break
        # update row length for next check
        row_length = len(row)
        if is_first_row:
            # make column aliases so that we can create aggregates even for unlabelled columns
            col_aliases = ["__{}__".format(i) for i in range(0, row_length)]
            # type check the first row to decide which aggregates to use
            col_types = ["num" if struct_utils.is_number(field) else "str" for field in row]
            is_first_row = False
        # if the row is a header row, add all its fields to the headers list
        if struct_utils.is_header_row(row):
            # tables are likely not representative of the file if under this row threshold, don't extract metadata
            if num_rows < min_rows:
                raise ExtractionFailed
            # set the column aliases to the most recent header row if they are unique
            # we do this because the most accurate headers usually appear first in the file after the preamble
            if len(set(row)) == len(row):
                for i in range(0, len(row)):
                    metadata["columns"][row[i].lower()] = metadata["columns"].pop(col_aliases[i])
                col_aliases = [header.lower() for header in row]

            for header in row:
                if header != "":
                    headers.append(header.lower())

        else:  # is a row of values
            num_rows += 1
            if not pass_fail:
                struct_utils.add_row_to_aggregates(metadata, row, col_aliases, col_types, nulls=nulls)

        if pass_fail and num_rows >= min_rows:
            raise ExtractionPassed

        # we've taken enough rows to use aggregates for null inference
            #TODO: Add back null inference (1 of 2)
        if null_inference and num_rows >= ni_rows:
            struct_utils.add_final_aggregates(metadata, col_aliases, col_types, num_rows)
            return _extract_columnar_metadata(file_handle, delimiter, pass_fail=pass_fail,
                                              lda_preamble=lda_preamble,
                                              null_inference=False,
                                              nulls=struct_utils.inferred_nulls(metadata)) #This commented out in struct
    # extraction passed but there are too few rows
    if num_rows < min_rows:
        raise ExtractionFailed
    # add the originally skipped rows into the aggregates
    for row in last_rows:
        if len(row) == row_length:
            struct_utils.add_row_to_aggregates(metadata, row, col_aliases, col_types, nulls=nulls)
    struct_utils.add_final_aggregates(metadata, col_aliases, col_types, num_rows)
    # add header list to metadata
    if len(headers) > 0:
        metadata["headers"] = list(set(headers))
    # we've parsed the whole table, now do null inference
    #TODO: Add back inferred nulls.
    if null_inference:
        try:
            return _extract_columnar_metadata(file_handle, delimiter, pass_fail=pass_fail,
                                              lda_preamble=lda_preamble,
                                              null_inference=False,
                                              nulls=struct_utils.inferred_nulls(metadata))
        except:
            return _extract_columnar_metadata(file_handle, delimiter, pass_fail=pass_fail,
                                              lda_preamble=lda_preamble,
                                              null_inference=False,
                                              nulls=None)


    # remove empty string aggregates that were placeholders in null inference
    for key in metadata["columns"].keys():
        if metadata["columns"][key] == {}:
            metadata["columns"].pop(key)

    #print(metadata)
    return metadata


# TODO: Can I do this without reopening the file?
def get_dataframes(filename, header, delim, file_pointer, skiprows = 0, dataframe_size = 1000):

    header = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"] #TODO: Un-hardcode this. Should get list of header_nms.

    iter_csv = pd.read_csv(filename, sep=delim, chunksize=100, header=None, skiprows=file_pointer)

    return iter_csv


# TODO: Currently assuming short freetext headers. This will take some time for long one (full re-reads)
def get_header_info(data, delim):
    # Get a line count.
    line_count = 0
    for line in data:
        line_count += 1
    # Figure out the length of file via binary search (in"seek_preamble")
    if line_count >= 5: #set arbitrary min value or bin-search not useful.
        # A. Get the length of the preamble.
        preamble_length = seek_preamble(data, delim, line_count)
        # B. Determine whether the next line is a freetext header
        data.seek(0)
        for i, line in enumerate(data):
            if i == preamble_length+1:  # +1 since that's one after the preamble.
                print("The header row is: " + str(line))
                header = struct_utils.is_header_row(struct_utils.fields(line, delim))
                if header == True:
                    header = struct_utils.fields(line,delim)
                else:
                    header = None

            elif i > preamble_length:
                break
        return (preamble_length, header, line_count)


def seek_preamble(data, delim, start_point, prev_val=0, last_move=None): #TODO: check last delim finding w/ new one.
    """
    Takes an open file_handle and num_lines in file; returns last line of freetext header.
    :param data -- open file handle
    :param delim -- delimiter to try
    :param start_point -- length of file #TODO: Rename
    :param prev_val -- the 'top' of where we look #TODO: Rename.
    :returns None if no ft header, last line of header if ft header.
    """
    if start_point - prev_val <= 1:
        data.seek(0)
        line_counts = []
        for i, line in enumerate(data):
            if i in range(prev_val-5, prev_val+5):
                line_counts.append((i, len(line.split(delim))))
            elif i > prev_val + 5:
                break
        ### Now walk through the lines to find the line with less freetext data than others.
        line_counts.reverse()
        last_count = line_counts[0][1]
        for item in line_counts:
            if item[1] == last_count:
                last_count = item[1]
                pass
            else:
                print("Should obviously change the answer")
                return item[0]
    else:
        midpoint = int((start_point+prev_val)/2)
        split_vals = []

        # Get midpoint-1, midpoint, midpoint+1 lines.
        data.seek(0)
        for i, line in enumerate(data):
            if i == midpoint-1 or i == midpoint or i == midpoint + 1:
                split_vals.append(len(line.split(delim)))  # Append the number of split items.
            elif i > midpoint + 1:
                break
        # Check if all three values are identical.
        if split_vals.count(split_vals[0]) == len(split_vals):
            # Move UP the file.
            return(seek_preamble(data, delim, midpoint, prev_val, "UP"))
        else:
            # Move DOWN the file.
            return(seek_preamble(data, delim, start_point, midpoint, "DOWN"))


def ni_data(metadata):
    """Format metadata into a 2D array so that it can be input to the null inference model.
    Columns are:
    [
        "min_1", "min_diff_1", "min_2", "min_diff_1", "min_3",
        "max_1", "max_diff_1", "max_2", "max_diff_1", "max_3",
        "avg"
    ]
        :param metadata: (dict) metadata dictionary containing aggregates
        :returns: (list(list(num))) a 2D array of data"""

    data = [
        [
            col_agg["min"][0] if "min" in col_agg.keys() and len(col_agg["min"]) > 0 else 0,
            col_agg["min"][1] - col_agg["min"][0] if "min" in col_agg.keys() and len(col_agg["min"]) > 1 else 0,
            col_agg["min"][1] if "min" in col_agg.keys() and len(col_agg["min"]) > 1 else 0,
            col_agg["min"][2] - col_agg["min"][1] if "min" in col_agg.keys() and len(col_agg["min"]) > 2 else 0,
            col_agg["min"][2] if "min" in col_agg.keys() and len(col_agg["min"]) > 2 else 0,

            col_agg["max"][0] if "max" in col_agg.keys() and len(col_agg["max"]) > 0 else 0,
            col_agg["max"][0] - col_agg["max"][1] if "max" in col_agg.keys() and len(col_agg["max"]) > 1 else 0,
            col_agg["max"][1] if "max" in col_agg.keys() and len(col_agg["max"]) > 1 else 0,
            col_agg["max"][1] - col_agg["max"][2] if "max" in col_agg.keys() and len(col_agg["max"]) > 2 else 0,
            col_agg["max"][2] if "max" in col_agg.keys() and len(col_agg["max"]) > 2 else 0  #,

            # col_agg["avg"] if "avg" in col_agg.keys() else 0,
        ]
        for col_alias, col_agg in metadata["columns"].iteritems()]


#process_structured_file('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header')
# with open('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header', 'rU') as f:
#     seek_preamble(f, ',', 135, 0)

filename= '/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header'
#print(extract_columnar_metadata(filename))
get_dataframes(filename, header=None, delim=',', file_length=210)