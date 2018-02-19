
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import struct_utils
import csv

MIN_ROWS = 5

class NonUniformDelimiter(Exception):
    """When the file cannot be pushed into a delimiter. """

def extract_columnar_metadata(filename, pass_fail=False, lda_preamble=False, null_inference=False, nulls=None):
    """Get metadata from column-formatted file.
            :param data: (str) a path to an unopened file.
            :param pass_fail: (bool) whether to exit after ascertaining file class
            :param lda_preamble: (bool) whether to collect the free-text preamble at the start of the file
            :param null_inference: (bool) whether to use the null inference model to remove nulls
            :param nulls: (list(int)) list of null indices
            :returns: (dict) ascertained metadata
            :raises: (ExtractionFailed) if the file cannot be read as a columnar file"""

    # pool = Pool(processes = 2)https://github.com/Dingyan/124-critters.git
    # result = pool.map(doubler, numbers)

    # for m_item in result:
    #   print(m_item)

    #try:

    t0 = time.time()
    with open(filename, 'rU') as data2:

        print("[DEBUG] Getting number of lines. ")
        line_count = 1
        for line in data2:
            line_count += 1

        print(str(line_count) + " lines.")

        print("[DEBUG] Getting delimiter data.")
        delimiter = get_delimiter(filename, line_count)
        print("Delimiter is " + delimiter)

        print("[DEBUG] Getting header data.")
        header_info = get_header_info(data2, delim=delimiter)  #TODO: use max-fields of ',', ' ', or '\t'???
        print(header_info)
        freetext_offset = header_info[0]
        header_col_labels = header_info[1]
        #line_count = header_info[2]
        print("[DEBUG] Successfully got header data!")

        # TODO: TYLER -- start here.
        print("[DEBUG] Getting dataframes")
        if header_col_labels != None:
            dataframes = get_dataframes(filename, header=header_col_labels, delim=',', skip_rows=freetext_offset)
        else:  # elif header_col_labels == None.
            dataframes = get_dataframes(filename, header=None, delim=',', skip_rows=freetext_offset)
        print("[DEBUG] Successfully got dataframes!")

    data2.close()

    t1 = time.time()
    print(t1-t0)


        # for item in dataframes:
        #     print(item)

        # Now process each data frame.
    print("[DEBUG] Extracting metadata using *m* processes...")
    for item in dataframes:
        extract_dataframe_metadata(filename, item)
    #metadata = extract_dataframe_metadata(filename)
        #pool = Pool(processes=2)
       # extract_dataframe_metadata(filename, dataframes)
        # result = pool.map(extract_dataframe_metadata, dataframes) #TODO: Cannot yet feed _extract_columnar metadata a dataframe!
        # for m_item in result: #TODO: Not returning processed metadata.
        #     print(m_item)

    #t1 = time.time()  #Currently at 0.020 seconds to get into Dataframes... not bad...

    # except:
    #     pass


def extract_dataframe_metadata(filename, df):

    t0 = time.time()
    # df = pd.read_csv(filename, skiprows=82)

    # Get only the numeric columns in data frame.
    ndf = df._get_numeric_data()  # TODO: get 3 UNIQUE minima, 3 UNIQUE maxima, and average for each column.
    # Get only the string columns in data frame.
    sdf = df.select_dtypes(include=[object])  # TODO: Get five most-occurring values (max five).

    # for col in sdf:
    #     print(ndf[col].value_counts())  # Just get 3 here.

    # print(sdf)
    vals = df.values

    t1 = time.time()

    print(t1-t0)

    try:
        for col in ndf:
            largest = df.nlargest(3, columns=col, keep='first')  # Output dataframe ordered by col.
            smallest = df.nsmallest(3, columns=col, keep='first')
            the_mean = ndf[col].mean()

            col_maxs = largest[col]
            col_mins = smallest[col]

            maxn = []
            minn = []
            for maxnum in col_maxs:
                maxn.append(maxnum)

            for minnum in col_mins:
                minn.append(minnum)

            # TODO: Create tuple entry (header_name (or index), [max1, max2, max3], [min1, min2, min3], avg)
            print(col, minn, maxn, the_mean) #TODO: Header_NAME and avg.
    except:
        top2 = "None"
    # return (ndf, sdf)
    return ndf


def get_delimiter(filename, numlines):

    # Step 1: Load last min_lines into dataframe.  Just to ensure it can be done.
    mini_df = pd.read_csv(filename, skiprows = numlines-MIN_ROWS)

    # Step 2: Get the delimiter of the last n lines.
    s = csv.Sniffer()
    with open(filename, 'rU') as fil:
        i = 1
        delims = []
        for line in fil:
            if i > numlines - MIN_ROWS:
                delims.append(s.sniff(line).delimiter)
            i += 1

        #print(delims)
        if delims.count(delims[0]) == len(delims):
            return delims[0]
        else:
            raise NonUniformDelimiter("Error in get_delimiter")

def get_dataframes(filename, header, delim, skip_rows = 0, dataframe_size = 1000):

    header = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"] #TODO: Un-hardcode this. Should get list of header_nms.

    skip_rows=82
    iter_csv = pd.read_csv(filename, sep=delim, chunksize=10, header=None, skiprows=skip_rows, iterator=True)

    return iter_csv


# Currently assuming short freetext headers.
def get_header_info(data, delim):
    data.seek(0)
    # Get a line count.
    line_count = 0
    for line in data:
        line_count += 1

    # Figure out the length of file via binary search (in"seek_preamble")
    if line_count >= 5: #set arbitrary min value or bin-search not useful.
        # A. Get the length of the preamble.
        preamble_length = get_last_preamble_line(data, delim, line_count)

        # B. Determine whether the next line is a freetext header
        data.seek(0)
        for i, line in enumerate(data):
            if i == preamble_length+1:  # +1 since that's one after the preamble.
                print("The header row is: " + str(line))

                has_header = struct_utils.is_header_row(struct_utils.fields(line, delim))
                if has_header:  # == True
                    header = struct_utils.fields(line, delim)
                else:
                    header = None

            elif i > preamble_length:
                break
        return (preamble_length, header)


def get_last_preamble_line(data, delim, start_point, prev_val=0, last_move=None): #TODO: check last delim finding w/ new one.
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
            return(get_last_preamble_line(data, delim, midpoint, prev_val, "UP"))
        else:
            # Move DOWN the file.
            return(get_last_preamble_line(data, delim, start_point, midpoint, "DOWN"))

extract_columnar_metadata('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header')
#get_delimiter('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header', 212)