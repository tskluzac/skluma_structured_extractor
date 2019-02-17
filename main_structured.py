
import time
import pandas as pd
import struct_utils
import csv
import math

MIN_ROWS = 5

"""
 TODO LIST: 
 1. Be able to isolate preamble and header (if exists) in a file. Hold preamble as free text string. 
 2. Should be able to make up header values for columns if they don't already exist. 
 3. Get meaningful numeric metadata
 4. Get meaningful nonnumeric metadata (most commonly used field values). 
 5. Data sampling. 
 6. Handle 2-line headers. ('/home/skluzacek/pub8/oceans/VOS_Natalie_Schulte_Lines/NS2010_09.csv')
 

"""

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

    with open(filename, 'rU') as data2:
        # Step 1. Quick scan for number of lines in file.
        print("[DEBUG] Getting number of lines. ")
        line_count = 1
        for _ in data2:
            line_count += 1

        print(str(line_count) + " lines.")

        # Step 2. Determine the delimiter of the file.
        print("[DEBUG] Getting delimiter data.")
        delimiter = get_delimiter(filename, line_count)
        print("Delimiter is " + delimiter)

        # Step 3. Isolate the header data.
        print("[DEBUG] Getting header data.")
        header_info = get_header_info(data2, delim=",")  #TODO: use max-fields of ',', ' ', or '\t'???
        freetext_offset = header_info[0]
        header_col_labels = header_info[1]

        print(header_info)

        print("[DEBUG] Successfully got header data!")

        # Step 4. Extract content-based metadata.
        print("[DEBUG] Getting dataframes")
        if header_col_labels != None:
            dataframes = get_dataframes(filename, header=header_col_labels, delim=delimiter, skip_rows=freetext_offset)
        else:
            dataframes = get_dataframes(filename, header=None, delim=delimiter, skip_rows=freetext_offset)
        print("[DEBUG] Successfully got dataframes!")
    data2.close()

    # Now process each data frame.
    print("[DEBUG] Extracting metadata using *m* processes...")

    # Iterate over each dataframe to extract values.
    df_metadata = []
    for df in dataframes:
        print(df)

        # df = df.str.split(",")

        metadata = extract_dataframe_metadata(df, header_col_labels)
        df_metadata.append(metadata)


def extract_dataframe_metadata(df, header):

    print("HEADERS:" + str(header))

    t0 = time.time()

    # Get only the numeric columns in data frame.
    ndf = df._get_numeric_data()

    print("NDF: " + str(ndf))

    # Get only the string columns in data frame.
    sdf = df.select_dtypes(include=[object])  # TODO: Get k most-occurring values (max five).

    print("SDF: " + str(ndf))


    tuple_list = []

    for col in ndf:

        largest = df.nlargest(3, columns=col, keep='first')  # Output dataframe ordered by col.
        smallest = df.nsmallest(3, columns=col, keep='first')
        the_mean = ndf[col].mean()

        print(the_mean)


        # Use GROUP_BY and then MEAN.
        col_maxs = largest[col]
        col_mins = smallest[col]

        maxn = []
        minn = []
        for maxnum in col_maxs:
            maxn.append(maxnum)

        for minnum in col_mins:
            minn.append(minnum)

        # (header_name (or index), [max1, max2, max3], [min1, min2, min3], avg)
        the_tuple = (col, minn, maxn, the_mean) #TODO: Header_NAME and avg.
        tuple_list.append((len(ndf), the_tuple))

        # print(the_tuple)

    return tuple_list


def get_delimiter(filename, numlines):

    # Step 1: Load last min_lines into dataframe.  Just to ensure it can be done.
    mini_df = pd.read_csv(filename, skiprows = numlines-MIN_ROWS, error_bad_lines=False)

    # Step 2: Get the delimiter of the last n lines.
    s = csv.Sniffer()
    with open(filename, 'r') as fil:
        i = 1
        delims = []
        for line in fil:
            #print(line)
            #line = line.encode('utf-8').strip()
            if i > numlines - MIN_ROWS and ('=' not in line):
                #print(line)
                delims.append(s.sniff(line).delimiter)
            i += 1

        #print(delims)
        if delims.count(delims[0]) == len(delims):
            return delims[0]
        else:
            raise NonUniformDelimiter("Error in get_delimiter")


def get_dataframes(filename, header, delim, skip_rows = 0, dataframe_size = 1000):

    skip_rows = 82
    iter_csv = pd.read_csv(filename, sep=delim, chunksize=100000, header=None, skiprows=skip_rows, error_bad_lines=False, iterator=True)

    return iter_csv


def count_fields(dataframe):
    print(dataframe.shape[1])


# Currently assuming short freetext headers.
def get_header_info(data, delim):

    data.seek(0)
    # TODO: Get the line count from the binary search.
    # Get the line count.
    line_count = 0
    for _ in data:
        line_count += 1

    # Figure out the length of file via binary search (in"seek_preamble")
    if line_count >= 5:  # set arbitrary min value or bin-search not useful.
        # A. Get the length of the preamble.
        preamble_length = _get_preamble(data, delim)

        print("P-length: " + str(preamble_length))
        # B. Determine whether the next line is a freetext header
        data.seek(0)

        header = None
        for i, line in enumerate(data):

            if preamble_length == None:
                header = None
                break
            if i == preamble_length:  # +1 since that's one after the preamble.
                print("The header row is: " + str(line))

                has_header = struct_utils.is_header_row(struct_utils.fields(line, delim))
                if has_header:  # == True
                    header = struct_utils.fields(line, delim)
                else:
                    header = None

            elif i > preamble_length:
                break

        return (preamble_length, header)


def _get_preamble(data, delim):
    data.seek(0)
    delim = ','
    max_nonzero_row = None
    max_nonzero_line_count = None
    last_preamble_line_num = None

    # *** Get number of delimited columns in last nonempty row (and row number) *** #
    delim_counts = {}
    for i, line in enumerate(data):
        cur_line_field_count = len(line.split(delim))

        if cur_line_field_count != 0:
            delim_counts[i] = cur_line_field_count
            max_nonzero_row = i
            max_nonzero_line_count = cur_line_field_count

    print(delim_counts)

    # [Weed out complicated cases] Now if the last three values are all the same...
    if delim_counts[max_nonzero_row] == delim_counts[max_nonzero_row - 1] == delim_counts[max_nonzero_row - 2]:
        # Now binary-search from the end to find the last row with that number of columns.
        starting_row = math.floor(max_nonzero_row - 2) / 2  # Start in middle of file for sanity.
        last_preamble_line_num = _last_preamble_line_bin_search(delim_counts, max_nonzero_line_count, starting_row,
                                                                upper_bd=0, lower_bd=max_nonzero_row - 2)

    return last_preamble_line_num


def _last_preamble_line_bin_search(field_cnt_dict, target_field_num, cur_row, upper_bd=None, lower_bd=None):

    # Check current row and next two to see if they are all the target value.
    cur_row = math.floor(cur_row)

    # If so, then we want to move up in the file.
    if field_cnt_dict[cur_row] == field_cnt_dict[cur_row+1] == field_cnt_dict[cur_row+2] == target_field_num:

        new_cur_row = cur_row - math.floor((cur_row - upper_bd)/2)

        # If we're in the first row, we should return here.
        if cur_row == 1 and field_cnt_dict[cur_row-1] == field_cnt_dict[cur_row] == target_field_num:
            return 0

        elif cur_row == 1 and field_cnt_dict[cur_row-1] != target_field_num:
            return 1

        else:
            recurse = _last_preamble_line_bin_search(field_cnt_dict, target_field_num, new_cur_row,
                                                     upper_bd=upper_bd, lower_bd=cur_row)
            return recurse

    elif field_cnt_dict[cur_row] == field_cnt_dict[cur_row+1] == target_field_num:
        return cur_row + 1

    # If not, then we want to move down in the file.
    else:
        new_cur_row = cur_row + math.floor((lower_bd - cur_row) / 2)

        if cur_row == new_cur_row:
            return cur_row + 1

        recurse = _last_preamble_line_bin_search(field_cnt_dict, target_field_num, new_cur_row,
                                                 upper_bd=cur_row, lower_bd=lower_bd)
        return recurse


# with open('/home/skluzacek/pub8/oceans/VOS_Natalie_Schulte_Lines/NS2010_09.csv', 'r') as f:
    # with open('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/comma_delim', 'r') as f:
    # print(_get_preamble(f, ','))

# with open('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/freetext_header', 'r') as f:

    # get_last_preamble_line(f, delim=',', start_point=100, upper_bd=5, lower_bd=210)


extract_columnar_metadata('/home/skluzacek/PycharmProjects/skluma_structured_extractor/tests/test_files/no_headers')