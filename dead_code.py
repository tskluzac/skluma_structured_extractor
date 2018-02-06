class ReverseReader:
    """Reads column-formatted files in reverse as lists of fields.
        :param file_handle: (file) open file
        :param delimiter: (string) delimiting character """

    def __init__(self, file_handle, delimiter=","):
        self.fh = file_handle
        self.fh.seek(0, os.SEEK_END)
        self.delimiter = delimiter
        self.position = self.fh.tell()
        self.prev_position = self.fh.tell()

    @staticmethod
    def fields(line, delim):
        # if space-delimited, do not keep whitespace fields, otherwise do
        fields = [field.strip() for field in re.split(delim if delim != " " else "\\s", line)]
        if delim in [" ", "\t", "\n"]:
            fields = filter(lambda f: f != "", fields)
        return fields

    def next(self):
        line = ''
        if self.position <= 0:
            raise StopIteration
        self.prev_position = self.position
        while self.position >= 0:
            self.fh.seek(self.position)
            next_char = self.fh.read(1)
            if next_char in ['\n', '\r']:
                self.position -= 1
                if len(line) > 1:
                    return self.fields(line[::-1], self.delimiter)
            else:
                line += next_char
                self.position -= 1
        return self.fields(line[::-1], self.delimiter)

    def __iter__(self):
        return self

