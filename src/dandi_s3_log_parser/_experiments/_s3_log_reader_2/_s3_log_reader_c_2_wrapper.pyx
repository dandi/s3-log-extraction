cdef extern from "_s3_log_reader_c_2.h":
    void extract_and_write_fifth_words(const char *input_filename, const char *output_filename)

def extract_fifth_words(input_filename: str, output_filename: str):
    extract_and_write_fifth_words(input_filename.encode('utf-8'), output_filename.encode('utf-8'))
