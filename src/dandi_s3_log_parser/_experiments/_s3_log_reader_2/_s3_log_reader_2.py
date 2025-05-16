import test_cython_2


def process_file_2(input_filename, output_filename):
    """
    Wrapper function to use the Cython method.
    """
    test_cython_2.extract_and_write_fifth_words(input_filename, output_filename)
