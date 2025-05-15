import test_cython

def process_file(filename, num_lines):
    """
    Wrapper function to use the Cython method.
    """
    test_cython.print_fourth_word_from_file(filename.encode("utf-8"), num_lines)
