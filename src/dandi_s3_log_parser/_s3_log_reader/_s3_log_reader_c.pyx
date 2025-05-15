def print_fourth_word_from_file(const char * filename, int num_lines):
    """
    Reads the first `num_lines` lines of a text file and prints the fourth word from each line.

    Parameters:
        filename: The path to the text file.
        num_lines: The number of lines to read from the file.
    """
    cdef int line_count = 0
    cdef str line
    cdef list words

    with open(filename, "r") as file:
        for line in file:
            if line_count >= num_lines:
                break
            words = line.split()
            if len(words) >= 4:
                print(words[3])  # Print the fourth word (index 3)
            else:
                print("Line does not have enough words.")
            line_count += 1