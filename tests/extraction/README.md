# Contributing

If you would like to contribute some failing examples to this test suite, please contact the maintainer (cody.c.baker.phd@gamil.com) prior to opening a pull request.

There are multiple fields that may require anonymization prior to being shared openly.

The names of the following test files are patterned off of the S3 log filename convention and may not accurately respect the timestamps of the lines within.



# 2020-01-01-05-06-35-0123456789ABCDEF (Easy lines)

For DANDI, the timestamps of each line for a log file correspond to the `YEAR/MONTH/DAY.log` of the directory.

The extraction tools are not dependent on this structure and so to simplify the test cases, we only maintain two files.

The 'easy' collection contains the most typical lines which follow a nice, simple, and reliable structure.



# 2022-01-01-05-06-35-0123456789ABCDEF (Hard lines)

The 'hard' collection contains many of the most difficult lines to extract as they were found from error reports.
