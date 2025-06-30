BEGIN {
    HTTP_PATTERN_REGEX = "HTTP/1\\."
}

{
    # Pre-URI fields like this should be unaffected
    split($1, pre_uri_fields, " ")
    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    split($2, post_uri_fields, " ")
    status = post_uri_fields[2]
    if (substr(status, 1, 1) != "2") {next}

    datetime = pre_uri_fields[3]
    parsed_timestamp = \
        substr(datetime, 11, 2) \
        MONTH_TO_NUMERIC[substr(datetime, 5, 3)] \
        substr(datetime, 2, 2) \
        substr(datetime, 14, 2) \
        substr(datetime, 17, 2) \
        substr(datetime, 20, 2)

    if (length(http_pattern_count) != 14) {
        print "Error: 'HTTP/1.' occurs " http_pattern_count " times - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
