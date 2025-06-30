BEGIN {
    FS = "HTTP/1\\."

    MONTH_TO_NUMERIC["Jan"] = "01"
    MONTH_TO_NUMERIC["Feb"] = "02"
    MONTH_TO_NUMERIC["Mar"] = "03"
    MONTH_TO_NUMERIC["Apr"] = "04"
    MONTH_TO_NUMERIC["May"] = "05"
    MONTH_TO_NUMERIC["Jun"] = "06"
    MONTH_TO_NUMERIC["Jul"] = "07"
    MONTH_TO_NUMERIC["Aug"] = "08"
    MONTH_TO_NUMERIC["Sep"] = "09"
    MONTH_TO_NUMERIC["Oct"] = "10"
    MONTH_TO_NUMERIC["Nov"] = "11"
    MONTH_TO_NUMERIC["Dec"] = "12"
}

{
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

    if (length(parsed_timestamp) != 12) {
        print "Error: Failed to parse timestamp " datetime " - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
