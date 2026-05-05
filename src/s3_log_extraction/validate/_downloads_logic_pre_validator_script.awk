BEGIN {
    FS = "HTTP/1\\."

    BYTES_SENT_REGEX = "^[0-9]+$"
}

{
    if (NF == 0) {next}

    split($1, pre_uri_fields, " ")
    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    split($2, post_uri_fields, " ")
    status = post_uri_fields[2]
    if (status != "200") {next}

    bytes_sent = post_uri_fields[4]
    total_bytes = post_uri_fields[5]

    if (bytes_sent ~ BYTES_SENT_REGEX && total_bytes ~ BYTES_SENT_REGEX && (bytes_sent + 0) < (total_bytes + 0)) {
        print "Bytes sent was less than the total bytes (object size) yet status was 200 - line #" NR " of " FILENAME > "/dev/stderr"
        print "Bytes sent: " bytes_sent > "/dev/stderr"
        print "Total bytes (object size): " total_bytes > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
