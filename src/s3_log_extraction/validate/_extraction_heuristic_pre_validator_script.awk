BEGIN {
    FS = "HTTP/1\\."

    if (!("DROGON_IP_REGEX" in ENVIRON)) {
        print "Environment variable DROGON_IP_REGEX is not set" > "/dev/stderr"
        exit 1
    }
    DROGON_IP_REGEX = ENVIRON["DROGON_IP_REGEX"]

    IP_REGEX = "^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$"
    STATUS_REGEX = "^[1-5][0-9]{2}$"
}

{
    if (NF == 0) {next}

    # Pre-URI fields like this should be unaffected
    split($1, pre_uri_fields, " ")
    ip = pre_uri_fields[5]
    if (ip !~ IP_REGEX) {
        print "Error with IP extraction - line #" NR " of " FILENAME > "/dev/stderr"
        print "Direct: \"" ip "\" (" typeof(ip) ")" > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
    if (ip ~ DROGON_IP_REGEX) {next}

    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    # Use strong validation rule to try to get reliable status, even in extreme cases
    if ($0 ~ /HTTP\/1\.1/) {
        split($0, direct_http_split, "HTTP/1.1")
        split(direct_http_split[2], direct_http_space_split, " ")
        status_from_direct_rule = direct_http_space_split[2]
    } else if ($0 ~ /HTTP\/1\.0/) {
        split($0, direct_http_split, "HTTP/1.0")
        split(direct_http_split[2], direct_http_space_split, " ")
        status_from_direct_rule = direct_http_space_split[2]
    } else {
        print "Line contained neither HTTP/1.1 or HTTP/1.0 - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
    if (status_from_direct_rule !~ STATUS_REGEX) {
        print "Error with direct status code detection - line #" NR " of " FILENAME > "/dev/stderr"
        print "Direct: \"" status_from_direct_rule "\" (" typeof(status_from_direct_rule) ")" > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }

    # Post-URI fields are more likely to be affected by failures of the heuristic
    split($2, post_uri_fields, " ")
    status_from_heuristic = post_uri_fields[2]
    if (status_from_heuristic !~ STATUS_REGEX && substr(status_from_direct_rule,1,1) == "2") {
        print "A directly detected success status code was discovered while the extraction rule failed to detect at all - line #" NR " of " FILENAME > "/dev/stderr"
        print "Extraction: " status_from_heuristic > "/dev/stderr"
        print "Direct: " status_from_direct_rule > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
    if (status_from_heuristic != status_from_direct_rule && substr(status_from_direct_rule,1,1) == "2") {
        print "Both status codes were extracted as valid numbers, the direct extraction was successful, but the two did not match - line #" NR " of " FILENAME > "/dev/stderr"
        print "Extraction: " status_from_heuristic > "/dev/stderr"
        print "Direct: " status_from_direct_rule > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }

    # Keeping this around as a note; it does find examples of bytes_sent being 0 ('-') from GET requests even when
    #    from BEGIN BYTES_SENT_REGEX = "^[0-9]+$"
    # size of object is not zero...
    # Impact on extraction heuristic is to therefore just always record the request as bytes_sent = 0
    #
    #    bytes_sent = post_uri_fields[4]
    #    total_bytes = post_uri_fields[5]
    #    if (bytes_sent !~ BYTES_SENT_REGEX && total_bytes != 0 && substr(status_from_direct_rule,1,1) == "2") {
    #        print "Bytes sent was not a valid number, total bytes was non-zero, and status was success - line #" NR " of "FILENAME > "/dev/stderr"
    #        print "Bytes sent: \"" bytes_sent "\" (" typeof(bytes_sent) ")" > "/dev/stderr"
    #        print $0 > "/dev/stderr"
    #        exit 1
    #    }
}
