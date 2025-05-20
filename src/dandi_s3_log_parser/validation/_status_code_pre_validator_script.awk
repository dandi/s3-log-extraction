{
    drogon_ip_regex = ENVIRON["DROGON_IP_REGEX"]

    split($1, pre_uri_fields, " ");

    # Pre-URI fields like this should be unaffected
    ip = pre_uri_fields[5];

    if (ip ~ / drogon_ip_regex /) {next}

    request_type = pre_uri_fields[8];
    if (request_type != "REST.GET.OBJECT") {next}

    # Use special rule to try to get reliable status, even in extreme cases
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
    if (status_from_direct_rule !~ /^[1-5][0-9]{2}$/) {
        print "Error with direct status code detection - line #" NR " of " FILENAME > "/dev/stderr"
        print "Direct: " status_from_direct_rule > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }

    # Post-URI fields are more likely to be affected
    split($3, post_uri_fields, " ");
    status_from_extraction_rule = post_uri_fields[1];
    if (status_from_extraction_rule !~ /^[1-5][0-9]{2}$/ && substr(status_from_direct_rule,1,1) == "2") {
        print "A directly detected success status code was discovered while the extraction rule failed to detect at all - line #" NR " of " FILENAME > "/dev/stderr"
        print "Extraction: " status_from_extraction_rule > "/dev/stderr"
        print "Direct: " status_from_direct_rule > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
    if (status_from_extraction_rule != status_from_direct_rule && substr(status_from_direct_rule,1,1) == "2") {
        print "Both status codes were extracted as valid numbers, the direct extraction was successful, but the two did not match - line #" NR " of " FILENAME > "/dev/stderr"
        print "Extraction: " status_from_extraction_rule > "/dev/stderr"
        print "Direct: " status_from_direct_rule > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
