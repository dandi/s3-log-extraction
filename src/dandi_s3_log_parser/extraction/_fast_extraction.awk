BEGIN {
    FS = "HTTP/1\\."

    if (!("DROGON_IP_REGEX" in ENVIRON)) {
        print "Environment variable DROGON_IP_REGEX is not set" > "/dev/stderr"
        exit 1
    }
    DROGON_IP_REGEX = ENVIRON["DROGON_IP_REGEX"]
    TEMPORARY_DIRECTORY = ENVIRON["TEMPORARY_DIRECTORY"]
}

{
    if (NF == 0) {next}

    # Pre-URI fields like this should be unaffected
    split($1, pre_uri_fields, " ")
    ip = pre_uri_fields[5]
    if (ip ~ DROGON_IP_REGEX) {next}

    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    split($2, post_uri_fields, " ")
    status = post_uri_fields[1]
    if (substr(status, 1, 1) != "2") {next}

    object_key = pre_uri_fields[9]
    timestamp = pre_uri_fields[3]
    bytes_sent = post_uri_fields[3]

    print object_key > TEMPORARY_DIRECTORY "/object_keys.txt"
    print substr(timestamp, 2, 21) > TEMPORARY_DIRECTORY "/timestamps.txt"
    print bytes_sent > TEMPORARY_DIRECTORY "/bytes_sent.txt"
    print ip > TEMPORARY_DIRECTORY "/ips.txt"
}
