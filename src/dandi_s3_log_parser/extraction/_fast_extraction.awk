BEGIN {
    FS = "HTTP/1\\."

    if (!("DROGON_IP_REGEX" in ENVIRON)) {
        print "Environment variable DROGON_IP_REGEX is not set" > "/dev/stderr"
        exit 1
    }
    DROGON_IP_REGEX = ENVIRON["DROGON_IP_REGEX"]
    TEMPORARY_DIRECTORY = ENVIRON["TEMPORARY_DIRECTORY"]

    OBJECT_KEYS_FILE_PATH = TEMPORARY_DIRECTORY "/object_keys.txt"
    TIMESTAMPS_FILE_PATH = TEMPORARY_DIRECTORY "/timestamps.txt"
    BYTES_SENT_FILE_PATH = TEMPORARY_DIRECTORY "/bytes_sent.txt"
    IPS_FILE_PATH = TEMPORARY_DIRECTORY "/ips.txt"
}

{
    if (NF == 0) {next}

    # Pre-URI fields like this should be unaffected
    split($1, pre_uri_fields, " ")
    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    ip = pre_uri_fields[5]
    if (ip ~ DROGON_IP_REGEX) {next}

    split($2, post_uri_fields, " ")
    status = post_uri_fields[2]
    if (substr(status, 1, 1) != "2") {next}

    bytes_sent = post_uri_fields[4]
    if (bytes_sent == "-") {next}

    object_key = pre_uri_fields[9]
    timestamp = pre_uri_fields[3]

    print object_key > OBJECT_KEYS_FILE_PATH
    print substr(timestamp, 2, 21) > TIMESTAMPS_FILE_PATH
    print bytes_sent > BYTES_SENT_FILE_PATH
    print ip > IPS_FILE_PATH
}
