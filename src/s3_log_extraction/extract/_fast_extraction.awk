BEGIN {
    FS = "HTTP/1\\."

    if (!("IPS_TO_SKIP_REGEX" in ENVIRON)) {
        print "Environment variable 'IPS_TO_SKIP_REGEX' is not set" > "/dev/stderr"
        exit 1
    }
    IPS_TO_SKIP_REGEX = ENVIRON["IPS_TO_SKIP_REGEX"]
    TEMPORARY_DIRECTORY = ENVIRON["TEMPORARY_DIRECTORY"]

    OBJECT_KEYS_FILE_PATH = TEMPORARY_DIRECTORY "object_keys.txt"
    TIMESTAMPS_FILE_PATH = TEMPORARY_DIRECTORY "timestamps.txt"
    BYTES_SENT_FILE_PATH = TEMPORARY_DIRECTORY "bytes_sent.txt"
    IPS_FILE_PATH = TEMPORARY_DIRECTORY "full_ips.txt"
}

{
    if (NF == 0) {next}

    # Pre-URI fields like this should be unaffected
    split($1, pre_uri_fields, " ")
    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    ip = pre_uri_fields[5]
    if (ip ~ IPS_TO_SKIP_REGEX) {next}

    split($2, post_uri_fields, " ")
    status = post_uri_fields[2]
    if (substr(status, 1, 1) != "2") {next}

    object_key = pre_uri_fields[9]
    object_type = substr(object_key, 1, 5)
    # SPECIAL CASE: Limit Zarr stores to their top level to limit the number of files
    if (object_type == "zarr/") {
        split(object_key, object_key_parts, "/")
        object_key = object_key_parts[1] "/" object_key_parts[2]
    } else if (object_type != "blobs") {
        # SPECIAL CASE: DANDI assigns files to 'blobs'
        # if trying to use this for more generic bucket mirroring, disable this
        next
    }

    timestamp = pre_uri_fields[3]
    bytes_sent = (post_uri_fields[4] == "-" ? 0 : post_uri_fields[4])

    print object_key > OBJECT_KEYS_FILE_PATH
    print substr(timestamp, 2, 21) > TIMESTAMPS_FILE_PATH
    print bytes_sent > BYTES_SENT_FILE_PATH
    print ip > IPS_FILE_PATH
}
