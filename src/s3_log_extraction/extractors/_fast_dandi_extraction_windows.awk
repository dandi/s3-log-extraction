BEGIN {
    FS = "HTTP/1\\."

    if (!("IPS_TO_SKIP_REGEX" in ENVIRON)) {
        print "Environment variable 'IPS_TO_SKIP_REGEX' is not set" > "/dev/stderr"
        exit 1
    }
    IPS_TO_SKIP_REGEX = ENVIRON["IPS_TO_SKIP_REGEX"]

    if (!("TEMPORARY_DIRECTORY" in ENVIRON)) {
        print "Environment variable 'TEMPORARY_DIRECTORY' is not set" > "/dev/stderr"
        exit 1
    }
    TEMPORARY_DIRECTORY = ENVIRON["TEMPORARY_DIRECTORY"] "/"

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

    datetime = pre_uri_fields[3]
    parsed_timestamp = \
        substr(datetime, 11, 2) \
        MONTH_TO_NUMERIC[substr(datetime, 5, 3)] \
        substr(datetime, 2, 2) \
        substr(datetime, 14, 2) \
        substr(datetime, 17, 2) \
        substr(datetime, 20, 2)

    bytes_sent = (post_uri_fields[4] == "-" ? 0 : post_uri_fields[4])

    data[object_key]["timestamps"][++data[object_key]["timestamps_count"]] = parsed_timestamp
    data[object_key]["bytes_sent"][++data[object_key]["bytes_sent_count"]] = bytes_sent
    data[object_key]["ip"][++data[object_key]["ip_count"]] = ip
}

END {
    for (object_key in data) {
        subdirectory = TEMPORARY_DIRECTORY object_key
        gsub("/", "\\", subdirectory)
        gsub("\\\\", "\\\\\\\\", subdirectory)
        system("mkdir -p " subdirectory)
    }

    for (object_key in data) {
        subdirectory = TEMPORARY_DIRECTORY object_key
        gsub("/", "\\", subdirectory)
        gsub("\\\\", "\\\\\\\\", subdirectory)
        timestamps_file_path = subdirectory "\\timestamps.txt"
        bytes_sent_file_path = subdirectory "\\bytes_sent.txt"
        full_ips_file_path = subdirectory "\\full_ips.txt"

        for (i = 1; i <= data[object_key]["timestamps_count"]; i++) {
            print data[object_key]["timestamps"][i] >> timestamps_file_path
        }
        for (i = 1; i <= data[object_key]["bytes_sent_count"]; i++) {
            print data[object_key]["bytes_sent"][i] >> bytes_sent_file_path
        }
        for (i = 1; i <= data[object_key]["ip_count"]; i++) {
            print data[object_key]["ip"][i] >> full_ips_file_path
        }
    }
}
