BEGIN {
    FS = "HTTP/1."
}

{
    split($0, pre_uri_fields, " ")

    request_type = pre_uri_fields[8]
    if (request_type != "REST.GET.OBJECT") {next}

    if (NF == 0 && request_type == "REST.GET.OBJECT") {
        print "No fields were split by 'HTTP/1.', but directly extracted request type was 'GET' - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
