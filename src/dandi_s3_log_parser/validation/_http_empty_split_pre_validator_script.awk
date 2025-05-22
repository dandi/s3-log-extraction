BEGIN {
    FS = "HTTP/1\\."
}

{
    # Pre-URI fields can be reliably extracted from direct split of entire line
    split($0, pre_uri_fields, " ")
    request_type = pre_uri_fields[8]
    if (NF == 0 && request_type == "REST.GET.OBJECT") {
        print "Splitting by HTTP pattern 'HTTP/1.' left no fields, but request was of type 'GET' - line" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
