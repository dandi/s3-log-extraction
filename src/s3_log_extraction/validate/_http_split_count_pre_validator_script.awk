BEGIN {
    HTTP_PATTERN_REGEX = "HTTP/1\\."
}

{
    # Check if "HTTP/1." occurs more than once
    http_pattern_count = gsub(HTTP_PATTERN_REGEX, "&")
    if (http_pattern_count > 1) {
        print "Error: 'HTTP/1.' occurs " http_pattern_count " times - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
