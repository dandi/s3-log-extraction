BEGIN {
    http_pattern_regex = "HTTP/1\\."
}

{
    # Check if "HTTP/1." occurs more than once
    http_pattern_count = gsub(http_pattern_regex, "&")
    if (http_pattern_count > 1) {
        print "Error: 'HTTP/1.' occurs " http_pattern_count " times - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
