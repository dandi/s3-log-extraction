{
    # Check if "HTTP/1." occurs more than once
    http_count = gsub(/HTTP\/1./, "&")
    if (http_count > 1) {
        print "Error: 'HTTP/1.' occurs " http_count " times - line #" NR " of " FILENAME > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
