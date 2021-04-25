#!/bin/awk
# counts the number of appearances for each word in the stream
BEGIN {
    RS = "[[:space:]]"
}
1 {
    if ($0 != "")
        a[$0]++
}
END {
    for (k in a)
        print k, a[k]
}
