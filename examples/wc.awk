#!/bin/awk
# counts the number of appearances for each word in the stream
1 {
    for (i=1; i<=NF; i++)
        if ($i != "")
            a[$i]++
}
END {
    for (k in a)
        print k, a[k]
}
