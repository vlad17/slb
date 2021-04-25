{
    for (i=2; i<=NF; i++) {
        s = index($i, ":")
        feature = substr($i, 1, s - 1)
        counts[feature] += 1
    }
}
END {
    for (k in counts)
        print k, counts[k]
}
