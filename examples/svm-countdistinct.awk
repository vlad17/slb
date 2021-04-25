BEGIN {
    MAXUNIQUE = 100
}
1 {
    for (i=2; i<=NF; i++) {
        s = index($i, ":")
        feature = substr($i, 1, s - 1)
        if (feature in values && length(values[feature]) == MAXUNIQUE)
          continue
        value = substr($i, s + 1)
        values[feature][value] = 1
    }
}
END {
    for (k in values)
        print k, length(values[k])
}
