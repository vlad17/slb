# slb: streaming load balancer

[![travis build](https://travis-ci.org/vlad17/slb.svg?branch=master)](https://travis-ci.org/vlad17/slb)

Like `parallel --pipe --roundrobin` but load balancing is performed based on input line hashing. When performing keyed aggregations in child processes this is crucial since then only one shard contains a given key. Here's a word count example:

```
cargo build --release
/usr/bin/time -f "%e sec %M KB" awk -f examples/wc.awk RS='[[:space:]]' lines.txt > awk.txt
# 
/usr/bin/time -f "%e sec %M KB" (tr '[[:space:]]' '\n' lines.txt | cargo/release/slb 'awk -f examples/wc.awk' > slb.txt)
# 
diff <(sort awk.txt) <(sort slb.txt) ; echo $?
# 
```

above tested on an 8-core machine
