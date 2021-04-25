# slb: sharded load balancer

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

And here's an example of counting the frequency of features in sparse SVM format of a large dataset, benchmarked on the large KDD12 dataset on a 32-core machine (assumes [ripgrep](https://github.com/BurntSushi/ripgrep), [GNU Parallel](https://www.gnu.org/software/parallel/) are installed). 

```
echo 'will cite' | parallel --citation 1>/dev/null 2>/dev/null
curl -o kdd12.tr.bz2 "https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/kdd12.tr.bz2"
bunzip2 kdd12.tr.bz2
du -hs kdd12.tr 
# 17G     kdd12.tr
parallel --pipepart -a kdd12.tr wc -l | awk '{a+=$0}END{print a}'
# num rows: 119705032
parallel --pipepart -a kdd12.tr wc -w | awk '{a+=$0}END{print a}'
# num nnz: 1436460384 - 119705032 = 1316755352

/usr/bin/time -f "%e sec %M KB" awk -f examples/svm-featurecount.awk kdd12.tr > results-awk.txt
# 1011.70 sec 13720908 KB

/usr/bin/time -f "%e sec %M KB" target/release/slb \
  --mapper 'sed -E "s/^[^ ]+ //" | sed -E "s/:[^ ]+//g" | tr " " "\n" | rg -v "^$"' \
  --folder 'awk -f examples/wc.awk' \
  --infile kdd12.tr > results-slb.txt
# 254.82 sec 881364 KB

# check we're correct
sort --parallel=$(($(nproc) / 2)) -k2nr -k 1 -o results-slb.txt results-slb.txt & \
sort --parallel=$(($(nproc) / 2)) -k2nr -k 1 -o results-awk.txt results-awk.txt & \
wait

diff results-slb.txt results-awk.txt >/dev/null ; echo $?
# 0
```

Note the above demonstrates the convenience of the tool:

* For large datasets, parallelism is essential.
* Compared to an equivalent map-reduce, we use less memory and less code.

The last point holds because `slb` ensures each parallel invocation recieves a _unique partition_ of the key space. In turn, we use less memory because each `wc.awk` process is only tracking counts for its own key space and less code because we do not need to write a combiner that merges two feature count maps.

