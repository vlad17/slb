# slb: sharded load balancer

Like `parallel --pipe --roundrobin` but load balancing is performed based on input line hashing. When performing keyed aggregations in child processes this is crucial since then only one shard contains a given key. Here's a word count example on a 16-physical-cpu machine:

```
curl -o enwik9.bz2 https://cs.fit.edu/~mmahoney/compression/enwik9.bz2
bunzip2 enwik9.bz2
examples/clean.sh < enwik9 > enwik9.clean ; rm enwik9

/usr/bin/time -f "%e sec" awk -f examples/wc.awk enwik9.clean > wikawk.txt
# 203.97 sec

/usr/bin/time -f "%e sec" slb \
  --mapper 'tr " " "\n" | rg -v "^$"' \
  --folder "awk '{a[\$0]++}END{for(k in a)print k,a[k]}'" \
  --infile enwik9.clean \
  --outprefix wikslb.
# 6.20 sec

diff <(sort wikawk.txt) <(cat wikslb.* | sort) ; echo $?
# 0
```

This demonstrates a "flatmap-fold" paradigm over the typical "map-reduce" one.

Each line

```
a    b c d -> flatmapper 1
f g   a b -> flatmapper 2
```

is handed off to an independent flat mapper `tr " " "\n" | rg -v "^$"` which puts a word on each line

```
flatmapper 1 ->
a
b
c
d

flatmapper 2 ->
f
g
a
b
```

whose outputs are then inspected line-by-line. The first word of each line is hashed (in this case, the entire line). Assuming `hash(a) == hash(b) == 1` and `hash(c) == hash(d) == hash(g) == hash(f) == 0` we'll input the corresponding keys from each flatmapper into a couple `awk '{a[$0]++}END{for(k in a)print k,a[k]}'` folders. And the outputs are then written to output files.

```
a b a b -> awk 1 -> {a: 2, b: 2} -> outprefix1
f g c d -> awk 0 -> {f: 1, g: 1, c: 1, d: 1} -> outprefix0
```

## Feature Frequency Calculation

Here's an example of counting the frequency of features in sparse [SVMlight](https://www.cs.cornell.edu/people/tj/svm_light/) format of a large dataset, benchmarked on the large KDD12 dataset on a 16-physical-cpu machine (assumes [ripgrep](https://github.com/BurntSushi/ripgrep), [GNU Parallel](https://www.gnu.org/software/parallel/) are installed).

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
# 1032.18 sec 13721032 KB

/usr/bin/time -f "%e sec %M KB" slb \
  --mapper 'sed -E "s/^[^ ]+ //" | sed -E "s/:[^ ]+//g" | tr " " "\n" | rg -v "^$"' \
  --folder "awk '{a[\$0]++}END{for(k in a)print k,a[k]}'" \
  --infile kdd12.tr \
  --outprefix results-slb.
# 122.50 sec 881436 KB
# note above doesn't count child memory
# eyeballing htop, max memory use is ~12.3GB

# check we're correct
cat results-slb.* > results-slb && rm results-slb.*
sort --parallel=$(($(nproc) / 2)) -k2nr -k1n -o results-slb results-slb & \
sort --parallel=$(($(nproc) / 2)) -k2nr -k1n -o results-awk.txt results-awk.txt & \
wait

diff results-slb results-awk.txt >/dev/null ; echo $?
# 0
```

## Count Distinct Feature Values

As another, similar example we could count the number of distinct values for each feature. In particular, for each feature we're looking to get the minimum of its total number of distinct values with 100 (as we might be inclined to consider anything with more than 99 values to be continuous).

```
curl -o kdda.bz2 "https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/kdda.bz2"
bunzip2 kdda.bz2
du -hs kdda
# 2.5G    kdda

/usr/bin/time -f "%e sec %M KB" awk -f examples/svm-countdistinct.awk kdda > cdawk.txt
# 388.72 sec 23895104 KB

/usr/bin/time -f "%e sec %M KB" slb \
  --mapper 'sed -E "s/^[^ ]+ //" | tr " " "\n" | tr ":" " " | rg -v "^$"' \
  --folder "awk '{if(!(\$1 in a)||length(a[\$1])<100)a[\$1][\$2]=1}END{for(k in a)print k,length(a[k])}'" \
  --infile kdda \
  --outprefix cdslb.
# 26.79 sec 1499992 KB

diff \
  <(sort --parallel=$(($(nproc) / 2)) -k2nr -k1n cdawk.txt) \
  <(cat cdslb.* | sort --parallel=$(($(nproc) / 2)) -k2nr -k1n) \
  > /dev/null ; echo $?
# 0
```

## Installation

Note the above examples demonstrate the convenience of the tool:

* For large datasets, parallelism is essential.
* Compared to an equivalent map-reduce, we use less memory, less time, and less code.

The last point holds because `slb` ensures each parallel invocation recieves a _unique partition_ of the key space. In turn, we use less memory because each folder is only tracking aggregates for its own key space and less code because we do not need to write a combiner that merges two maps.

To install locally from `crates.io`, run

```
cargo install slb
```

## Dev Stuff

Rudimentary testing via `./test.sh`.

Re-publish to `crates.io` with `cd slb && cargo publish`.

