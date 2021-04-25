#!/bin/bash
# lightweight shell tester for slb

set -euo pipefail

testfiles=$(find examples -maxdepth 1 -name '*.txt')
echo "found test files:"
for f in $testfiles ; do
    du -h "$f"
done

cargo build --release

cwd=$(pwd)

echo
test_dir=$(mktemp -d)
echo "test directory: $test_dir"
# comment below to keep files around
trap "rm -rf $test_dir" EXIT
echo

pushd "$test_dir"

for f in $testfiles ; do
    echo "testing $f"
    f="$cwd/$f"
    b=$(basename "$f")
    awk -f "$cwd/examples/wc.awk" "$f" > "expected-$b"
    "$cwd/target/release/slb" \
        --mapper 'tr "[:space:]" "\n" | rg -v "^$"' \
        --folder "awk '{a[\$0]++}END{for(k in a)print k,a[k]}'" \
        --infile "$f" \
        --outprefix "actual-$b."
    cat actual-${b}.* > "actual-$b"
    rm actual-${b}.*
    sort -k2nr -k1 -o "expected-$b" "expected-$b"
    sort -k2nr -k1 -o "actual-$b" "actual-$b"
    diff "expected-$b" "actual-$b" >/dev/null
done 
popd >/dev/null
