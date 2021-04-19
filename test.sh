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
#trap "rm -rf $test_dir" EXIT
echo

pushd "$test_dir"

for f in $testfiles ; do
    echo "testing $f"
    f="$cwd/$f"
    b=$(basename "$f")
    awk -f "$cwd/examples/wc.awk" -v RS='[[:space:]]' "$f" > "expected-$b"
    tr '[:space:]' '\n' < "$f" | rg -v '^$' | "$cwd/target/release/slb" 'awk -f '"$cwd/examples/wc.awk" > "actual-$b"
    sort -k2r,1 -o "expected-$b" "expected-$b"
    sort -k2r,1 -o "actual-$b" "actual-$b"
    diff "expected-$b" "actual-$b" | head 
done 
popd
