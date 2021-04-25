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
    tr '[:upper:]' '[:lower:]' < "$f" | tr -c '[:alnum:]- ' ' ' | awk -f "$cwd/examples/wc.awk" -v RS='[[:space:]]' > "expected-$b"
    "$cwd/target/release/slb" \
        --mapper 'tr "[:upper:]" "[:lower:]" | tr -c "[:alnum:]- " " " | tr "[:space:]" "\n" | rg -v "^$"' \
        --folder 'awk -f '"$cwd/examples/wc.awk" \
        --infile "$f" > "actual-$b"
    sort -k2nr -k1 -o "expected-$b" "expected-$b"
    sort -k2nr -k1 -o "actual-$b" "actual-$b"
    diff "expected-$b" "actual-$b" >/dev/null
done 
popd >/dev/null
