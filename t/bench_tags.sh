#!/bin/bash
#
# Usage: ./t/bench_tags.sh <limit number of tags to print>
#

LIMIT=${1:-3}
rm -f bench.master
for tag in master $(git tag -l | tac | head -n $LIMIT); do
  if ! test -e "bench.$tag"; then
    git checkout $tag >/dev/null 2>&1
    cd lmd && go test -ldflags "-s -w -X main.Build=1f7957488" -v -bench=B\* -run=^$ . -benchmem > ../bench.$tag
    cd ..
  fi
  if [ "$tag" != "master" ]; then
    git checkout master >/dev/null 2>&1
    ./t/diffBench.pl bench.master bench.$tag
  fi
done

git checkout master >/dev/null 2>&1
