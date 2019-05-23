#!/bin/bash
#
# Usage: ./t/bench_tags.sh <limit number of tags to print> <limit number of commits>
#

CURBRANCH=$(git branch --no-color 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/')
TAGLIMIT=${1:-10}
COMMITLIMIT=${2:-10}
FOLDER=$(pwd)/.benchmarks
SRCDIR="$(pwd)/lmd"
CURDIR="$(pwd)"

mkdir -p $FOLDER
cd $SRCDIR
for tag in $(git tag -l | tac | head -n $TAGLIMIT); do
  if [ "x$(ls $FOLDER/bench.*.$tag* 2>/dev/null)" = "x" ]; then
    git checkout $tag >/dev/null 2>&1 || ( echo "changing to tag $tag failed"; exit 1 )
    name="bench.$(git show -s --format="%at.%h" -1).$tag"
    echo "creating benchmark: $name ($(git show -s --format="%s" -1))"
    go test -ldflags "-s -w -X main.Build=test" -v -bench=B\* -run=^$ . -benchmem > $FOLDER/$name
  fi
done

cd $CURDIR
git checkout $CURBRANCH >/dev/null 2>&1
cd $SRCDIR

for hash in $(git show -s --format="%h" -$COMMITLIMIT); do
  if [ "x$(ls $FOLDER/bench.*.$hash* 2>/dev/null)" = "x" ]; then
    git checkout $hash >/dev/null 2>&1 || ( echo "changing to tag $tag failed"; exit 1 )
    name="bench.$(git show -s --format="%at.%h" -1)"
    echo "creating benchmark: $name ($(git show -s --format="%s" -1))"
    go test -ldflags "-s -w -X main.Build=test" -v -bench=B\* -run=^$ . -benchmem > $FOLDER/$name
  fi
done

cd $CURDIR
git checkout $CURBRANCH >/dev/null 2>&1

# cleanup none-existing tags
cd $FOLDER
for hash in $(comm -13 <(git show -s --format="%h" -9999999 | sort) <(ls -1 bench.* | cut -d '.' -f 3 | sort) | sort -u); do
    for file in $(ls -1 bench.*$hash*); do
      echo "removing obsolete benchmark: $file"
      rm -f $file
    done
done
cd $CURDIR
