#!/bin/bash
#
# create_benchmarks.sh: create benchmark files for given number of tags and commits
#
# Usage: ./t/create_benchmarks.sh <limit number of tags to print> <limit number of commits>
#

CURBRANCH=$(git branch --no-color 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/')
TAGLIMIT=${1:-10}
COMMITLIMIT=${2:-10}
FOLDER=$(pwd)/.benchmarks
SRCDIR="$(pwd)/lmd"
CURDIR="$(pwd)"

mkdir -p $FOLDER
MISSING=""
NUM=0

for hash in $(git show -s --format="%h" -$COMMITLIMIT); do
  if [ "x$(ls $FOLDER/bench.*.$hash* 2>/dev/null)" = "x" ]; then
    TAG=$(git describe --tag --exact-match $hash 2>/dev/null)
    # only if not directly on a tag, tags will be handled with the next block
    if [ "x$TAG" = "x" ]; then
      name="bench.$(git show -s --format="%at.%h" $hash)"
      MISSING="$MISSING $name"
      NUM=$(( NUM + 1 ))
    fi
  fi
done

for tag in $(git tag -l | tac | head -n $TAGLIMIT); do
  if [ "x$(ls $FOLDER/bench.*.$tag* 2>/dev/null)" = "x" ]; then
    name="bench.$(git show -s --format="%at.%h" $tag).$tag"
    MISSING="$MISSING $name"
    NUM=$(( NUM + 1 ))
  fi
done

# cleanup none-existing tags
cd $FOLDER
for hash in $(comm -13 <(git show -s --format="%h" -9999999 | sort) <(find $FOLDER -name bench\* | cut -d '.' -f 4 | sort -u) | sort -u); do
    for file in $(ls -1 bench.*$hash*); do
      echo "removing obsolete benchmark: $file"
      rm -f $file
    done
done

CUR=1
for file in $MISSING; do
  cd $CURDIR
  hash=$(echo "$file" | cut -d "." -f 3)
  git checkout go.mod >/dev/null 2>&1
  git checkout $hash >/dev/null 2>&1 || { echo "changing to hash $hash failed"; exit 1; }
  go mod vendor >/dev/null 2>&1
  cd $SRCDIR
  printf "%02d/%02d creating benchmark: %s (%s)\n" $CUR $NUM $file "$(git show -s --format="%s" $hash)"
  go test -ldflags "-s -w -X main.Build=test" -v -bench=B\* -run=^$ . -benchmem -benchtime 10s > $FOLDER/$file
  CUR=$(( CUR + 1 ))
done

cd $CURDIR
git checkout go.mod >/dev/null 2>&1
git checkout $CURBRANCH >/dev/null 2>&1
go mod vendor >/dev/null 2>&1

