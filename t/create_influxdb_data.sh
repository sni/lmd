#!/bin/bash
#
# create_influxdb_data.sh: creates data file for given number of benchmarks usable for influxdb data imports
#
# Usage: ./t/create_influxdb_data.sh <database> <limit number of tags to print> <limit number of commits> > /tmp/data
#        influx -import -path=/tmp/data -precision=s
#

DATABASE=$1
TAGLIMIT=${2:-5}
COMMITLIMIT=${3:-10}
FOLDER=$(pwd)/.benchmarks
HOSTNAME=$(hostname)
DIR=$(pwd)

if [ "x$DATABASE" = "x" ]; then
    echo "usage: $0 <database>"
    exit 1
fi

./t/create_benchmarks.sh $TAGLIMIT $COMMITLIMIT >&2

cat <<EOT
# DDL
CREATE DATABASE $DATABASE WITH DURATION INF SHARD DURATION 90d
DROP SERIES FROM $DATABASE WHERE "hostname" = "$HOSTNAME" AND "folder" = "$DIR"

# DML
# CONTEXT-DATABASE: $DATABASE

EOT

cd $FOLDER
find . -name bench\* | sort | \
while read file; do
    SIFS="$IFS"
    IFS="."
    FILE=($file)
    timestamp=${FILE[2]}
    hash=${FILE[3]}
    IFS="$SIFS"
    msg=$(git show -s --format="%s" $hash | tr -d '"')
    grep "ns/op" $file | grep "allocs/op" | \
    grep -v "NaN" | \
        while read line; do
            test=$(echo "$line" | awk '{ print $1 }' | sed -e 's/\-[0-9]*$//g')
            LINE=($line)
            # set tags
            echo -n 'benchmark,name='$test',hostname='$HOSTNAME',folder='$DIR' '
            # set fields
            echo -n 'commit="'$hash'",msg="'$msg'",ns_per_op='${LINE[2]}'i,allocs_per_op='${LINE[6]}'i,bytes_per_op='${LINE[4]}'i '
            echo $timestamp
        done
done

# add all tags
for tag in $(git tag -l); do
  timestamp=$(git show -s --format="%at" $tag)
  echo 'tags,hostname='$HOSTNAME',folder='$DIR' title="'$tag'" '$timestamp
done

# add all commits
git show -s --format="%h %at" -9999999 | \
while read commit timestamp; do
  msg=$(git show -s --format="%s" $commit | tr -d '"')
  echo 'commits,hostname='$HOSTNAME',folder='$DIR' title="'$commit'",text="'$msg'" '$timestamp
done
