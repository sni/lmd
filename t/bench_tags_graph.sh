#!/bin/bash
#
# bench_tags_graph.sh: shows a gnuplot graph for given number of benchmarks
#
# Usage: ./t/bench_tags_graph.sh <limit number of tags to print> <limit number of commits>
#

TAGLIMIT=${1:-3}
COMMITLIMIT=${2:-5}
FILTER=$3
./t/create_benchmarks.sh $TAGLIMIT $COMMITLIMIT

CURFOLDER=$(pwd)
BENCHMARKS=$(pwd)/.benchmarks
cd $BENCHMARKS
LASTFILE=$(ls -1 bench.* | tail -n 1)
FILES=$(echo $(ls -1 bench.*.*.v* | tail -n 3 ; ls -1 bench.*.* | tail -n 5) | sort)

for bench in $(grep ^Benchmark $LASTFILE | grep alloc | awk '{print $1}'); do
  if [ -n $FILTER ] && ! [[ $bench =~ $FILTER ]]; then
    continue
  fi
  echo ""
  echo "***********************************"
  echo "$bench:"

  nr=0
  ticks=$(
  grep ^$bench $FILES | grep alloc | \
    while read line; do
      tag=$(echo $line | cut -d ':' -f 1 | cut -d '.' -f 4-7 | tr -d 'v')
      if [ "x$tag" = "x" ]; then
      tag="x"
      fi
      echo -n "'$tag' $nr,"
      nr=$((nr + 1))
    done
  )

  grep ^$bench $FILES | grep alloc | \
    while read line; do
      ops=$(echo $line | awk '{print $3}')
      echo "$ops"
    done > ops.txt

  grep ^$bench $FILES | grep alloc | \
    while read line; do
      bytes=$(echo $line | awk '{print $5}')
      echo "$bytes"
    done > bytes.txt

  grep ^$bench $FILES | grep alloc | \
    while read line; do
      alloc=$(echo $line | awk '{print $7}')
      echo "$alloc"
    done > alloc.txt

  gnuplot <<EOT
set terminal 'dumb' size 210,20;
set multiplot layout 1,3;
unset key;
set yrange ["0":];
set boxwidth 1;
set style data lines
set xtics ($ticks);
set title 'ns/op';
plot 'ops.txt';
set title 'B/op';
plot 'bytes.txt';
set title 'allocs/op';
plot 'alloc.txt';
unset multiplot;
exit;
EOT

  rm -f ops.txt
  rm -f bytes.txt
  rm -f alloc.txt
done
