#!/bin/bash
#
# Usage: ./t/bench_tags_graph.sh <limit number of tags to print>
#

LIMIT=${1:-7}
#rm -f bench.master
for tag in master $(git tag -l | tac | head -n $LIMIT); do
  if ! test -e "bench.$tag"; then
    git checkout $tag >/dev/null 2>&1
    cd lmd && go test -ldflags "-s -w -X main.Build=test" -v -bench=B\* -run=^$ . -benchmem > ../bench.$tag
    cd ..
  fi
done

for bench in $(grep ^Benchmark bench.master | awk '{print $1}'); do
  echo ""
  echo "***********************************"
  echo "$bench:"

  nr=0
  ticks=$(
  grep ^$bench bench.v* bench.master | tail -n $LIMIT | \
    while read line; do
      tag=$(echo $line | awk -F: '{print $1}' | sed -e 's/^bench\.//g')
      echo -n "'$tag' $nr,"
      nr=$((nr + 1))
    done
  )

  grep ^$bench bench.v* bench.master | tail -n $LIMIT | \
    while read line; do
      ops=$(echo $line | awk '{print $3}')
      echo "$ops"
    done > ops.txt

  grep ^$bench bench.v* bench.master | tail -n $LIMIT | \
    while read line; do
      bytes=$(echo $line | awk '{print $5}')
      echo "$bytes"
    done > bytes.txt

  grep ^$bench bench.v* bench.master | tail -n $LIMIT | \
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