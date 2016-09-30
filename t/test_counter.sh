#!/bin/bash

NUMTESTS=$(cat *_test.go | grep -c "func Test")
RUN=0
PASS=0

while read line
do
  echo "$line" | sed ''/PASS/s//$(printf "\033[32mPASS\033[0m")/'' | sed ''/FAIL/s//$(printf "\033[31mFAIL\033[0m")/''
  echo "$line" | grep -c -e "\(PASS\|SKIP\):" >/dev/null && PASS=$((PASS + 1))
  echo "$line" | grep -c -e "=== RUN"         >/dev/null && RUN=$((RUN + 1))
done <&0

echo "$PASS/$RUN tests passed"
[ $RUN -eq $NUMTESTS -a $RUN -eq $PASS ] || exit 1
