#!/bin/bash

FOUND=0

while read line
do
  if [[ "$line" == *"_test.go"*  ]]; then
    continue
  fi
  if [[ "$line" == *"ParseRequestHeaderLine request.go"*  ]]; then
    continue
  fi
  if [[ "$line" == *"Less response.go"*  ]]; then
    continue
  fi
  if [[ "$line" == *"matchStringListFilter filter.go"*  ]]; then
    continue
  fi
  if [[ "$line" == *"GetVirtRowComputedValue peer.go"*  ]]; then
    continue
  fi
  echo "FAIL: complexity: $line" | sed ''/FAIL/s//$(printf "\033[31mFAIL\033[0m")/''
  FOUND=$((FOUND + 1))
done <&0

exit $FOUND
