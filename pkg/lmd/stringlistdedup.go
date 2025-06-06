package lmd

import (
	"fmt"
	"sort"
	"time"
	"unique"

	"github.com/OneOfOne/xxhash"
	"github.com/jellydator/ttlcache/v3"
)

var (
	// global dedupStrListCache cache for string lists.
	dedupStrListCache = ttlcache.New[uint64, []string]()

	// default ttl duration for cache entries.
	defaultCacheTTL = 10 * time.Minute
)

func dedupStringList(input []string) []string {
	if len(input) == 0 {
		return input
	}

	// Sort the input to ensure consistent ordering for deduplication.
	sort.Strings(input)

	hasher := xxhash.New64()
	for i := range input {
		_, err := hasher.WriteString(input[i])
		if err != nil {
			panic(fmt.Sprintf("hash function failed: %s", err.Error()))
		}
		_, err = hasher.WriteString(ListSepChar1)
		if err != nil {
			panic(fmt.Sprintf("hash function failed: %s", err.Error()))
		}
	}
	sum := hasher.Sum64()

	list := dedupStrListCache.Get(sum)
	if list != nil {
		return (list.Value())
	}

	dedupedList := make([]string, len(input))
	for i, str := range input {
		// deduplicate each string as well
		dedupedList[i] = unique.Make(str).Value()
	}

	dedupStrListCache.Set(sum, dedupedList, defaultCacheTTL)

	return dedupedList
}
