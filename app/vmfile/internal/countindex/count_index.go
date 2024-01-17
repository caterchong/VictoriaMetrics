package countindex

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func CountIndex(storageDataPath string, indexTableFlag int8) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	if indexTableFlag < 1 || indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", indexTableFlag)
	}
	metricIDSet := metricidset.NewMetricIDSet()
	indexMap := map[byte]uint64{}
	allCounter := map[int]map[string]map[int]map[int]uint64{}
	var cnt uint64
	callback := func(
		tableType int,
		partDir string,
		metaIndexRowNo int,
		blockHeaderNo int,
		itemNo int,
		data []byte) (isStop bool) {
		indexMap[data[0]]++
		cnt++ // total inde count
		table, ok := allCounter[tableType]
		if !ok {
			table = make(map[string]map[int]map[int]uint64)
			allCounter[tableType] = table
		}
		partName := filepath.Base(partDir)
		part, ok := table[partName]
		if !ok {
			part = make(map[int]map[int]uint64)
			table[partName] = part
		}
		indexBlock, ok := part[metaIndexRowNo]
		if !ok {
			indexBlock = make(map[int]uint64)
			part[metaIndexRowNo] = indexBlock
		}
		indexBlock[blockHeaderNo]++
		if data[0] == storage.NsPrefixMetricIDToMetricName {
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			metricID := encoding.UnmarshalUint64(data[9:])
			if metricIDSet.Add(accountID, projectID, metricID) {
				return // 不添加重复的 metric
			}
		}
		return false
	}
	err := storage.IterateAllIndexes(storageDataPath, indexTableFlag, callback)
	if err != nil {
		logger.Panicf("storage.IterateAllIndexes error, err=%w", err)
	}
	//
	partCount := 0
	indexBlockCount := 0
	blockCount := 0
	var totalItemCount uint64
	for tableIndex, table := range allCounter {
		fmt.Printf("table index:%d, part count=%d\n", tableIndex, len(table))
		for _, partName := range sortPartName(table) {
			part := table[partName]
			fmt.Printf("\tpart name:%s, index block count=%d\n", partName, len(part))
			partCount++
			for _, indexBlockNo := range sortIndexBlockNo(part) {
				indexBlock := part[indexBlockNo]
				fmt.Printf("\t\tindex block number:%d, block count=%d\n", indexBlockNo, len(indexBlock))
				indexBlockCount++
				for _, blockNo := range sortBlockNo(indexBlock) {
					itemCount := indexBlock[blockNo]
					fmt.Printf("\t\t\tblock number:%d, item count=%d\n", blockNo, itemCount)
					blockCount++
					totalItemCount += itemCount
				}
			}
		}
	}
	fmt.Printf("count by index type:\n")
	for _, indexType := range sortIndexType(indexMap) {
		count := indexMap[byte(indexType)]
		fmt.Printf("\tindex type=%d, count=%d\n", indexType, count)
	}
	fmt.Printf("total table count:%d\n", len(allCounter))
	fmt.Printf("total part count:%d\n", partCount)
	fmt.Printf("total index block count:%d\n", indexBlockCount)
	fmt.Printf("total block count:%d\n", blockCount)
	fmt.Printf("total item count:%d\n", totalItemCount)
	fmt.Printf("record count:%d\n", cnt)
	fmt.Printf("unique metric id count:%d\n", metricIDSet.Len())
}

func sortPartName(m map[string]map[int]map[int]uint64) []string {
	arr := make([]string, 0, len(m))
	for name := range m {
		arr = append(arr, name)
	}
	sort.Strings(arr)
	return arr
}

func sortIndexType(m map[byte]uint64) []int {
	arr := make([]int, 0, len(m))
	for indexType := range m {
		arr = append(arr, int(indexType))
	}
	sort.Ints(arr)
	return arr
}

func sortBlockNo(m map[int]uint64) []int {
	arr := make([]int, 0, len(m))
	for blockNo := range m {
		arr = append(arr, blockNo)
	}
	sort.Ints(arr)
	return arr
}

func sortIndexBlockNo(m map[int]map[int]uint64) []int {
	arr := make([]int, 0, len(m))
	for indexBlockNo := range m {
		arr = append(arr, indexBlockNo)
	}
	sort.Ints(arr)
	return arr
}
