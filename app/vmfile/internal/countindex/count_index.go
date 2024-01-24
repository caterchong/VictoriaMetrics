package countindex

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func CountIndex(storageDataPath string, indexTableFlag int8) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	if indexTableFlag < 1 || indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", indexTableFlag)
	}
	var metricCountUseIndex1 uint64
	metricIDSet := metricidset.NewMetricIDSet()
	indexMap := map[byte]uint64{}
	allCounter := map[int]map[string]map[int]map[int]uint64{}
	var cnt uint64
	tempPrefix := []byte{storage.NsPrefixTagToMetricIDs, 0, 0, 0, 0, 0, 0, 0, 0, 1} // metricGroupName -> metric id
	// 统计索引 1 下面的其他索引
	addedMetricIDForIndex1MetricGroup := metricidset.NewMetricIDSet()
	addedMetricIDForIndex1GroupAndTag := metricidset.NewMetricIDSet()
	addedMetricIDForIndex1Tag := metricidset.NewMetricIDSet()
	tag := mergeset.TagIndex{}
	// 日期索引
	dateOfMetrics := make(map[uint64]*metricidset.MetricIDSet, 100)
	dateAndTagOfMetrics1 := make(map[uint64]*metricidset.MetricIDSet, 100)
	dateAndTagOfMetrics2 := make(map[uint64]*metricidset.MetricIDSet, 100)
	dateAndTagOfMetrics3 := make(map[uint64]*metricidset.MetricIDSet, 100)
	// 日期 + metric 索引
	dateOfMetricsForMetricData := make(map[uint64]*metricidset.MetricIDSet, 100)
	//
	callback := func(
		tableType int,
		partDir string,
		metaIndexRowNo int,
		blockHeaderNo int,
		itemNo int,
		data []byte) (isStop bool) {
		indexMap[data[0]]++
		cnt++ // total index count
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
		} else if data[0] == storage.NsPrefixTagToMetricIDs {
			//进一步统计下面的三种索引
			tag.Reset()
			if err := tag.Unmarshal(data); err != nil {
				logger.Panicf("err=%w", err)
			}
			var set *metricidset.MetricIDSet
			if len(tag.MetricGroup) > 0 {
				if len(tag.Key) > 0 {
					set = addedMetricIDForIndex1GroupAndTag
					indexMap[11]++
				} else {
					set = addedMetricIDForIndex1MetricGroup
					indexMap[12]++
				}
			} else {
				set = addedMetricIDForIndex1Tag
				indexMap[13]++
			}
			for _, metricID := range tag.MetricIDs {
				set.Add(tag.AccountID, tag.ProjectID, metricID)
			}
			//
			if bytes.Equal(tempPrefix, data[:len(tempPrefix)]) {
				left := data[len(tempPrefix):]
				idx := bytes.IndexByte(left, 1)
				if idx >= 0 {
					left = left[idx+1:]
					metricCountUseIndex1 += uint64(len(left) / 8)
				}
			}
		} else if data[0] == storage.NsPrefixDateToMetricID {
			// 日期索引
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			date := encoding.UnmarshalUint64(data[9:])
			data = data[17:]
			if len(data) < 8 || len(data)%8 != 0 {
				logger.Panicf("date index format error")
			}
			m, ok := dateOfMetrics[date]
			if !ok {
				m = metricidset.NewMetricIDSet()
				dateOfMetrics[date] = m
			}
			for i := 0; i < len(data); i += 8 {
				metricID := encoding.UnmarshalUint64(data[i : i+8])
				m.Add(accountID, projectID, metricID)
			}
		} else if data[0] == storage.NsPrefixDateTagToMetricIDs {
			// 日期 + tag -> metric ids
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			date := encoding.UnmarshalUint64(data[9:])
			data = data[17:]
			//进一步统计下面的三种索引
			tag.Reset()
			if err := tag.UnmarshalFromTag(data); err != nil {
				logger.Panicf("err=%w", err)
			}
			var set *metricidset.MetricIDSet
			var has bool
			if len(tag.MetricGroup) > 0 {
				if len(tag.Key) > 0 {
					set, has = dateAndTagOfMetrics1[date]
					if !has {
						set = metricidset.NewMetricIDSet()
						dateAndTagOfMetrics1[date] = set
					}
					indexMap[61]++
				} else {
					set, has = dateAndTagOfMetrics2[date]
					if !has {
						set = metricidset.NewMetricIDSet()
						dateAndTagOfMetrics2[date] = set
					}
					indexMap[62]++
				}
			} else {
				set, has = dateAndTagOfMetrics3[date]
				if !has {
					set = metricidset.NewMetricIDSet()
					dateAndTagOfMetrics3[date] = set
				}
				indexMap[63]++
			}
			for _, metricID := range tag.MetricIDs {
				set.Add(accountID, projectID, metricID)
			}
		} else if data[0] == storage.NsPrefixDateMetricNameToTSID {
			date := encoding.UnmarshalUint64(data[1:])
			data = data[9:]
			accountID := encoding.UnmarshalUint32(data[0:])
			projectID := encoding.UnmarshalUint32(data[4:])
			data = data[8:]
			idx := bytes.IndexByte(data, 1)
			if idx < 0 {
				logger.Panicf("date+metric index format error")
			}
			//metricGroupName := data[:idx]
			data = data[idx+1:]
			idx = bytes.IndexByte(data, 2)
			if idx < 0 {
				logger.Panicf("date+metric index format error, tag end not found")
			}
			data = data[idx+1:]
			if len(data) < 8 || len(data)%8 != 0 {
				logger.Panicf("date index format error")
			}
			m, ok := dateOfMetricsForMetricData[date]
			if !ok {
				m = metricidset.NewMetricIDSet()
				dateOfMetricsForMetricData[date] = m
			}
			for i := 0; i < len(data); i += 8 {
				metricID := encoding.UnmarshalUint64(data[i : i+8])
				m.Add(accountID, projectID, metricID)
			}
			// curTag := storage.Tag{}
			// var err error
			// for {
			// 	curTag.Reset()
			// 	data, err = curTag.Unmarshal(data)
			// }
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
		if indexType > 10 {
			continue
		}
		count := indexMap[byte(indexType)]
		fmt.Printf("\tindex type=%d, count=%d\n", indexType, count)
		if indexType == 1 {
			fmt.Printf("\t\t sub type=GroupAndTag, count=%d, metricID=%d\n", indexMap[11], addedMetricIDForIndex1GroupAndTag.Len())
			fmt.Printf("\t\t sub type=MetricGroup, count=%d, metricID=%d\n", indexMap[12], addedMetricIDForIndex1MetricGroup.Len())
			fmt.Printf("\t\t sub type=Tag, count=%d, metricID=%d\n", indexMap[13], addedMetricIDForIndex1Tag.Len())
		} else if indexType == storage.NsPrefixDateToMetricID {
			// 输出日期
			for _, date := range sortDateKey(dateOfMetrics) {
				set := dateOfMetrics[uint64(date)]
				fmt.Printf("\t\t%s: metric id count:%d\n", dateToString(uint64(date)), set.Len())
			}
		} else if indexType == storage.NsPrefixDateTagToMetricIDs {
			// 输出日期
			fmt.Printf("\t\t sub type=GroupAndTag, count=%d\n", indexMap[61])
			for _, date := range sortDateKey(dateAndTagOfMetrics1) {
				set := dateAndTagOfMetrics1[uint64(date)]
				fmt.Printf("\t\t\t%s: metric id count:%d\n", dateToString(uint64(date)), set.Len())
			}
			fmt.Printf("\t\t sub type=MetricGroup, count=%d\n", indexMap[62])
			for _, date := range sortDateKey(dateAndTagOfMetrics2) {
				set := dateAndTagOfMetrics2[uint64(date)]
				fmt.Printf("\t\t\t%s: metric id count:%d\n", dateToString(uint64(date)), set.Len())
			}
			fmt.Printf("\t\t sub type=Tag, count=%d\n", indexMap[63])
			for _, date := range sortDateKey(dateAndTagOfMetrics3) {
				set := dateAndTagOfMetrics3[uint64(date)]
				fmt.Printf("\t\t\t%s: metric id count:%d\n", dateToString(uint64(date)), set.Len())
			}
		} else if indexType == storage.NsPrefixDateMetricNameToTSID {
			// 输出日期
			for _, date := range sortDateKey(dateOfMetricsForMetricData) {
				set := dateOfMetricsForMetricData[uint64(date)]
				fmt.Printf("\t\t%s: metric id count:%d\n", dateToString(uint64(date)), set.Len())
			}
		}
	}
	fmt.Printf("total table count:%d\n", len(allCounter))
	fmt.Printf("total part count:%d\n", partCount)
	fmt.Printf("total index block count:%d\n", indexBlockCount)
	fmt.Printf("total block count:%d\n", blockCount)
	fmt.Printf("total item count:%d\n", totalItemCount)
	fmt.Printf("record count:%d\n", cnt)
	fmt.Printf("unique metric id count:%d\n", metricIDSet.Len())
	fmt.Printf("\tmetricCountUseIndex1=%d\n", metricCountUseIndex1)
	checkTree(storageDataPath)
}

func sortDateKey(m map[uint64]*metricidset.MetricIDSet) []int {
	out := make([]int, 0, len(m))
	for date := range m {
		out = append(out, int(date))
	}
	sort.Ints(out)
	return out
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

func checkTree(storageDataPath string) {
	indexDbDir := filepath.Join(storageDataPath, storage.IndexdbDirname)
	_, currPath, prevPath := storage.GetIndexDBTableNames(indexDbDir)
	for _, tableDir := range []string{currPath, prevPath} {
		partNames := storage.ReadPartsNameOfIndexTable(tableDir)
		mergeset.CheckTree(tableDir, partNames)
	}
}

func dateToString(date uint64) string {
	if date == 0 {
		return "1970-01-01"
	}
	t := time.Unix(int64(date*24*3600), 0).UTC()
	return t.Format("2006-01-02")
}
