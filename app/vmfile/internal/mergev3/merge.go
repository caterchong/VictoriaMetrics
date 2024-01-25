package mergev3

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/simplemerge"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func Merge(fromPaths []string, toPath string) {
	if len(fromPaths) < 1 {
		logger.Errorf("from path count must great 0")
		return
	}
	for _, p := range fromPaths {
		if !fs.IsPathExist(p) {
			logger.Errorf("from path %s not exists", p)
			return
		}
	}
	if len(fromPaths) > 1 && simplemerge.CheckMetricIDDuplicate(fromPaths) {
		return
	}
	// 计算出重复的索引有多少，然后把重复索引丢弃
	mainid, set, err := GetDupIndexInfo(fromPaths)
	if err != nil {
		logger.Errorf("err=%w", err)
		return
	}
	//
	ts := uint64(time.Now().UnixNano())
	if cgroup.AvailableCPUs() > 1 {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			//mergeData(fromPaths, toPath, &ts)
		}()
		go func() {
			defer wg.Done()
			mergeIndex(fromPaths, toPath, &ts, mainid, set)
		}()
		wg.Wait()
	} else {
		mergeIndex(fromPaths, toPath, &ts, mainid, set)
		//mergeData(fromPaths, toPath, &ts)
	}
}

// func MergeDataV2(fromPaths []string, toPath string) {
// 	if len(fromPaths) < 1 {
// 		logger.Errorf("from path count must great 0")
// 		return
// 	}
// 	for _, p := range fromPaths {
// 		if !fs.IsPathExist(p) {
// 			logger.Errorf("from path %s not exists", p)
// 			return
// 		}
// 	}
// 	//fs.MustMkdirIfNotExist(toPath)
// 	ts := time.Now().UnixNano()
// 	dstPartitionDir := filepath.Join(toPath, storage.DataDirname, "big")
// 	fs.MustMkdirIfNotExist(dstPartitionDir)
// 	fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "small"))
// 	mergeData(fromPaths, dstPartitionDir, &ts)
// 	fmt.Printf("OK\n")
// }

func mergeData(fromPaths []string, toPath string, ts *uint64) {
	dstPartitionDir := filepath.Join(toPath, storage.DataDirname, "big")
	fs.MustMkdirIfNotExist(dstPartitionDir)
	fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "small"))

	// 找到分区名
	monthMap := map[string][]string{} // 所有存在过的月份分区，都要记录下来
	for _, p := range fromPaths {
		// 拷贝数据
		dataDir := filepath.Join(p, storage.DataDirname)
		for _, partitionName := range []string{"small", "big"} {
			srcPartitionDir := filepath.Join(dataDir, partitionName)
			if !fs.IsPathExist(srcPartitionDir) {
				continue
			}
			monthly := storage.GetMonthlyPartitionNames(srcPartitionDir)
			for _, month := range monthly {
				monthlyPartitionDir := filepath.Join(srcPartitionDir, month)
				arr, has := monthMap[month]
				if !has {
					arr = make([]string, 0, 12)
				}
				monthMap[month] = append(arr, monthlyPartitionDir)
			}
		}
	}
	// 遍历每个月份
	partDirs := make([]string, 0, 100)
	for month, dirs := range monthMap {
		dstPartDir := filepath.Join(dstPartitionDir, month, fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
		partDirs = partDirs[:0]
		for _, monthlyPartitionDir := range dirs {
			partNames := storage.GetPartNames(monthlyPartitionDir)
			for _, part := range partNames {
				partDir := filepath.Join(monthlyPartitionDir, part)
				partDirs = append(partDirs, partDir) // 以月份归类的源数据目录
			}
		}
		if err := storage.Merge(partDirs, dstPartDir); err != nil {
			logger.Panicf("merge error, err=%w", err)
		}
	}
	fmt.Printf("Merge data ok\n")
}

func mergeIndex(fromPaths []string, toPath string, ts *uint64, mainid map[uint64][]uint64, setOfDupID *uint64set.Set) {
	dstPrevTable := filepath.Join(toPath, storage.IndexdbDirname, fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
	fs.MustMkdirIfNotExist(dstPrevTable)
	//
	dstPartDir := filepath.Join(toPath, storage.IndexdbDirname,
		fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)),
		fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
	//fs.MustMkdirIfNotExist(dstPartDir)  // 必须不能创建
	// 所有数据的所有 part 目录
	dirs := GetIndexDbPartDirs(fromPaths)
	if len(dirs) == 0 {
		logger.Errorf("not have any dir to merge")
		return
	}
	//
	index1Filter := reindex.NewFilterForTagIndex()
	index2Filter := reindex.NewFilterForMetricIDIndex()
	index3Filter := reindex.NewFilterForMetricIDIndex()
	index5Filter := reindex.NewFilterForDateToMetricIDIndex()
	index6Filter := reindex.NewFilterForDateAndTagToMetricIDIndex()
	index7Filter := reindex.NewFilterForDateAndMetricNameIndex()

	prepareBlockCallback := func(alldata []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
		idx := 0
		//has7 := false
		for idx < len(items) {
			cur := items[idx].Bytes(alldata)
			indexType := cur[0]
			//
			switch indexType {
			case storage.NsPrefixTagToMetricIDs:
				if index1Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index1Filter.SkipCount++
					continue
				}
				//增加重复索引的判断逻辑   需要在一个数组中操作，删除不要的部分

				index1Filter.AddCount++
			case storage.NsPrefixMetricIDToTSID:
				if index2Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index2Filter.SkipCount++
					continue
				}
				index2Filter.AddCount++
			case storage.NsPrefixMetricIDToMetricName:
				if index3Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index3Filter.SkipCount++
					continue
				}
				index3Filter.AddCount++
			case storage.NsPrefixDateToMetricID:
				if index5Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index5Filter.SkipCount++
					continue
				}
				index5Filter.AddCount++
			case storage.NsPrefixDateTagToMetricIDs:
				if index6Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index6Filter.SkipCount++
					continue
				}
				index6Filter.AddCount++
			case storage.NsPrefixDateMetricNameToTSID:
				//has7 = true
				if index7Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index7Filter.SkipCount++
					continue
				}
				index7Filter.AddCount++
			}
			idx++
		}
		// if len(items) > 0 && has7 {
		// 	// 处理 metric name 的逻辑
		// 	checkMetricNameDup(alldata, items)
		// }
		return alldata, items
	}
	mergeset.MergePartsByPartDirs("", dirs,
		dstPartDir,
		prepareBlockCallback,
		nil,
	)
	fmt.Printf("Merge Index OK\n")
	fmt.Printf("\tindex 1: add %d, skip %d\n", index1Filter.AddCount, index1Filter.SkipCount)
	fmt.Printf("\tindex 2: add %d, skip %d\n", index2Filter.AddCount, index2Filter.SkipCount)
	fmt.Printf("\tindex 3: add %d, skip %d\n", index3Filter.AddCount, index3Filter.SkipCount)
	fmt.Printf("\tindex 5: add %d, skip %d\n", index5Filter.AddCount, index5Filter.SkipCount)
	fmt.Printf("\tindex 6: add %d, skip %d\n", index6Filter.AddCount, index6Filter.SkipCount)
	fmt.Printf("\tindex 7: add %d, skip %d\n", index7Filter.AddCount, index7Filter.SkipCount)
	fmt.Printf("\n")
	// var dupCountOfMetricName uint64
	// for _, m := range metricNameMap {
	// 	dupCountOfMetricName += uint64(len(m))
	// }
	// fmt.Printf("dup metric name: %d, dup id=%d\n", len(metricNameMap), dupCountOfMetricName)
}

const metricNameStartAt = 17

var metricNameMap = map[string]map[uint64]struct{}{}

func checkMetricNameDup(alldata []byte, items []mergeset.Item) {
	idx := 0
	var prevItem []byte
	for idx < len(items) {
		data := items[idx].Bytes(alldata)
		if data[0] != storage.NsPrefixDateMetricNameToTSID {
			prevItem = nil
			idx++
			continue
		}
		// date := encoding.UnmarshalUint64(data[1:])
		// accountID := encoding.UnmarshalUint32(data[9:])
		// projectID := encoding.UnmarshalUint32(data[13:])
		data = data[metricNameStartAt:]
		// idx := bytes.IndexByte(data, 1)
		// if idx < 0 {
		// 	logger.Panicf("date+metric index format error")
		// }
		// //metricGroupName := data[:idx]
		// data = data[idx+1:]
		loc := bytes.IndexByte(data, 2)
		if loc < 0 {
			logger.Panicf("date+metric index format error, tag end not found")
		}
		toCompare := data[:loc+1] //
		if len(prevItem) == 0 {
			prevItem = toCompare
			idx++
			continue
		}
		if !bytes.Equal(prevItem, toCompare) {
			prevItem = toCompare
			idx++
			continue
		}
		data = data[loc+1:]
		if len(data) < 8 || len(data)%8 != 0 {
			logger.Panicf("metric data format error")
		}
		// 1个 metricName 指向 多个 tsid
		metricNameKey := string(toCompare)
		metricIDs, has := metricNameMap[metricNameKey]
		if !has {
			metricIDs = make(map[uint64]struct{})
			metricNameMap[metricNameKey] = metricIDs
		}
		// 把 metric id 加入 map
		for i := 0; i < len(data); i += 8 {
			metricIDs[encoding.UnmarshalUint64(data[i:i+8])] = struct{}{}
		}
		//还要把前一条加入到 map
		prevData := items[idx-1].Bytes(alldata)
		a1 := prevData[metricNameStartAt:]
		loc = bytes.IndexByte(a1, 2)
		if loc < 0 {
			logger.Panicf("prev format error, not found metric")
		}
		a2 := a1[:loc+1]
		if !bytes.Equal(a2, toCompare) || !bytes.Equal(a2, prevItem) {
			logger.Panicf("prev format error,metric mismatch")
		}
		a3 := a1[loc+1:]
		if len(a3) < 8 || len(a3)%8 != 0 {
			logger.Panicf("prev format error,metric id error")
		}
		prevItemRow := prevData[metricNameStartAt+len(toCompare):]
		if len(prevItemRow) < 8 || len(prevItemRow)%8 != 0 {
			fmt.Printf("idx: %d\n", idx)
			fmt.Printf("all : %X\n", string(prevData))
			fmt.Printf("comp: %X\n", string(toCompare))
			fmt.Printf("prev: %X\n", string(prevItem))
			fmt.Printf("tail: %X(%d)\n", string(prevItemRow), len(prevItemRow))
			logger.Panicf("prev metric data format error")
		}
		for i := 0; i < len(prevItemRow); i += 8 {
			metricIDs[encoding.UnmarshalUint64(prevItemRow[i:i+8])] = struct{}{}
		}
		idx++
	}
}
