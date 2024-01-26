package mergev2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/simplemerge"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func Merge(fromPaths []string, toPath string) {
	if len(fromPaths) < 1 {
		logger.Errorf("from path count must great 0")
		os.Exit(1)
		return
	}
	for _, p := range fromPaths {
		if !fs.IsPathExist(p) {
			logger.Errorf("from path %s not exists", p)
			os.Exit(2)
			return
		}
	}
	if len(fromPaths) > 1 && simplemerge.CheckMetricIDDuplicate(fromPaths) {
		os.Exit(3)
		return
	}
	ts := uint64(time.Now().UnixNano())
	if cgroup.AvailableCPUs() > 1 {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			mergeData(fromPaths, toPath, &ts)
		}()
		go func() {
			defer wg.Done()
			mergeIndex(fromPaths, toPath, &ts)
		}()
		wg.Wait()
	} else {
		mergeIndex(fromPaths, toPath, &ts)
		mergeData(fromPaths, toPath, &ts)
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

func mergeIndex(fromPaths []string, toPath string, ts *uint64) {
	dstPrevTable := filepath.Join(toPath, storage.IndexdbDirname, fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
	fs.MustMkdirIfNotExist(dstPrevTable)
	//
	dstPartDir := filepath.Join(toPath, storage.IndexdbDirname,
		fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)),
		fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
	//fs.MustMkdirIfNotExist(dstPartDir)  // 必须不能创建
	// 所有数据的所有 part 目录
	dirs := make([]string, 0, 200)
	for _, p := range fromPaths {
		indexdbDir := filepath.Join(p, storage.IndexdbDirname)
		if !fs.IsPathExist(indexdbDir) {
			logger.Panicf("indexdb [%s] not exists", indexdbDir)
		}
		_, indexTableCurr, indexTablePrev := storage.GetIndexDBTableNames(indexdbDir)
		// 找到 part names
		currPartNames := storage.ReadPartsNameOfIndexTable(indexTableCurr)
		if len(currPartNames) == 0 {
			logger.Infof("current partition empty: %s", indexTableCurr)
		}
		prevPartNames := storage.ReadPartsNameOfIndexTable(indexTablePrev)
		if len(prevPartNames) == 0 {
			logger.Infof("prev partition empty: %s", indexTablePrev)
		}
		for _, partName := range currPartNames {
			partDir := filepath.Join(indexTableCurr, partName)
			dirs = append(dirs, partDir)
		}
		for _, partName := range prevPartNames {
			partDir := filepath.Join(indexTablePrev, partName)
			dirs = append(dirs, partDir)
		}
	}
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
				if index7Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index7Filter.SkipCount++
					continue
				}
				index7Filter.AddCount++
			}
			idx++
		}
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
}
