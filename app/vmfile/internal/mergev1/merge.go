package mergev1

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/simplemerge"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
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
	fs.MustMkdirIfNotExist(toPath)
	if len(fromPaths) > 1 && simplemerge.CheckMetricIDDuplicate(fromPaths) {
		return
	}
	ts := time.Now().UnixNano()
	mergeData(fromPaths, toPath, &ts)
	mergeIndex(fromPaths, toPath, &ts)
}

func mergeData(fromPaths []string, toPath string, ts *int64) {
	fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "big"))
	fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "small"))
	// 找到分区名
	for _, p := range fromPaths {
		// 拷贝数据
		dataDir := filepath.Join(p, storage.DataDirname)
		monthMap := map[string]struct{}{}
		for _, partitionName := range []string{"small", "big"} {
			srcPartitionDir := filepath.Join(dataDir, partitionName)
			if !fs.IsPathExist(srcPartitionDir) {
				continue
			}
			monthly := storage.GetMonthlyPartitionNames(srcPartitionDir)
			for _, month := range monthly {
				if partitionName == "small" {
					monthMap[month] = struct{}{}
				}
				monthlyPartitionDir := filepath.Join(srcPartitionDir, month)
				partNames := storage.GetPartNames(monthlyPartitionDir)
				dstMonthlyDir := filepath.Join(toPath, storage.DataDirname, partitionName, month)
				fs.MustMkdirIfNotExist(dstMonthlyDir)
				for _, part := range partNames {
					partDir := filepath.Join(monthlyPartitionDir, part)
					(*ts)++
					newPartName := fmt.Sprintf("%016X", *ts)
					simplemerge.CopyDir(partDir, filepath.Join(dstMonthlyDir, newPartName))
				}
			}
		}
		// 所有小分区的月份，大分区中必须存在
		for month := range monthMap {
			fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "big", month))
		}
	}
}

func mergeIndex(fromPaths []string, toPath string, ts *int64) {
	dstPrevTable := filepath.Join(toPath, storage.IndexdbDirname, fmt.Sprintf("%016X", *ts))
	(*ts)++
	fs.MustMkdirIfNotExist(dstPrevTable)
	//
	dstPartDir := filepath.Join(toPath, storage.IndexdbDirname, fmt.Sprintf("%016X", *ts), fmt.Sprintf("%016X", *ts+1))
	(*ts) += 2
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
		prevPartNames := storage.ReadPartsNameOfIndexTable(indexTablePrev)
		for _, partName := range currPartNames {
			partDir := filepath.Join(indexTableCurr, partName)
			dirs = append(dirs, partDir)
		}
		for _, partName := range prevPartNames {
			partDir := filepath.Join(indexTablePrev, partName)
			dirs = append(dirs, partDir)
		}
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
	fmt.Printf("OK\n")
	fmt.Printf("\tindex 1: add %d, skip %d\n", index1Filter.AddCount, index1Filter.SkipCount)
	fmt.Printf("\tindex 2: add %d, skip %d\n", index2Filter.AddCount, index2Filter.SkipCount)
	fmt.Printf("\tindex 3: add %d, skip %d\n", index3Filter.AddCount, index3Filter.SkipCount)
	fmt.Printf("\tindex 5: add %d, skip %d\n", index5Filter.AddCount, index5Filter.SkipCount)
	fmt.Printf("\tindex 6: add %d, skip %d\n", index6Filter.AddCount, index6Filter.SkipCount)
	fmt.Printf("\tindex 7: add %d, skip %d\n", index7Filter.AddCount, index7Filter.SkipCount)
}
