package mergev2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/simplemerge"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// convert from "2020-01-01 00:00:00" to unix timestamp
func convertTimeToUnix(timeStr string) int64 {
	if timeStr == "" {
		return 0
	}
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		logger.Errorf("parse time error: %s", err)
		os.Exit(1)
	}
	return t.UnixMilli()
}

func Merge(fromPaths []string, toPath string, retentionDeadline string) {
	if len(fromPaths) < 1 {
		logger.Errorf("from path count must great 0")
		os.Exit(1)
		return
	}
	retentionDeadlineMilli := convertTimeToUnix(retentionDeadline)
	for _, p := range fromPaths {
		if !fs.IsPathExist(p) {
			logger.Errorf("from path %s not exists", p)
			os.Exit(2)
			return
		}
	}
	logger.Infof("retention deadline: %d, topath:%s", retentionDeadlineMilli, toPath)
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
			mergeData(fromPaths, toPath, &ts, retentionDeadlineMilli)
		}()
		go func() {
			defer wg.Done()
			mergeIndex(fromPaths, toPath, &ts, retentionDeadlineMilli)
		}()
		wg.Wait()
	} else {
		mergeIndex(fromPaths, toPath, &ts, retentionDeadlineMilli)
		mergeData(fromPaths, toPath, &ts, retentionDeadlineMilli)
	}
}

func mergeData(fromPaths []string, toPath string, ts *uint64, retentionDeadline int64) {
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
		if err := storage.Merge(partDirs, dstPartDir, retentionDeadline); err != nil {
			logger.Panicf("merge error, err=%w", err)
		}
	}
	fmt.Printf("Merge data ok\n")
}

func mergeIndex(fromPaths []string, toPath string, ts *uint64, retentionDeadline int64) {
	dstPrevTable := filepath.Join(toPath, storage.IndexdbDirname, fmt.Sprintf("%016X", atomic.AddUint64(ts, 1)))
	fs.MustMkdirIfNotExist(dstPrevTable)
	fs.MustWriteSync(filepath.Join(dstPrevTable, ".placeholder"), []byte("just for placeholder")) // 不写这个文件，backup到s3丢失目录

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

	//换成秒，精确到天
	retentionDate := ((retentionDeadline / 1000) + 86399) / 86400

	skipCount := 0
	// 对于index来说，所谓陈旧index，只是带有Date的Index， Date日期在指定的retentionDeadline之前的需要删除
	removeStaleDateIndex := func(alldata []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
		idx := 0
		newItems := items[:0]
		for idx < len(items) {
			cur := items[idx].Bytes(alldata)
			indexType := cur[0]
			if indexType == storage.NsPrefixDateToMetricID || indexType == storage.NsPrefixDateTagToMetricIDs || indexType == storage.NsPrefixDateMetricNameToTSID {
				date := encoding.UnmarshalUint64(cur[9:])
				if date < uint64(retentionDate) {
					skipCount++
					idx++
					continue
				}
			}
			newItems = append(newItems, items[idx])
			idx++
		}
		return alldata, newItems
	}

	mergeset.MergePartsByPartDirs("", dirs,
		dstPartDir,
		removeStaleDateIndex,
		nil,
	)
	fmt.Printf("Merge Index OK\n")
	fmt.Printf("\tindex skip %d\n", skipCount)
}
