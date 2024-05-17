package mergedatav2

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

const (
	dataDirname = "data"
)

func MergeDataV2(fromPaths []string, toPath string) {
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
	//fs.MustMkdirIfNotExist(toPath)
	ts := time.Now().UnixNano()
	dstPartitionDir := filepath.Join(toPath, dataDirname, "big")
	fs.MustMkdirIfNotExist(dstPartitionDir)
	fs.MustMkdirIfNotExist(filepath.Join(toPath, dataDirname, "small"))
	mergeData(fromPaths, dstPartitionDir, &ts)
	fmt.Printf("OK\n")
}

func mergeData(fromPaths []string, dstPartitionDir string, ts *int64) {
	// 找到分区名
	monthMap := map[string][]string{} // 所有存在过的月份分区，都要记录下来
	for _, p := range fromPaths {
		// 拷贝数据
		dataDir := filepath.Join(p, dataDirname)
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
		dstPartDir := filepath.Join(dstPartitionDir, month, fmt.Sprintf("%016X", *ts))
		(*ts)++
		partDirs = partDirs[:0]
		for _, monthlyPartitionDir := range dirs {
			partNames := storage.GetPartNames(monthlyPartitionDir)
			for _, part := range partNames {
				partDir := filepath.Join(monthlyPartitionDir, part)
				partDirs = append(partDirs, partDir) // 以月份归类的源数据目录
			}
		}
		if err := storage.Merge(partDirs, dstPartDir, 0); err != nil {
			logger.Panicf("merge error, err=%w", err)
		}
	}
}
