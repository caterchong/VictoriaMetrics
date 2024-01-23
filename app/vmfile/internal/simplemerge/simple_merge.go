package simplemerge

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/compare"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func SimpleMerge(fromPaths []string, toPath string, flag int) {
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
	// if !fs.IsPathExist(filepath.Dir(toPath)) {
	// 	logger.Errorf("to path's parent path %s not exists", filepath.Dir(toPath))
	// 	return
	// }
	if checkMetricIDDuplicate(fromPaths) {
		return
	}
	// 下面拷贝文件
	//fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.IndexdbDirname))
	if flag&2 != 0 {
		fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "big"))
		fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "small"))
	}
	ts := time.Now().UnixNano()
	targetPrevName := fmt.Sprintf("%016X", ts)
	targetPrevDir := filepath.Join(toPath, storage.IndexdbDirname, targetPrevName)
	ts++
	targetCurrName := fmt.Sprintf("%016X", ts)
	targetCurrDir := filepath.Join(toPath, storage.IndexdbDirname, targetCurrName)
	if flag&1 != 0 {
		fs.MustMkdirIfNotExist(targetPrevDir) // prev
		fs.MustMkdirIfNotExist(targetCurrDir) // next
	}
	// 找到分区名
	var err error
	for _, p := range fromPaths {
		indexDbDir := filepath.Join(p, storage.IndexdbDirname)
		// 拷贝索引
		if flag&1 != 0 {
			_, curr, prev := storage.GetIndexDBTableNames(indexDbDir)
			for _, item := range [][]string{
				{curr, targetCurrDir},
				{prev, targetPrevDir},
			} {
				srcTableDir := item[0]
				dstTableDir := item[1]
				if !fs.IsPathExist(srcTableDir) {
					continue
				}
				partNames := storage.ReadPartsNameOfIndexTable(srcTableDir)
				for _, partName := range partNames {
					copyDir := filepath.Join(srcTableDir, partName)
					ts++
					newPartName := fmt.Sprintf("%016X", ts)
					err = CopyDir(copyDir, filepath.Join(dstTableDir, newPartName))
					if err != nil {
						logger.Errorf("CopyDir error, path=%s, err=%w", copyDir, err)
						return
					}
				}
			}
		}
		// 拷贝数据
		if flag&2 != 0 {
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
						ts++
						newPartName := fmt.Sprintf("%016X", ts)
						CopyDir(partDir, filepath.Join(dstMonthlyDir, newPartName))
					}
				}
			}
			// 所有小分区的月份，大分区中必须存在
			for month := range monthMap {
				fs.MustMkdirIfNotExist(filepath.Join(toPath, storage.DataDirname, "big", month))
			}
		}
	}
}

func checkMetricIDDuplicate(fromPaths []string) bool {
	firstOne := fromPaths[0]
	allMetricIDs := compare.GetAllMetricIDOfCurrentTable(firstOne)
	for i := 1; i < len(fromPaths); i++ {
		curPath := fromPaths[i]
		metricIds := compare.GetAllMetricIDOfCurrentTable(curPath)
		if metricidset.HaveIntersection(metricIds, allMetricIDs) {
			logger.Errorf("metric id duplicate, can not merge: %s", curPath)
			return true
		}
		allMetricIDs.UnionMayOwn(metricIds)
	}
	return false
}

// CopyDir 递归拷贝目录
func CopyDir(src string, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	// 创建目标目录
	err = os.MkdirAll(dst, 0777)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		fileInfo, err := os.Stat(srcPath)
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			// 递归拷贝子目录
			err = CopyDir(srcPath, dstPath)
			if err != nil {
				return err
			}
		} else {
			// 拷贝文件
			err = CopyFile(srcPath, dstPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CopyFile(src, dst string) error {
	// 打开源文件
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// 创建目标文件
	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// 将源文件的内容复制到目标文件
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	// 确保写入目标文件
	return destFile.Sync()
}
