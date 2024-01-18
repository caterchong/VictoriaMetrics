package rebuilddata

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func RebuildData(storageDataPath string, outputDir string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	//metricIDs := make(map[uint64]struct{}, 100000)
	newPartDirNames := make([]string, 0, 12)
	//newPartDirNames := make(map[string][]string)
	partitionNames := []string{"big", "small"}
	for _, partitionName := range partitionNames {
		partitionDir := filepath.Join(storageDataPath, "data", partitionName)
		if !fs.IsPathExist(partitionDir) {
			fmt.Printf("partition %s not exists\n", partitionName)
			continue
		}
		monthlyPartitionNames := storage.GetMonthlyPartitionNames(partitionDir)
		for _, month := range monthlyPartitionNames {
			m := storage.NewTsidDataMap()
			m.ReadFromMonthlyPartitionDir(storageDataPath, partitionName, month)
			// {
			// 	for metricID := range m.DefaultTenant {
			// 		metricIDs[metricID] = struct{}{}
			// 	}
			// }
			//

			//partNames := storage.GetPartNames(filepath.Join(partitionDir, month))
			//newPartName := storage.GetNewPartDirName(partNames) + storage.TempName
			newPartName := fmt.Sprintf("%016X", uint64(time.Now().UnixNano())) + storage.TempName
			//monthlyDir := filepath.Join(storageDataPath, storage.DataDirname, partitionName, month)
			//newPartDirNames[monthlyDir] = append(newPartDirNames[monthlyDir], newPartName)
			newPartDirNames = append(newPartDirNames, newPartName)
			//newPartDirNames = append(newPartDirNames, filepath.Join(storageDataPath, storage.DataDirname, partitionName, month, newPartName))
			writer := &storage.PartWriter{}
			writer.Reset()
			writer.CreateNewPart(outputDir, "big", month, newPartName)
			//
			m.MergeAll(writer)
			writer.Close()
			m.Close()
		}
	}
	// 删除所有文件夹，除了最近用于合并的文件夹
	for _, partitionName := range partitionNames {
		partitionDir := filepath.Join(storageDataPath, "data", partitionName)
		if !fs.IsPathExist(partitionDir) {
			continue
		}
		monthlyPartitionNames := storage.GetMonthlyPartitionNames(partitionDir)
		for _, month := range monthlyPartitionNames {
			monthlyPartitionDir := filepath.Join(partitionDir, month)
			reindex.MustRemoveAllExcept(monthlyPartitionDir, newPartDirNames)
		}
	}
	partitionDir := filepath.Join(storageDataPath, "data", "big")
	monthlyPartitionNames := storage.GetMonthlyPartitionNames(partitionDir)
	for _, month := range monthlyPartitionNames {
		monthlyPartitionDir := filepath.Join(partitionDir, month)
		reindex.MustRemoveAllExcept(monthlyPartitionDir, newPartDirNames)
		for _, partName := range newPartDirNames {
			if !strings.HasSuffix(partName, storage.TempName) {
				logger.Panicf("part name error:%s", partName)
			}
			srcDir := filepath.Join(monthlyPartitionDir, partName)
			if fs.IsPathExist(srcDir) {
				os.Rename(srcDir, filepath.Join(monthlyPartitionDir, partName[:len(partName)-len(storage.TempName)]))
			}
		}
	}
	//fmt.Printf("metricIDs:%d\n", len(metricIDs))
	fmt.Printf("ok\n")
}
