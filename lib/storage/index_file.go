package storage

// utilities for operator tsdb index file directly

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

const (
	IndexTableFlagOfCurr = 1
	IndexTableFlagOfPrev = 2
)

func IterateAllIndexesFromTableDir(ctx *mergeset.IterateContext, tableDir string, tableType int, callback mergeset.IndexIteratorFunc) (isStop bool, err error) {
	names := ReadPartsNameOfIndexTable(tableDir)
	for _, name := range names {
		partDir := filepath.Join(tableDir, name)
		isStop, err = mergeset.IterateAllIndexesFromPartDir(ctx, tableType, partDir, callback)
		if err != nil {
			err = fmt.Errorf("read part dir [%s] error, err=%s", partDir, err.Error())
			return
		}
		if isStop {
			return
		}
	}
	return
}

func IterateAllIndexes(storageDataPath string, indexTableFlag int8, callback mergeset.IndexIteratorFunc) (err error) {
	indexdbDir := filepath.Join(storageDataPath, IndexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		return fmt.Errorf("indexdb [%s] not exists", indexdbDir)
	}
	_, indexTableCurr, indexTablePrev := GetIndexDBTableNames(indexdbDir)
	var ctx mergeset.IterateContext
	if indexTableFlag&IndexTableFlagOfCurr != 0 {
		var isStop bool
		isStop, err = IterateAllIndexesFromTableDir(&ctx, indexTableCurr, IndexTableFlagOfCurr, callback)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	if indexTableFlag&IndexTableFlagOfPrev != 0 {
		_, err = IterateAllIndexesFromTableDir(&ctx, indexTablePrev, IndexTableFlagOfPrev, callback)
	}
	return
}

func SearchIndexes(storageDataPath string, prefix []byte, indexTableFlag int8, callback mergeset.SearchIndexIteratorFunc) error {
	if len(prefix) == 0 {
		return fmt.Errorf("prefix len must not 0")
	}
	indexdbDir := filepath.Join(storageDataPath, IndexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		return fmt.Errorf("indexdb [%s] not exists", indexdbDir)
	}
	_, indexTableCurr, indexTablePrev := GetIndexDBTableNames(indexdbDir)
	tables := []string{}
	if indexTableFlag&IndexTableFlagOfCurr != 0 {
		tables = append(tables, indexTableCurr)
	}
	if indexTableFlag&IndexTableFlagOfPrev != 0 {
		tables = append(tables, indexTablePrev)
	}
	var ctx mergeset.IterateContext
	for _, path := range tables {
		names := ReadPartsNameOfIndexTable(path)
		for _, name := range names {
			partDir := filepath.Join(path, name)
			isStop, err := mergeset.SearchIndexesFromPartDir(&ctx, partDir, prefix, callback)
			if err != nil {
				return fmt.Errorf("read part dir [%s] error, err=%s", partDir, err.Error())
			}
			if isStop {
				return nil
			}
		}
	}
	return nil
}

func ReadPartsNameOfIndexTable(srcDir string) []string {
	des := fs.MustReadDir(srcDir)
	var partNames []string
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories.
			continue
		}
		partName := de.Name()
		if isSpecialDir(partName) {
			// Skip special dirs.
			continue
		}
		partNames = append(partNames, partName)
	}
	sort.Strings(partNames)
	return partNames
}

func GetNewPartDirName(partNames []string) string {
	if len(partNames) == 0 {
		return fmt.Sprintf("%016X", uint64(time.Now().UnixNano()))
	}
	if !sort.StringsAreSorted(partNames) {
		sort.Strings(partNames)
	}
	n, err := strconv.ParseUint(partNames[len(partNames)-1], 16, 64)
	if err != nil {
		return fmt.Sprintf("%016X", uint64(time.Now().UnixNano()))
	}
	return fmt.Sprintf("%016X", n+1)
}

func GetTotalItemsCount(storageDataPath string, indexTableFlag int8) (total uint64, err error) {
	indexdbDir := filepath.Join(storageDataPath, IndexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		err = fmt.Errorf("indexdb [%s] not exists", indexdbDir)
		return
	}
	_, indexTableCurr, indexTablePrev := GetIndexDBTableNames(indexdbDir)
	if indexTableFlag&IndexTableFlagOfCurr != 0 {
		total, err = GetTotalItemsCountFromTableDir(indexTableCurr)
		if err != nil {
			return
		}
	}
	if indexTableFlag&IndexTableFlagOfPrev != 0 {
		var temp uint64
		temp, err = GetTotalItemsCountFromTableDir(indexTablePrev)
		if err != nil {
			return
		}
		total += temp
	}
	return
}

func GetTotalItemsCountFromTableDir(tableDir string) (total uint64, err error) {
	names := ReadPartsNameOfIndexTable(tableDir)
	for _, name := range names {
		partDir := filepath.Join(tableDir, name)
		totalItems, _, _, _ := mergeset.ReadMetaDataFromPartDir(partDir)
		total += totalItems
	}
	return
}

// // 把索引全部写成另一个 part 文件
// func WriteIndexToNewPart(targetDir *string, indexdbDir string, indexTableFlag int) error {
// 	if !fs.IsPathExist(indexdbDir) {
// 		return fmt.Errorf("indexdb [%s] not exists", indexdbDir)
// 	}
// 	_, indexPartionsCur, indexPartionsPrev := GetIndexDBTableNames(indexdbDir)
// 	partitions := []string{}
// 	if indexTableFlag&IndexPartitionFlagOfCur != 0 {
// 		partitions = append(partitions, indexPartionsCur)
// 	}
// 	if indexTableFlag&IndexPartitionFlagOfPrev != 0 {
// 		partitions = append(partitions, indexPartionsPrev)
// 	}
// 	for _, path := range partitions { // 这一层两个，不需要顺序
// 		names := readPartsName(path)
// 		for _, name := range names { // part 目录与时间相关，无序  // ??? 在 part 之间搜索难道不需要排序吗?
// 			partDir := filepath.Join(path, name)
// 			isStop, err := mergeset.ReadAllIndexesFromPartDir(partDir, nil, func(data []byte) (isStop bool) {
// 				//这里处理每条索引
// 				return
// 			})
// 			if err != nil {
// 				return fmt.Errorf("read part dir [%s] error, err=%s", partDir, err.Error())
// 			}
// 			if isStop {
// 				return nil
// 			}
// 		}
// 	}
// 	return nil
// }
