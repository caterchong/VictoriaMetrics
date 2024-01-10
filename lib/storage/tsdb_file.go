package storage

// utilities for operator tsdb file directly

import (
	"fmt"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

const (
	IndexTableFlagOfCurr = 1
	IndexTableFlagOfPrev = 2
)

func IterateAllIndexes(storageDataPath string, prefix []byte, indexTableFlag int8, callback func(data []byte) (isStop bool)) error {
	indexdbDir := filepath.Join(storageDataPath, indexdbDirname)
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
		names := readPartsNameOfIndexTable(path)
		for _, name := range names {
			partDir := filepath.Join(path, name)
			isStop, err := mergeset.IterateAllIndexesFromPartDir(&ctx, partDir, prefix, callback)
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

func readPartsNameOfIndexTable(srcDir string) []string {
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
	return partNames
}
