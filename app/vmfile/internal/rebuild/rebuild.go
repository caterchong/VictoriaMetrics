package rebuild

import (
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/rebuilddata"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// const (
// 	tempDirName = ".must-remove."
// )

func Rebuild(storageDataPath string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	// 删除 part.json
	removePartJsonFile(storageDataPath)
	//
	reindex.Reindex(storageDataPath, storage.IndexTableFlagOfCurr|storage.IndexTableFlagOfPrev, "")
	rebuilddata.RebuildData(storageDataPath, storageDataPath)
}

// 删除 part.json
func removePartJsonFile(storageDataPath string) {
	dirs := []string{
		filepath.Join(storageDataPath, storage.IndexdbDirname),
		filepath.Join(storageDataPath, storage.DataDirname, "small"),
		filepath.Join(storageDataPath, storage.DataDirname, "big"),
	}
	for _, d := range dirs {
		if !fs.IsPathExist(d) {
			continue
		}
		for _, dir := range fs.MustReadDir(d) {
			if !dir.IsDir() {
				continue
			}
			os.Remove(filepath.Join(d, dir.Name(), storage.PartsFilename))
		}
	}
}
