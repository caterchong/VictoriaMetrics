package reindex

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func Reindex(storageDataPath string, indexTableFlag int8, outputDir string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	//fs.MustMkdirIfNotExist(filepath.Dir())
	// if !fs.IsPathExist(outputDir) {
	// 	logger.Panicf("output dir [%s] not exists", outputDir)
	// }
	if indexTableFlag < 1 || indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", indexTableFlag)
	}
	//
	totalItems, err := storage.GetTotalItemsCount(storageDataPath, indexTableFlag)
	if err != nil {
		logger.Panicf("storage.GetTotalItemsCount error, err=%w", err)
	}
	//
	if len(outputDir) == 0 {
		if indexTableFlag == 3 {
			// 当不指定输出目录的时候，使用当前数据目录
			indexdbDir := filepath.Join(storageDataPath, storage.IndexdbDirname)
			if !fs.IsPathExist(indexdbDir) {
				logger.Panicf("indexdb [%s] not exists", indexdbDir)
			}
			_, indexTableCurr, indexTablePrev := storage.GetIndexDBTableNames(indexdbDir)
			partNames := storage.ReadPartsNameOfIndexTable(indexTableCurr)
			newPartName := storage.GetNewPartDirName(partNames)
			outputDir = filepath.Join(indexTableCurr, newPartName+".must-remove.")
			//
			defer func() {
				MustRemoveAllExcept(indexTablePrev, nil)
				MustRemoveAllExcept(indexTableCurr, []string{newPartName + ".must-remove."})
				os.Rename(outputDir, filepath.Join(indexTableCurr, newPartName))
			}()
			//
		} else {
			logger.Panicf("must set output dir when choose index table")
		}
	}

	//
	writer := mergeset.NewMultiIndexWriter(outputDir, totalItems)
	defer writer.Close()
	//
	if indexTableFlag == 3 {
		curTableMetricID := GetAllMetricIDOfCurrentTable(storageDataPath) // 得到 current 分区的所有 id
		callback := func(
			tableType int,
			partDir string,
			metaIndexRowNo int,
			blockHeaderNo int,
			itemNo int,
			data []byte) (isStop bool) {
			indexType := data[0]
			//
			switch indexType {
			case storage.NsPrefixMetricIDToTSID, storage.NsPrefixMetricIDToMetricName:
				accountID := encoding.UnmarshalUint32(data[1:])
				projectID := encoding.UnmarshalUint32(data[5:])
				metricID := encoding.UnmarshalUint64(data[9:])
				if tableType == storage.IndexTableFlagOfPrev && curTableMetricID.Has(accountID, projectID, metricID) {
					return false
				}
				// todo: 除了上面的两种索引，其他索引都有重复的可能，需要做一个去重的功能
				//case storage.NsPrefixTagToMetricIDs: // tag -> metric id
				//	showTagIndex(data, tableType, curTableMetricID)
			}
			writer.Write(data)
			return false
		}
		err = storage.IterateAllIndexes(storageDataPath, indexTableFlag, callback)
	} else {
		callback := func(
			tableType int,
			partDir string,
			metaIndexRowNo int,
			blockHeaderNo int,
			itemNo int,
			data []byte) (isStop bool) {
			writer.Write(data)
			return false
		}
		err = storage.IterateAllIndexes(storageDataPath, indexTableFlag, callback)
	}
	if err != nil {
		logger.Panicf("storage.IterateAllIndexes error, err=%w", err)
	}
	fmt.Printf("ok\n")
}

func showTagIndex(data []byte, tableType int, curTableMetricID *metricidset.MetricIDSet) {
	t := TagIndex{}
	err := ParseTagIndex(data, &t)
	if err != nil {
		fmt.Printf("\n\n%X\n\n", data)
		logger.Panicf("tag index format error, err=%w", err)
	}
	if tableType != storage.IndexTableFlagOfPrev {
		return
	}
	// todo: 判断所有的 metrics id， 把重复的 metrics id 进行删除
	if len(t.MetricGroupName) > 0 {
		if len(t.Key) > 0 {
			fmt.Printf("\t\t%s{%s=\"%s\"} %d(%d)\n", string(t.MetricGroupName), string(t.Key), string(t.Value), t.MetricIDs[0], len(t.MetricIDs))
		} else {
			fmt.Printf("\t\t%s{} %d(%d)\n", string(t.MetricGroupName), t.MetricIDs[0], len(t.MetricIDs))
		}
	} else {
		fmt.Printf("\t\t{%s=\"%s\"} %d(%d)\n", string(t.Key), string(t.Value), t.MetricIDs[0], len(t.MetricIDs))
	}
}

func MustRemoveAllExcept(dir string, names []string) {
	s, err := os.Stat(dir)
	if err != nil {
		logger.Panicf("stat [%s] error, err=%w", dir, err)
	}
	if !s.IsDir() {
		logger.Panicf("path [%s] not dir", dir)
	}
	des := fs.MustReadDir(dir)
	nameMap := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameMap[n] = struct{}{}
	}
	for _, de := range des {
		if _, has := nameMap[de.Name()]; has {
			continue
		}
		if de.IsDir() {
			os.RemoveAll(filepath.Join(dir, de.Name()))
		} else {
			os.Remove(filepath.Join(dir, de.Name()))
		}
	}
}

func getDifferenceSetOfMetricID(storageDataPath string) *metricidset.MetricIDSet {
	notInCurr := metricidset.NewMetricIDSet()
	//
	metricIDSet := GetAllMetricIDOfCurrentTable(storageDataPath)
	var prefix = [1]byte{storage.NsPrefixMetricIDToTSID}
	callback := func(data []byte) (isStop bool) {
		if data[0] != storage.NsPrefixMetricIDToTSID {
			logger.Panicf("data is not 2")
		}
		curAccountID := encoding.UnmarshalUint32(data[1:])
		curProjectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		if !metricIDSet.Has(curAccountID, curProjectID, metricID) {
			notInCurr.Add(curAccountID, curProjectID, metricID)
		}
		return
	}
	err := storage.SearchIndexes(storageDataPath, prefix[:], storage.IndexTableFlagOfPrev, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	return notInCurr
}

func GetAllMetricIDOfCurrentTable(storageDataPath string) *metricidset.MetricIDSet {
	metricIDSet := metricidset.NewMetricIDSet()
	var prefix = [1]byte{storage.NsPrefixMetricIDToTSID}
	callback := func(data []byte) (isStop bool) {
		if data[0] != storage.NsPrefixMetricIDToTSID {
			logger.Panicf("data is not 2")
		}
		curAccountID := encoding.UnmarshalUint32(data[1:])
		curProjectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		metricIDSet.Add(curAccountID, curProjectID, metricID)
		return
	}
	err := storage.SearchIndexes(storageDataPath, prefix[:], storage.IndexTableFlagOfCurr, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	return metricIDSet
}

type TagIndex struct {
	AccountID       uint32
	ProjectID       uint32
	MetricIDs       []uint64
	MetricGroupName []byte
	Key             []byte
	Value           []byte
}

func ParseTagIndex(data []byte, out *TagIndex) (err error) {
	out.AccountID = encoding.UnmarshalUint32(data[1:])
	out.ProjectID = encoding.UnmarshalUint32(data[5:])
	data = data[9:]
	switch data[0] {
	case 1:
		// metricGroupName -> metric id
		data = data[1:]
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(metricGroupName -> metric id)")
			return
		}
		out.MetricGroupName = data[:idx]
		data = data[idx+1:]
	case 0xfe:
		// metricGroupName + tag -> metric id
		data = data[1:]
		var nameLen uint64
		data, nameLen, err = encoding.UnmarshalVarUint64(data)
		if err != nil {
			err = fmt.Errorf("name len error(index 3/fe), err=%w", err)
			return
		}
		out.MetricGroupName = data[:nameLen]
		data = data[nameLen:]
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(index 3/fe)")
			return
		}
		out.Key = data[:idx]
		data = data[idx+1:]
		idx = bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag value(index 3/fe)")
			return
		}
		out.Value = data[:idx]
		data = data[idx+1:]
	default:
		// tag -> metric id
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(index 3/)")
			return
		}
		out.Key = data[:idx]
		data = data[idx+1:]
		idx = bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag value(index 3/)")
			return
		}
		out.Value = data[:idx]
		data = data[idx+1:]
	}
	if len(data) < 8 || len(data)%8 != 0 {
		err = fmt.Errorf("metric id format error(metricGroupName -> metric id)")
		return
	}
	for len(data) > 0 {
		out.MetricIDs = append(out.MetricIDs, encoding.UnmarshalUint64(data))
		data = data[8:]
	}
	return
}
