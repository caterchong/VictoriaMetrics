package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

// func GetMonthPartitions(dataDir string) {
// 	ds := fs.MustReadDir(dataDir)
// 	for _, item := range ds {
// 		if !item.IsDir() {
// 			continue
// 		}
// 	}
// }

func GetPartHeader(partDir string) (RowsCount uint64, BlocksCount uint64, MinTimestamp int64, MaxTimestamp int64) {
	var ph partHeader
	ph.MustReadMetadata(partDir)
	RowsCount = ph.RowsCount
	BlocksCount = ph.BlocksCount
	MinTimestamp = ph.MinTimestamp
	MaxTimestamp = ph.MaxTimestamp
	return
}

func EachMetaIndexRow(partitionType int8, partDir string, callback func(AccountID uint32, ProjectID uint32, MetricID uint64, BlockHeadersCount uint32, IndexBlockOffset uint64, IndexBlockSize uint32, indexBinData []byte)) error {
	content, err := mergeset.ReadFile(nil, filepath.Join(partDir, metaindexFilename))
	if err != nil {
		return err
	}
	rows, err := unmarshalMetaindexRows(nil, bytes.NewReader(content))
	if err != nil {
		return err
	}
	indexBin, err2 := os.ReadFile(filepath.Join(partDir, "index.bin"))
	if err2 != nil {
		logger.Panicf("open index.bin error")
	}
	for i := range rows {
		row := &rows[i]
		indexBinData := indexBin[row.IndexBlockOffset : row.IndexBlockOffset+uint64(row.IndexBlockSize)]
		var blocksBuf []byte
		blocksBuf, err = encoding.DecompressZSTD(blocksBuf[:0], indexBinData)
		if err != nil {
			return err
		}
		callback(row.TSID.AccountID, row.TSID.ProjectID, row.TSID.MetricID, row.BlockHeadersCount, row.IndexBlockOffset, row.IndexBlockSize, blocksBuf) // todo: 这里的 tsid 只是开始的 tsid
	}
	return nil
}

func EachPart(partitionType int8, monthlyPartitionDir string, callback func(partitionType int8, path string) (isStop bool, err error)) error {
	partNames := mustReadPartNamesFromDir(monthlyPartitionDir)
	sort.Strings(partNames)
	for _, partName := range partNames {
		partDir := filepath.Join(monthlyPartitionDir, partName)
		isStop, err := callback(partitionType, partDir)
		if err != nil {
			return err
		}
		if isStop {
			return nil
		}
	}
	return nil
}

func EachMonthlyPartition(partitionType int8, partitionDir string, callback func(partitionType int8, path string) (isStop bool, err error)) error {
	ptNames := make(map[string]bool)
	mustPopulatePartitionNames(partitionDir, ptNames)
	for _, monthName := range sortMonthlyPartitionNames(ptNames) {
		monthDir := filepath.Join(partitionDir, monthName)
		isStop, err := callback(partitionType, monthDir)
		if err != nil {
			return err
		}
		if isStop {
			return nil
		}
	}
	return nil
}

func sortMonthlyPartitionNames(m map[string]bool) []string {
	arr := make([]string, 0, len(m))
	for n := range m {
		arr = append(arr, n)
	}
	sort.Strings(arr)
	return arr
}

const (
	DataPartitionTypeSmall = 1
	DataPartitionTypeBig   = 2
)

func EachPartition(storagePath string, dataPartitionFlag int8, callback func(partitionType int8, path string) (isStop bool, err error)) (err error) {
	dataPath := filepath.Join(storagePath, dataDirname)
	if !fs.IsPathExist(dataPath) {
		err = fmt.Errorf("data path [%s] not exists", dataPath)
		return
	}
	var isStop bool
	if dataPartitionFlag&DataPartitionTypeSmall != 0 {
		smallPartitionsPath := filepath.Join(dataPath, smallDirname)
		if fs.IsPathExist(smallPartitionsPath) {
			isStop, err = callback(DataPartitionTypeSmall, smallPartitionsPath)
			if err != nil {
				return
			}
			if isStop {
				return
			}
		}
	}
	if dataPartitionFlag&DataPartitionTypeBig != 0 {
		bigPartitionsPath := filepath.Join(dataPath, bigDirname)
		if fs.IsPathExist(bigPartitionsPath) {
			_, err = callback(DataPartitionTypeBig, bigPartitionsPath)
			if err != nil {
				return
			}
		}
	}
	return
}

// func GetPartsInfo(storagePath string) (err error) {
// 	dataPath := filepath.Join(storagePath, dataDirname)
// 	if !fs.IsPathExist(dataPath) {
// 		err = fmt.Errorf("data path [%s] not exists", dataPath)
// 		return
// 	}
// 	smallPartitionsPath := filepath.Join(dataPath, smallDirname)
// 	if fs.IsPathExist(smallPartitionsPath) {
// 		ptNames := make(map[string]bool)
// 		mustPopulatePartitionNames(smallPartitionsPath, ptNames)
// 		for monthName := range ptNames {
// 			monthDir := filepath.Join(smallPartitionsPath, monthName)
// 			partNames := mustReadPartNamesFromDir(monthDir)
// 		}
// 	}
// 	bigPartitionsPath := filepath.Join(dataPath, bigDirname)
// 	if fs.IsPathExist(bigPartitionsPath) {
// 		ptNames := make(map[string]bool)
// 		mustPopulatePartitionNames(bigPartitionsPath, ptNames)
// 		//mustReadPartNamesFromDir
// 	}
// 	return
// }

func GetBlocksData(src []byte, blockHeadersCount int, callback func(AccountID uint32, ProjectID uint32, MetricID uint64, RowsCount uint32)) error {
	arr, err := unmarshalBlockHeaders(nil, src, blockHeadersCount)
	if err != nil {
		return err
	}
	for i := range arr {
		block := &arr[i]
		callback(block.TSID.AccountID, block.TSID.ProjectID, block.TSID.MetricID, block.RowsCount)
	}
	return nil
}

type AllDataIteratorCallBackFunc func(tsid *TSID, rowCount uint32) (isStop bool, err error)

func IterateAllDatas(storagePath string, dataPartitionFlag int8, callback AllDataIteratorCallBackFunc) (err error) {
	dataPath := filepath.Join(storagePath, dataDirname)
	if !fs.IsPathExist(dataPath) {
		err = fmt.Errorf("data path [%s] not exists", dataPath)
		return
	}
	var isStop bool
	smallPartitionsPath := filepath.Join(dataPath, smallDirname)
	bigPartitionsPath := filepath.Join(dataPath, bigDirname)
	if dataPartitionFlag&DataPartitionTypeSmall != 0 && fs.IsPathExist(smallPartitionsPath) {
		isStop, err = IterateAllDatasFromPartition(smallPartitionsPath, DataPartitionTypeSmall, callback)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	if dataPartitionFlag&DataPartitionTypeBig != 0 && fs.IsPathExist(bigPartitionsPath) {
		_, err = IterateAllDatasFromPartition(bigPartitionsPath, DataPartitionTypeBig, callback)
		if err != nil {
			return
		}
	}
	return
}

func IterateAllDatasFromPartition(partitionDir string, dataPartitionFlag int8, callback AllDataIteratorCallBackFunc) (isStop bool, err error) {
	ptNames := make(map[string]bool)
	mustPopulatePartitionNames(partitionDir, ptNames)
	for _, monthName := range sortMonthlyPartitionNames(ptNames) {
		monthDir := filepath.Join(partitionDir, monthName)
		isStop, err = IterateAllDatasFromMonthlyPartition(monthDir, dataPartitionFlag, callback)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	return
}

func IterateAllDatasFromMonthlyPartition(monthlyPartitionDir string, dataPartitionFlag int8, callback AllDataIteratorCallBackFunc) (isStop bool, err error) {
	partNames := mustReadPartNamesFromDir(monthlyPartitionDir)
	sort.Strings(partNames)
	for _, partName := range partNames {
		partDir := filepath.Join(monthlyPartitionDir, partName)
		isStop, err = IterateAllDatasFromPart(partDir, dataPartitionFlag, callback)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	return
}

func IterateAllDatasFromPart(partDir string, dataPartitionFlag int8, callback AllDataIteratorCallBackFunc) (isStop bool, err error) {
	var content []byte
	content, err = mergeset.ReadFile(nil, filepath.Join(partDir, metaindexFilename))
	if err != nil {
		return
	}
	var rows []metaindexRow
	rows, err = unmarshalMetaindexRows(nil, bytes.NewReader(content))
	if err != nil {
		return
	}
	var indexBin []byte
	indexBin, err = os.ReadFile(filepath.Join(partDir, "index.bin"))
	if err != nil {
		return
	}
	for i := range rows {
		row := &rows[i]
		indexBinData := indexBin[row.IndexBlockOffset : row.IndexBlockOffset+uint64(row.IndexBlockSize)]
		var blocksBuf []byte
		blocksBuf, err = encoding.DecompressZSTD(nil, indexBinData)
		if err != nil {
			return
		}
		var blocks []blockHeader
		blocks, err = unmarshalBlockHeaders(nil, blocksBuf, int(row.BlockHeadersCount))
		if err != nil {
			logger.Errorf("part dir:%s", partDir)
			logger.Errorf("row:%+v", row)
			err = fmt.Errorf("unmarshalBlockHeaders error, err=%w", err)
			return
		}
		isStop, err = IterateAllDatasFromIndexBlock(partDir, dataPartitionFlag, row, blocks, callback)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	return
}

func IterateAllDatasFromIndexBlock(partDir string, dataPartitionFlag int8,
	meta *metaindexRow,
	blocks []blockHeader,
	callback AllDataIteratorCallBackFunc) (isStop bool, err error) {
	for i := range blocks {
		block := &blocks[i]
		isStop, err = callback(&block.TSID, block.RowsCount)
		if err != nil {
			return
		}
		if isStop {
			return
		}
	}
	return
}
