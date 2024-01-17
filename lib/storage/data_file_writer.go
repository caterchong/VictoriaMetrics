package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

type PartWriter struct {
	blockStreamWriter
	mrs                 []*metaindexRow
	bhs                 []*blockHeader
	blockHeaderTotalLen uint64
	ph                  partHeader
	rowsMerged          uint64
	partDir             string
}

func (w *PartWriter) Reset() {
	w.blockStreamWriter.reset()
	if cap(w.mrs) == 0 {
		w.mrs = make([]*metaindexRow, 0, 10)
	}
	mr := &metaindexRow{}
	mr.Reset()
	w.mrs = append(w.mrs[:0], mr) // current row
	w.bhs = w.bhs[:0]
	w.blockHeaderTotalLen = 0
	w.ph.Reset()
	w.rowsMerged = 0
}

const (
	ZstdCompressLevelOfHighest = 4
)

func (w *PartWriter) CreateNewPart(storageDataPath string, partitionName string,
	monthlyPartitionName string, partName string) {
	partDir := filepath.Join(storageDataPath, dataDirname, partitionName, monthlyPartitionName, partName)
	w.blockStreamWriter.MustInitFromFilePart(partDir, true, ZstdCompressLevelOfHighest)
	w.partDir = partDir
}

func (w *PartWriter) WriteBlock(b *Block) {
	atomic.AddUint64(&w.rowsMerged, uint64(b.rowsCount()))
	b.deduplicateSamplesDuringMerge()
	_, timestampsData, valuesData :=
		b.MarshalData(w.blockStreamWriter.timestampsBlockOffset, w.blockStreamWriter.valuesBlockOffset)
	usePrevTimestamps := len(w.blockStreamWriter.prevTimestampsData) > 0 &&
		bytes.Equal(timestampsData, w.blockStreamWriter.prevTimestampsData)
	if usePrevTimestamps {
		// The current timestamps block equals to the previous timestamps block.
		// Update headerData so it points to the previous timestamps block. This saves disk space.
		_, timestampsData, valuesData =
			b.MarshalData(w.blockStreamWriter.prevTimestampsBlockOffset, w.blockStreamWriter.valuesBlockOffset)
	}
	bh := &blockHeader{}
	w.bhs = append(w.bhs, bh)
	*bh = b.bh
	w.blockHeaderTotalLen += uint64(bh.MarshalLen())
	//
	mr := w.mrs[len(w.mrs)-1]
	mr.RegisterBlockHeader(bh) // 这里几乎赋值了所有 mr 的字段

	if w.blockHeaderTotalLen >= maxBlockSize { // blockHeader 序列化后的长度超过 64 kb 后，产生一个新的 metaIndexRow
		w.flushBlockHeaders()
	}
	if !usePrevTimestamps {
		w.blockStreamWriter.prevTimestampsData = append(w.blockStreamWriter.prevTimestampsData[:0], timestampsData...)
		w.blockStreamWriter.prevTimestampsBlockOffset = w.blockStreamWriter.timestampsBlockOffset
		fs.MustWriteData(w.blockStreamWriter.timestampsWriter, timestampsData)
		w.blockStreamWriter.timestampsBlockOffset += uint64(len(timestampsData))
	}
	fs.MustWriteData(w.blockStreamWriter.valuesWriter, valuesData)
	w.blockStreamWriter.valuesBlockOffset += uint64(len(valuesData))
	updatePartHeader(b, &w.ph)
}

func (w *PartWriter) flushBlockHeaders() { // 产生一个新的 metaindexRow
	if w.blockHeaderTotalLen == 0 {
		return
	}
	// 对 bhs 进行排序
	sort.Sort(BlockHeaders(w.bhs))
	w.blockStreamWriter.indexData = w.blockStreamWriter.indexData[:0]
	for _, bh := range w.bhs {
		w.blockStreamWriter.indexData = bh.Marshal(w.blockStreamWriter.indexData)
	}
	// Write compressed index block to index data.
	w.blockStreamWriter.compressedIndexData = encoding.CompressZSTDLevel(
		w.blockStreamWriter.compressedIndexData[:0], w.blockStreamWriter.indexData,
		w.blockStreamWriter.compressLevel)
	fs.MustWriteData(w.blockStreamWriter.indexWriter, w.blockStreamWriter.compressedIndexData)

	// Write metaindex row to metaindex data.
	mr := w.mrs[len(w.mrs)-1]
	mr.IndexBlockOffset = w.blockStreamWriter.indexBlockOffset
	mr.IndexBlockSize = uint32(len(w.blockStreamWriter.compressedIndexData))
	// Update offsets.
	w.blockStreamWriter.indexBlockOffset += uint64(mr.IndexBlockSize)
	w.blockStreamWriter.indexData = w.blockStreamWriter.indexData[:0]
	mr = &metaindexRow{}
	mr.Reset()
	w.mrs = append(w.mrs, mr)
}

func (w *PartWriter) Close() {
	w.flushBlockHeaders()
	w.mrs = w.mrs[:len(w.mrs)-1] // 最后一个没有使用
	if len(w.mrs) == 0 {
		logger.Panicf("impossible error")
	}
	sort.Sort(MetaIndexRows(w.mrs))
	w.blockStreamWriter.metaindexData = w.blockStreamWriter.metaindexData[:0]
	for _, mr := range w.mrs {
		w.blockStreamWriter.metaindexData = mr.Marshal(w.blockStreamWriter.metaindexData)
	}
	//
	// Write metaindex data.
	w.blockStreamWriter.compressedMetaindexData = encoding.CompressZSTDLevel(
		w.blockStreamWriter.compressedMetaindexData[:0],
		w.blockStreamWriter.metaindexData,
		w.blockStreamWriter.compressLevel)
	fs.MustWriteData(w.blockStreamWriter.metaindexWriter, w.blockStreamWriter.compressedMetaindexData)

	// Close writers.
	w.blockStreamWriter.timestampsWriter.MustClose()
	w.blockStreamWriter.valuesWriter.MustClose()
	w.blockStreamWriter.indexWriter.MustClose()
	w.blockStreamWriter.metaindexWriter.MustClose()

	w.blockStreamWriter.reset()
	// 写 metadata.json
	w.ph.MustWriteMetadata(w.partDir)
}

// func (w *PartWriter) WriteRaw(bh *blockHeader, timestampsFile *fs.ReaderAt, valuesFile *fs.ReaderAt) {
// 	tsBuf := timestampsFile.ReadByOffset(int64(bh.TimestampsBlockOffset), int64(bh.TimestampsBlockSize))
// 	fs.MustWriteData(w.blockStreamWriter.timestampsWriter, tsBuf)
// 	//
// 	valuesBuf := valuesFile.ReadByOffset(int64(bh.ValuesBlockOffset), int64(bh.ValuesBlockSize))
// 	fs.MustWriteData(w.blockStreamWriter.valuesWriter, valuesBuf)
// 	//
// 	w.blockStreamWriter.timestampsBlockOffset += uint64(bh.TimestampsBlockSize)
// 	w.blockStreamWriter.valuesBlockOffset += uint64(bh.ValuesBlockSize)
// }

func GetMonthlyPartitionNames(partitionsPath string) []string {
	out := make([]string, 0, 12)
	des := fs.MustReadDir(partitionsPath)
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories
			continue
		}
		ptName := de.Name()
		if ptName == snapshotsDirname {
			// Skip directory with snapshots
			continue
		}
		out = append(out, ptName)
	}
	sort.Strings(out)
	return out
}

func GetPartNames(monthlyPartitionDir string) []string {
	names := mustReadPartNamesFromDir(monthlyPartitionDir)
	sort.Strings(names)
	return names
}

func GetNewPartName(partNames []string) string {
	if len(partNames) == 0 {
		return fmt.Sprintf("%016X", time.Now().UnixNano())
	}
	lastOne := partNames[len(partNames)-1]
	n, err := strconv.ParseUint(lastOne, 16, 64)
	if err != nil {
		return fmt.Sprintf("%016X", time.Now().UnixNano())
	}
	return fmt.Sprintf("%016X", n+1)
}

// 统计 tsid 对应数据文件的块信息
type TsidDataInfo struct {
	PartitionName        string
	MonthlyPartitionName string
	PartName             string
	MetaIndexRowIndex    int
	BlockIndex           int
	BlockHeader          blockHeader
	PartInstance         *part
}

type TsidDataMap struct {
	DefaultTenant map[uint64][]*TsidDataInfo
	Tenants       map[uint32]map[uint32]map[uint64][]*TsidDataInfo
	//
	indexBinContent []byte
	zstdBuf         []byte
	bhs             []blockHeader
}

func NewTsidDataMap() *TsidDataMap {
	inst := &TsidDataMap{
		DefaultTenant: make(map[uint64][]*TsidDataInfo, 100000),
		Tenants:       make(map[uint32]map[uint32]map[uint64][]*TsidDataInfo),
	}
	inst.Reset()
	return inst
}

func (m *TsidDataMap) Reset() {
	m.indexBinContent = m.indexBinContent[:0]
	m.zstdBuf = m.zstdBuf[:0]
	m.bhs = m.bhs[:0]
}

func (m *TsidDataMap) ReadFromMonthlyPartitionDir(storagePath string, partitionName string, monthlyPartitionName string) (err error) {
	monthlyDir := filepath.Join(storagePath, dataDirname, partitionName, monthlyPartitionName)
	if !fs.IsPathExist(monthlyDir) {
		err = fmt.Errorf("dir [%s] not exists", monthlyDir)
		return
	}
	partNames := GetPartNames(monthlyDir)
	// 遍历每个 part
	//var zstdBuf []byte
	//var indexBinContent []byte
	for _, partName := range partNames {
		partDir := filepath.Join(monthlyDir, partName)
		partInst := mustOpenFilePart(partDir)
		m.indexBinContent, err = mergeset.ReadFile(m.indexBinContent[:0], filepath.Join(partDir, indexFilename))
		//indexBinContent, err = os.ReadFile(filepath.Join(partDir, indexFilename))
		if err != nil {
			partInst.MustClose()
			return
		}
		for i := range partInst.metaindex {
			row := &partInst.metaindex[i]
			indexBlock := m.indexBinContent[row.IndexBlockOffset : row.IndexBlockOffset+uint64(row.IndexBlockSize)]
			m.zstdBuf, err = encoding.DecompressZSTD(m.zstdBuf[:0], indexBlock)
			if err != nil {
				partInst.MustClose()
				return
			}
			bhs, err := unmarshalBlockHeaders(m.bhs[:0], m.zstdBuf, int(row.BlockHeadersCount))
			if err != nil {
				partInst.MustClose()
				return err
			}
			for j := range bhs {
				block := &bhs[j]
				// 建立索引
				tenant := m.DefaultTenant
				if block.TSID.AccountID != 0 || block.TSID.ProjectID != 0 {
					m1, ok := m.Tenants[block.TSID.AccountID]
					if !ok {
						m1 = make(map[uint32]map[uint64][]*TsidDataInfo)
						m.Tenants[block.TSID.AccountID] = m1
					}
					m2, ok := m1[block.TSID.ProjectID]
					if !ok {
						m2 = make(map[uint64][]*TsidDataInfo)
						m1[block.TSID.ProjectID] = m2
					}
					tenant = m2
				}
				metricInfo, ok := tenant[block.TSID.MetricID]
				if !ok {
					metricInfo = make([]*TsidDataInfo, 0, 10)

				}
				metricInfo = append(metricInfo, &TsidDataInfo{
					PartitionName:        partitionName,
					MonthlyPartitionName: monthlyPartitionName,
					PartName:             partName,
					MetaIndexRowIndex:    i,
					BlockIndex:           j,
					BlockHeader:          *block, // 值类型，可以直接复制
					PartInstance:         partInst,
				})
				tenant[block.TSID.MetricID] = metricInfo
			}
		}
	}
	return
}

func (m *TsidDataMap) MergeAll(writer *PartWriter) {
	for metricID, arr := range m.DefaultTenant {
		m.MergeTsid(0, 0, metricID, arr, writer)
	}
	for accountID, m1 := range m.Tenants {
		for projectID, m2 := range m1 {
			for metricID, arr := range m2 {
				m.MergeTsid(accountID, projectID, metricID, arr, writer)
			}
		}
	}
}

type TsidDataInfoArray []*TsidDataInfo

func (arr TsidDataInfoArray) Len() int {
	return len(arr)
}

func (arr TsidDataInfoArray) Less(i, j int) bool {
	a := &arr[i].BlockHeader
	b := &arr[j].BlockHeader
	if a.MaxTimestamp < b.MinTimestamp {
		return true
	}
	return a.MinTimestamp < b.MinTimestamp
}

func (arr TsidDataInfoArray) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func (m *TsidDataMap) MergeTsid(accountID, projectID uint32, metricID uint64, arr []*TsidDataInfo, writer *PartWriter) {
	sort.Sort(TsidDataInfoArray(arr))
	//todo: 检查 Scale / PrecisionBits
	//逐个遍历
	target := getBlock()
	defer putBlock(target)
	target.Init(&arr[0].BlockHeader.TSID, nil, nil, arr[0].BlockHeader.Scale, arr[0].BlockHeader.PrecisionBits)
	var rowsDeleted uint64
	i := 0
	for ; i < len(arr)/2; i += 2 {
		b1 := arr[i]
		b2 := arr[i+1]
		//
		a := LoadBlockFromFile(&b1.BlockHeader, b1.PartInstance.timestampsFile, b1.PartInstance.valuesFile)
		b := LoadBlockFromFile(&b2.BlockHeader, b2.PartInstance.timestampsFile, b2.PartInstance.valuesFile)
		var err error
		err = a.UnmarshalData()
		if err != nil {
			logger.Panicf("block UnmarshalData error, err=%w", err)
		}
		err = b.UnmarshalData()
		if err != nil {
			logger.Panicf("block UnmarshalData error, err=%w", err)
		}
		mergeBlocks(target, a, b, 0, &rowsDeleted)
		if target.tooBig() {
			// 开始写入
			writer.WriteBlock(target)
			target.Reset()
			target.Init(&arr[0].BlockHeader.TSID, nil, nil, arr[0].BlockHeader.Scale, arr[0].BlockHeader.PrecisionBits)
		}
		putBlock(a)
		putBlock(b)
	}
	if i >= len(arr) {
		return
	}
	// 处理奇数块
	for ; i < len(arr); i++ {
		b1 := arr[i]
		a := LoadBlockFromFile(&b1.BlockHeader, b1.PartInstance.timestampsFile, b1.PartInstance.valuesFile)
		var err error
		err = a.UnmarshalData()
		if err != nil {
			logger.Panicf("block UnmarshalData error, err=%w", err)
		}
		writer.WriteBlock(target)
		putBlock(a)
		//writer.WriteRaw(&arr[i].BlockHeader, arr[i].PartInstance.timestampsFile.(*fs.ReaderAt), arr[i].PartInstance.valuesFile.(*fs.ReaderAt))
	}
}

// func newPartForWrite(storagePath string, partitionName string, monthlyPartitionName string) (err error) {
// 	monthlyDir := filepath.Join(storagePath, dataDirname, partitionName, monthlyPartitionName)
// 	if !fs.IsPathExist(monthlyDir) {
// 		err = fmt.Errorf("dir [%s] not exists", monthlyDir)
// 		return
// 	}
// 	partNames := GetPartNames(monthlyDir)
// 	newPartName := GetNewPartName(partNames)
// 	partDir := filepath.Join(monthlyDir, newPartName)
// 	writer := getBlockStreamWriter()
// 	writer.reset()
// 	writer.MustInitFromFilePart(partDir, true, 4)
// 	// 遍历每个 part
// 	var zstdBuf []byte
// 	for _, partName := range partNames {
// 		partDir := filepath.Join(monthlyDir, partName)
// 		partInst := mustOpenFilePart(partDir)
// 		var indexBinContent []byte
// 		indexBinContent, err = os.ReadFile(filepath.Join(partDir, indexFilename))
// 		if err != nil {
// 			return
// 		}
// 		for i := range partInst.metaindex {
// 			row := &partInst.metaindex[i]
// 			indexBlock := indexBinContent[row.IndexBlockOffset : row.IndexBlockOffset+uint64(row.IndexBlockSize)]
// 			zstdBuf, err = encoding.DecompressZSTD(zstdBuf[:0], indexBlock)
// 			if err != nil {
// 				return
// 			}
// 			arr, err := unmarshalBlockHeaders(nil, zstdBuf, int(row.BlockHeadersCount))
// 			if err != nil {
// 				return err
// 			}
// 			for i := range arr {
// 				//block := &arr[i]
// 				// 建立索引
// 				//callback(block.TSID.AccountID, block.TSID.ProjectID, block.TSID.MetricID, block.RowsCount)
// 			}
// 		}
// 	}
// 	// 下面开始读取多个 part , 然后写文件
// 	//writer.WriteExternalBlock()
// 	return
// }
