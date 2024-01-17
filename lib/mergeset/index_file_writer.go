package mergeset

import (
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// utilities for operator tsdb index file directly

type PartWriter struct {
	blockStreamWriter
	mrs                        []*metaindexRow // todo: 浅拷贝导致了这里的问题
	bhs                        []*blockHeader
	unpackedIndexBlockBufLen   uint64
	unpackedMetaIndexRowBufLen uint64
	ph                         partHeader
	partDir                    string
}

func NewPartWriterFromPartDir(dir string) (*PartWriter, error) {
	pw := &PartWriter{}
	pw.Reset()
	pw.partDir = dir
	pw.MustInitFromFilePart(dir, true, 100)
	return pw, nil
}

func (w *PartWriter) Reset() {
	w.blockStreamWriter.reset()
	w.ph.Reset()
	if cap(w.mrs) == 0 {
		w.mrs = make([]*metaindexRow, 0, 20)
	}
	mr := &metaindexRow{}
	mr.Reset()
	w.mrs = append(w.mrs, mr)
	//w.mrs[0].Reset()
	//w.mrs = w.mrs[:1]
	w.bhs = w.bhs[:0]
	w.unpackedIndexBlockBufLen = 0
	w.unpackedMetaIndexRowBufLen = 0
}

func (w *PartWriter) MustClose() {
	if len(w.mrs) == 0 {
		w.blockStreamWriter.MustClose()
		return
	}
	//todo
	w.flushUnsortedIndexData()
	w.flushUnsortedMetaIndexRowData()
	//
	w.metaindexWriter.MustClose()
	w.indexWriter.MustClose()
	w.itemsWriter.MustClose()
	w.lensWriter.MustClose()
	w.ph.MustWriteMetadata(w.partDir)
}

// MustInitFromFilePart

func (w *PartWriter) WriteUnsortedBlock(ib *inmemoryBlock) {
	ib.SortItems()
	dedup := ib.DeDuplicate()
	if dedup > 0 {
		logger.Infof("dedup count=%d", dedup)
	}
	if ib.CheckDuplicate() {
		logger.Panicf("index dup")
	}
	// todo: 为了解决索引可能重复的问题，需要在这里做一个去重的功能
	lastItem := ib.items[len(ib.items)-1].Bytes(ib.data)
	if len(w.ph.lastItem) == 0 {
		w.ph.lastItem = append(w.ph.lastItem[:0], lastItem...)
	} else {
		if string(lastItem) > string(w.ph.lastItem) {
			w.ph.lastItem = append(w.ph.lastItem[:0], lastItem...)
		}
	}
	//
	// w.compressLevel = getCompressLevel(uint64(ib.Len()))
	bh := &blockHeader{} // 每调用一次就是增加一个 block
	w.bhs = append(w.bhs, bh)
	bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType =
		ib.MarshalSortedData(&w.sb, bh.firstItem[:0], bh.commonPrefix[:0], w.compressLevel)
	mr := w.mrs[len(w.mrs)-1]
	// Write itemsData
	fs.MustWriteData(w.itemsWriter, w.sb.itemsData)
	bh.itemsBlockSize = uint32(len(w.sb.itemsData))
	bh.itemsBlockOffset = w.itemsBlockOffset
	w.itemsBlockOffset += uint64(bh.itemsBlockSize)

	// Write lensData
	fs.MustWriteData(w.lensWriter, w.sb.lensData)
	bh.lensBlockSize = uint32(len(w.sb.lensData))
	bh.lensBlockOffset = w.lensBlockOffset
	w.lensBlockOffset += uint64(bh.lensBlockSize)

	// Write blockHeader
	w.unpackedIndexBlockBufLen += bh.MarshalLen()
	{
		n := bh.MarshalLen()
		var buf [1024]byte
		n1 := len(bh.Marshal(buf[:0]))
		if int(n) != n1 {
			logger.Panicf("MarshalLen=%d, marshel=%d", n, n1)
		}
	}

	mr.blockHeadersCount++
	w.ph.blocksCount++
	w.ph.itemsCount += uint64(bh.itemsCount)
	if w.unpackedIndexBlockBufLen >= maxIndexBlockSize {
		w.flushUnsortedIndexData()
	}
}

func (w *PartWriter) flushUnsortedIndexData() {
	if w.unpackedIndexBlockBufLen == 0 {
		// Nothing to flush.
		logger.Infof("w.unpackedIndexBlockBufLen == 0")
		return
	}
	//logger.Infof("w.unpackedIndexBlockBufLen = %d", w.unpackedIndexBlockBufLen)

	// sort block headers
	sort.Sort(BlockHeaders(w.bhs))
	//logger.Infof("w.bhs len=%d", len(w.bhs))
	w.unpackedIndexBlockBuf = w.unpackedIndexBlockBuf[:0]
	var mrLen uint64
	for idx, bh := range w.bhs {
		if len(bh.firstItem) == 0 {
			logger.Panicf("bh.firstItem empty")
		}
		if bh.itemsBlockSize == 0 || bh.lensBlockSize == 0 {
			logger.Panicf("bh.itemsBlockSize==0 || bh.lensBlockSize==0")
		}
		w.unpackedIndexBlockBuf = bh.Marshal(w.unpackedIndexBlockBuf)
		mrLen += bh.MarshalLen()
		if uint64(len(w.unpackedIndexBlockBuf)) != mrLen {
			logger.Panicf("uint64(len(w.unpackedIndexBlockBuf))!=mrLen, index=%d", idx)
		}
		//logger.Infof("w.unpackedIndexBlockBuf len = %d, added len=%d", len(w.unpackedIndexBlockBuf), w.unpackedIndexBlockBufLen)
	}
	//logger.Infof("w.unpackedIndexBlockBuf len = %d, added len=%d", len(w.unpackedIndexBlockBuf), w.unpackedIndexBlockBufLen)
	if uint64(len(w.unpackedIndexBlockBuf)) != w.unpackedIndexBlockBufLen {
		logger.Panicf("uint64(len(w.unpackedIndexBlockBuf))!=w.unpackedIndexBlockBufLen")
	}
	w.unpackedIndexBlockBufLen = 0
	mr := w.mrs[len(w.mrs)-1] // current metaIndexRow
	mr.firstItem = append(mr.firstItem[:0], w.bhs[0].firstItem...)
	mr.blockHeadersCount = uint32(len(w.bhs))
	// Write indexBlock.  // 需要把多个 block 写入 blockIndex
	w.packedIndexBlockBuf = encoding.CompressZSTDLevel(w.packedIndexBlockBuf[:0], w.unpackedIndexBlockBuf, w.compressLevel)
	fs.MustWriteData(w.indexWriter, w.packedIndexBlockBuf)
	mr.indexBlockSize = uint32(len(w.packedIndexBlockBuf))
	// w.indexBlockOffset 是一个全局的偏移量
	mr.indexBlockOffset = w.indexBlockOffset
	{
		n := mr.MarshalLen()
		var buf [1024]byte
		n1 := len(mr.Marshal(buf[:0]))
		if int(n) != n1 {
			logger.Panicf("MarshalLen=%d, marshel=%d", n, n1)
		}
	}
	w.unpackedMetaIndexRowBufLen += mr.MarshalLen()
	//
	w.indexBlockOffset += uint64(mr.indexBlockSize)
	w.unpackedIndexBlockBuf = w.unpackedIndexBlockBuf[:0]
	w.bhs = w.bhs[:0]
	// todo: 什么时候才写入 w.mrs 呢?
	// if w.unpackedMetaIndexRowBufLen >= maxIndexBlockSize {
	// 	//w.flushUnsortedIndexData()
	// 	// todo: 写入整个 meta index row
	// 	//w.unpackedMetaIndexRowBufLen = 0
	// 	w.flushUnsortedMetaIndexRowData()
	// }
	// Write metaindexRow.
	//bsw.unpackedMetaindexBuf = bsw.mr.Marshal(bsw.unpackedMetaindexBuf)
	//bsw.mr.Reset()
	//logger.Infof("w.mrs len=%d", len(w.mrs))
	{
		for i := range w.mrs {
			mr := w.mrs[i]
			if len(mr.firstItem) == 0 {
				logger.Panicf("mr.firstItem empty, index=%d", i)
			}
			if mr.blockHeadersCount == 0 {
				logger.Panicf("mr.blockHeadersCount == 0")
			}
			if mr.indexBlockSize == 0 {
				logger.Panicf("mr.indexBlockSize==0")
			}
			if mr.indexBlockSize > 2*maxIndexBlockSize {
				logger.Panicf("mr.indexBlockSize > 2*maxIndexBlockSize, size=%d, index=%d", mr.indexBlockSize, i)
			}
		}
	}
	if cap(w.mrs)-len(w.mrs) == 0 {
		mrs := make([]*metaindexRow, 0, cap(w.mrs)*2)
		mrs = append(mrs, w.mrs...)
		w.mrs = mrs
		logger.Infof("w.mrs len=%d, !!!!!!!!!! copy array", len(w.mrs))
	}
	newMr := &metaindexRow{}
	newMr.Reset()
	w.mrs = append(w.mrs, newMr)
	//w.mrs = w.mrs[:len(w.mrs)+1]
	//w.mrs[len(w.mrs)-1].Reset()
	//logger.Infof("w.mrs len=%d", len(w.mrs))
}

func (w *PartWriter) flushUnsortedMetaIndexRowData() {
	if w.unpackedMetaIndexRowBufLen == 0 {
		logger.Infof("w.unpackedMetaIndexRowBufLen == 0")
		return
	}
	w.unpackedMetaIndexRowBufLen = 0
	last := w.mrs[len(w.mrs)-1]
	if last.blockHeadersCount == 0 {
		w.mrs = w.mrs[:len(w.mrs)-1]
		//logger.Infof("w.mrs len=%d", len(w.mrs))
	}
	{
		for i := range w.mrs {
			mr := w.mrs[i]
			if len(mr.firstItem) == 0 {
				logger.Panicf("mr.firstItem empty, index=%d", i)
			}
			if mr.blockHeadersCount == 0 {
				logger.Panicf("mr.blockHeadersCount == 0")
			}
			if mr.indexBlockSize == 0 {
				logger.Panicf("mr.indexBlockSize==0")
			}
			if mr.indexBlockSize > 2*maxIndexBlockSize {
				logger.Panicf("mr.indexBlockSize > 2*maxIndexBlockSize, size=%d, index=%d", mr.indexBlockSize, i)
			}
		}
	}
	sort.Sort(MetaIndexRows(w.mrs))
	tmp := w.mrs
	ok := sort.SliceIsSorted(tmp, func(i, j int) bool {
		return string(tmp[i].firstItem) < string(tmp[j].firstItem)
	})
	if !ok {
		logger.Panicf("not sorted")
	}
	w.ph.firstItem = append(w.ph.firstItem[:0], w.mrs[0].firstItem...)
	w.unpackedMetaindexBuf = w.unpackedMetaindexBuf[:0]
	for i := range w.mrs {
		mr := w.mrs[i]
		w.unpackedMetaindexBuf = mr.Marshal(w.unpackedMetaindexBuf)
	}
	// ok := sort.SliceIsSorted(tmp, func(i, j int) bool {
	// 	return string(tmp[i].firstItem) < string(tmp[j].firstItem)
	// })
	// Compress and write metaindex.
	w.packedMetaindexBuf = encoding.CompressZSTDLevel(w.packedMetaindexBuf[:0],
		w.unpackedMetaindexBuf, w.compressLevel)
	fs.MustWriteData(w.metaindexWriter, w.packedMetaindexBuf)
	//after those, part must close
}

type MetaIndexRows []*metaindexRow

func (arr MetaIndexRows) Len() int {
	return len(arr)
}

func (arr MetaIndexRows) Less(i, j int) bool {
	return string(arr[i].firstItem) < string(arr[j].firstItem)
}

func (arr MetaIndexRows) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

type BlockHeaders []*blockHeader

func (arr BlockHeaders) Len() int {
	return len(arr)
}

func (arr BlockHeaders) Less(i, j int) bool {
	return string(arr[i].firstItem) < string(arr[j].firstItem)
}

func (arr BlockHeaders) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

//type InmemoryBlock inmemoryBlock

type MultiIndexWriter struct {
	Indexes map[byte]*inmemoryBlock
	Writer  *PartWriter
}

func NewMultiIndexWriter(dir string, totalItemsCount uint64) *MultiIndexWriter {
	inst := &MultiIndexWriter{
		Indexes: make(map[byte]*inmemoryBlock, 10),
	}
	inst.Writer, _ = NewPartWriterFromPartDir(dir)
	inst.Writer.blockStreamWriter.compressLevel = getCompressLevel(totalItemsCount)
	inst.Writer.blockStreamWriter.compressLevel = 4 // 直接设置最大压缩率
	return inst
}

func (mw *MultiIndexWriter) Write(data []byte) {
	indexType := data[0]
	ib, ok := mw.Indexes[indexType]
	if !ok {
		ib = getInmemoryBlock()
		mw.Indexes[indexType] = ib
	}
	if ib.Add(data) {
		return
	}
	mw.Writer.WriteUnsortedBlock(ib)
	ib.Reset()
	if !ib.Add(data) {
		logger.Panicf("impossible error")
	}
}

func (mw *MultiIndexWriter) Close() {
	for _, ib := range mw.Indexes {
		if ib == nil || ib.Len() == 0 {
			continue
		}
		mw.Writer.WriteUnsortedBlock(ib)
		ib.Reset()
		putInmemoryBlock(ib)
	}
	mw.Writer.MustClose()
}
