package mergeset

// utilities for operator tsdb index file directly

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type IndexIteratorFunc func(
	tableType int,
	partDir string,
	metaIndexRowNo int,
	blockHeaderNo int,
	itemNo int,
	data []byte) (isStop bool)

type SearchIndexIteratorFunc func(data []byte) (isStop bool)

func minValue(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Less(a, b []byte) bool {
	minLen := minValue(len(a), len(b))
	return string(a[:minLen]) < string(b[:minLen])
}

func ComparePrefix(a, b []byte) int {
	minLen := minValue(len(a), len(b))
	return bytes.Compare(a[:minLen], b[:minLen])
}

type IterateContext struct {
	PartHeader       partHeader
	MetaIndexBin     []byte
	MetaIndexRows    []metaindexRow
	InmemoryBlock    inmemoryBlock
	IndexBlockBuf    []byte
	DecompressedData []byte
	BlockHeaders     []blockHeader
	StorageBlock     storageBlock
}

func ReadFile(dst []byte, filePath string) ([]byte, error) {
	s, err := os.Stat(filePath)
	if err != nil {
		return dst, err
	}
	var f *os.File
	f, err = os.Open(filePath)
	if err != nil {
		return dst, err
	}
	if cap(dst) < int(s.Size()) {
		dst = make([]byte, s.Size(), s.Size()*2)
	} else {
		dst = dst[:s.Size()]
	}
	_, err = f.Read(dst)
	if err != nil {
		return dst, err
	}
	return dst, nil
}

func IterateAllIndexesFromPartDir(ctx *IterateContext, tableType int, partDir string, callback IndexIteratorFunc) (isStop bool, err error) {
	ctx.PartHeader.Reset()
	ctx.PartHeader.MustReadMetadata(partDir)
	ctx.MetaIndexBin, err = ReadFile(ctx.MetaIndexBin[:0], filepath.Join(partDir, metaindexFilename))
	if err != nil {
		err = fmt.Errorf("read metaindex.bin error, err=%s", err.Error())
		return
	}
	ctx.MetaIndexRows, err = unmarshalMetaindexRows(ctx.MetaIndexRows[:0], bytes.NewReader(ctx.MetaIndexBin)) // ctx.MetaIndexRows must be sorted
	if err != nil {
		err = fmt.Errorf("metaindex.bin unmarshalMetaindexRows error, err=%s", err.Error())
		return
	}
	indexFilePath := filepath.Join(partDir, indexFilename)
	indexFile := fs.MustOpenReaderAt(indexFilePath)
	defer indexFile.MustClose()
	itemsFile := fs.MustOpenReaderAt(filepath.Join(partDir, itemsFilename))
	defer itemsFile.MustClose()
	lensFile := fs.MustOpenReaderAt(filepath.Join(partDir, lensFilename))
	defer lensFile.MustClose()
	for i := range ctx.MetaIndexRows { // each index block
		row := &ctx.MetaIndexRows[i]
		if cap(ctx.IndexBlockBuf) < int(row.indexBlockSize) {
			ctx.IndexBlockBuf = make([]byte, int(row.indexBlockSize), int(row.indexBlockSize)*2)
		} else {
			ctx.IndexBlockBuf = ctx.IndexBlockBuf[:int(row.indexBlockSize)]
		}
		indexFile.MustReadAt(ctx.IndexBlockBuf, int64(row.indexBlockOffset))
		ctx.DecompressedData, err = encoding.DecompressZSTD(ctx.DecompressedData[:0], ctx.IndexBlockBuf)
		if err != nil {
			err = fmt.Errorf("index.bin DecompressZSTD error, err=%s", err.Error())
			return
		}
		if cap(ctx.BlockHeaders) < int(row.blockHeadersCount) {
			ctx.BlockHeaders = make([]blockHeader, int(row.blockHeadersCount), int(row.blockHeadersCount)*2)
		} else {
			ctx.BlockHeaders = ctx.BlockHeaders[:int(row.blockHeadersCount)]
		}
		for j := 0; j < int(row.blockHeadersCount); j++ {
			ctx.BlockHeaders[j].Reset()
			ctx.DecompressedData, err = ctx.BlockHeaders[j].UnmarshalNoCopy(ctx.DecompressedData)
			if err != nil {
				err = fmt.Errorf("index.bin UnmarshalNoCopy error, err=%s", err.Error())
				return
			}
			// ctx.BlockHeaders must be sorted
		}
		for j := range ctx.BlockHeaders { // each block
			bh := &ctx.BlockHeaders[j]
			ctx.StorageBlock.Reset()
			if fs.IsDisableMmap() {
				ctx.StorageBlock.itemsData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.itemsData, int(bh.itemsBlockSize))
				itemsFile.MustReadAt(ctx.StorageBlock.itemsData, int64(bh.itemsBlockOffset))
				ctx.StorageBlock.lensData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.lensData, int(bh.lensBlockSize))
				lensFile.MustReadAt(ctx.StorageBlock.lensData, int64(bh.lensBlockOffset))
			} else {
				ctx.StorageBlock.itemsData = itemsFile.ReadByOffset(int64(bh.itemsBlockOffset), int64(bh.itemsBlockSize))
				ctx.StorageBlock.lensData = lensFile.ReadByOffset(int64(bh.lensBlockOffset), int64(bh.lensBlockSize))
			}

			ctx.InmemoryBlock.Reset()
			if err = ctx.InmemoryBlock.UnmarshalData(&ctx.StorageBlock, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
				err = fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
				return
			}
			// todo: 这里缓存 ib 对象
			items := ctx.InmemoryBlock.items
			data := ctx.InmemoryBlock.data
			for k, idx := range items { // ib.items is sorted
				item := idx.Bytes(data)
				if callback(tableType, partDir, i, j, k, item) {
					isStop = true
					return
				}
			}
		} //end of each block
	} //end of each index block
	return
}

func SearchIndexesFromPartDir(ctx *IterateContext, partDir string, prefix []byte, callback SearchIndexIteratorFunc) (isStop bool, err error) {
	ctx.PartHeader.Reset()
	ctx.PartHeader.MustReadMetadata(partDir)
	if Less(prefix, ctx.PartHeader.firstItem) || Less(ctx.PartHeader.lastItem, prefix) {
		return
	}
	ctx.MetaIndexBin, err = ReadFile(ctx.MetaIndexBin[:0], filepath.Join(partDir, metaindexFilename))
	if err != nil {
		err = fmt.Errorf("read metaindex.bin error, err=%s", err.Error())
		return
	}
	ctx.MetaIndexRows, err = unmarshalMetaindexRows(ctx.MetaIndexRows[:0], bytes.NewReader(ctx.MetaIndexBin)) // ctx.MetaIndexRows must be sorted
	if err != nil {
		err = fmt.Errorf("metaindex.bin unmarshalMetaindexRows error, err=%s", err.Error())
		return
	}
	indexFilePath := filepath.Join(partDir, indexFilename)
	indexFile := fs.MustOpenReaderAt(indexFilePath)
	defer indexFile.MustClose()
	itemsFile := fs.MustOpenReaderAt(filepath.Join(partDir, itemsFilename))
	defer itemsFile.MustClose()
	lensFile := fs.MustOpenReaderAt(filepath.Join(partDir, lensFilename))
	defer lensFile.MustClose()
	tempPrefix := append(prefix, '\xff')
	//todo: use binary search on ctx.MetaIndexRows
	rows := ctx.MetaIndexRows[:]
	for i := range rows { // each index block
		row := &rows[i]
		if Less(prefix, row.firstItem) {
			continue
		}
		if len(row.firstItem) >= len(prefix) &&
			string(row.firstItem[:len(prefix)]) > string(prefix) {
			break
		}
		if cap(ctx.IndexBlockBuf) < int(row.indexBlockSize) {
			ctx.IndexBlockBuf = make([]byte, int(row.indexBlockSize), int(row.indexBlockSize)*2)
		} else {
			ctx.IndexBlockBuf = ctx.IndexBlockBuf[:int(row.indexBlockSize)]
		}
		indexFile.MustReadAt(ctx.IndexBlockBuf, int64(row.indexBlockOffset))
		ctx.DecompressedData, err = encoding.DecompressZSTD(ctx.DecompressedData[:0], ctx.IndexBlockBuf)
		if err != nil {
			err = fmt.Errorf("index.bin DecompressZSTD error, err=%s", err.Error())
			return
		}
		if cap(ctx.BlockHeaders) < int(row.blockHeadersCount) {
			ctx.BlockHeaders = make([]blockHeader, int(row.blockHeadersCount), int(row.blockHeadersCount)*2)
		} else {
			ctx.BlockHeaders = ctx.BlockHeaders[:int(row.blockHeadersCount)]
		}
		for j := 0; j < int(row.blockHeadersCount); j++ {
			ctx.BlockHeaders[j].Reset()
			ctx.DecompressedData, err = ctx.BlockHeaders[j].UnmarshalNoCopy(ctx.DecompressedData)
			if err != nil {
				err = fmt.Errorf("index.bin UnmarshalNoCopy error, err=%s", err.Error())
				return
			}
			// ctx.BlockHeaders must be sorted
		}
		bhs := ctx.BlockHeaders[:]
		for j := range bhs { // each block // todo: 这里做二分查找
			bh := &bhs[j]
			if Less(prefix, bh.firstItem) {
				continue
			}
			if len(bh.firstItem) >= len(prefix) &&
				string(bh.firstItem[:len(prefix)]) > string(prefix) {
				break
			}
			ctx.StorageBlock.Reset()
			if fs.IsDisableMmap() {
				ctx.StorageBlock.itemsData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.itemsData, int(bh.itemsBlockSize))
				itemsFile.MustReadAt(ctx.StorageBlock.itemsData, int64(bh.itemsBlockOffset))
				ctx.StorageBlock.lensData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.lensData, int(bh.lensBlockSize))
				lensFile.MustReadAt(ctx.StorageBlock.lensData, int64(bh.lensBlockOffset))
			} else {
				ctx.StorageBlock.itemsData = itemsFile.ReadByOffset(int64(bh.itemsBlockOffset), int64(bh.itemsBlockSize))
				ctx.StorageBlock.lensData = lensFile.ReadByOffset(int64(bh.lensBlockOffset), int64(bh.lensBlockSize))
			}

			ctx.InmemoryBlock.Reset()
			if err = ctx.InmemoryBlock.UnmarshalData(&ctx.StorageBlock, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
				err = fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
				return
			}
			// todo: 这里缓存 ib 对象
			items := ctx.InmemoryBlock.items
			data := ctx.InmemoryBlock.data
			if len(prefix) > 0 {
				start := sort.Search(len(items), func(i int) bool {
					if len(items[i].Bytes(data)) >= len(prefix) {
						return string(prefix) <= string(items[i].Bytes(data)[:len(prefix)])
					}
					return string(prefix) <= string(items[i].Bytes(data))
				})
				items = items[start:]
				end := sort.Search(len(items), func(i int) bool {
					return string(tempPrefix) < string(items[i].Bytes(data))
				})
				items = items[:end]
			}
			for _, idx := range items { // ib.items 是排序的
				item := idx.Bytes(data)
				if callback(item) {
					isStop = true
					return
				}
			}
		} //end of each block
	} //end of each index block
	return
}

func ReadMetaDataFromPartDir(partDir string) (itemsCount uint64, blocksCount uint64, firstItem []byte, lastItem []byte) {
	var ph partHeader
	ph.Reset()
	ph.MustReadMetadata(partDir)
	itemsCount = ph.itemsCount
	blocksCount = ph.blocksCount
	firstItem = ph.firstItem
	lastItem = ph.lastItem
	return
}

// type BlockHeaderSlice []blockHeader

// func (arr BlockHeaderSlice) Less(i, j int) bool {
// 	return string(arr[i].firstItem) < string(arr[j].firstItem)
// }

// func FilterRangeOfBlockerHeaders(prefix []byte, bhs []blockHeader)[]blockHeader{
// 	if len(prefix)==0{
// 		return bhs
// 	}
// 	// 下面进行二分查找
// 	start, end := 0, len(bhs)
// 	equalLoc := -1
// 	for start< end{
// 		mid := int(uint(start+end)>>1)
// 		cur := bhs[mid].firstItem
// 		minLen := minValue(len(prefix), len(cur))
// 		c := bytes.Compare(prefix[:minLen], cur[:minLen])
// 		switch c{
// 		case -1:
// 			start = mid + 1
// 		case 0:
// 			equalLoc = mid
// 			end = mid
// 		default:
// 			end = mid
// 		}
// 	}

// 	i, j := 0, n
// 	for i < j {
// 		h := int(uint(i+j) >> 1) // avoid overflow when computing h
// 		// i ≤ h < j
// 		if !f(h) {
// 			i = h + 1 // preserves f(i-1) == false
// 		} else {
// 			j = h // preserves f(j) == true
// 		}
// 	}
// 	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
// 	return i
// }

// type PartWriter struct {
// 	//IB      *inmemoryBlock
// 	//Indexes   map[byte][]*inmemoryBlock // 每种索引，对应 inmomery block 数组
// 	//ItemCount uint64
// 	PartHeader partHeader
// 	Writer     *blockStreamWriter
// }

// func NewPartWriter(partDir string) *PartWriter {
// 	fs.MustMkdirIfNotExist(partDir)
// 	inst := &PartWriter{}
// 	inst.PartHeader.Reset()
// 	inst.Writer = getBlockStreamWriter()
// 	return inst
// }

// 每次写一种类型的索引

// 写入索引
// func (p *PartWriter) WriteIndex(data []byte) error {
// 	arr, ok := p.Indexes[data[0]]
// 	if !ok {
// 		arr = make([]*inmemoryBlock, 0, 15)
// 	}
// 	if len(arr) == 0 {
// 		arr = append(arr, getInmemoryBlock())
// 	}
// 	cur := arr[len(arr)-1]
// 	if !cur.Add(data) {
// 		arr = append(arr, getInmemoryBlock()) // todo: 可以加多少块，取决于物理内存的剩余量
// 		cur = arr[len(arr)-1]
// 		if !cur.Add(data) {
// 			panic("impossible error")
// 		}
// 	}
// 	p.ItemCount++
// 	p.Indexes[data[0]] = arr
// 	return nil
// }

// func (p *PartWriter) WriteToFile(targetFile string) {
// 	writer := getBlockStreamWriter()
// 	compressLevel := getCompressLevel(p.ItemCount)
// 	writer.MustInitFromFilePart(targetFile, true, compressLevel)
// 	for indexType := range p.SortIndex() {
// 		ibs := p.Indexes[byte(indexType)]
// 		for _, ib := range ibs {
// 			ib.SortItems()
// 			writer.WriteUnsortedBlock(ib)
// 		}
// 	}
// 	writer.MustClose()
// 	// todo: 写 metaindex.json
// 	var ph partHeader
// 	ph.MustWriteMetadata(targetFile)
// }

// func (p *PartWriter) SortIndex() []int {
// 	arr := make([]int, 0, len(p.Indexes))
// 	for indexType := range p.Indexes {
// 		arr = append(arr, int(indexType))
// 	}
// 	sort.Ints(arr)
// 	return arr
// }

func Show(t *Table) {
	fmt.Printf("part file count:%d\n", len(t.fileParts))
	var blockCount uint64
	for _, p := range t.fileParts {
		fmt.Printf("part header:%+v\n", p.p.ph)
		fmt.Printf("index block count:%d\n", len(p.p.mrs))
		for _, row := range p.p.mrs {
			blockCount += uint64(row.blockHeadersCount)
		}
	}
	fmt.Printf("blockCount=%d\n", blockCount)
}

type PartHeaders []partHeader

func (arr PartHeaders) Len() int {
	return len(arr)
}

func (arr PartHeaders) Less(i, j int) bool {
	if string(arr[i].lastItem) < string(arr[j].firstItem) {
		return true
	}
	return string(arr[i].firstItem) < string(arr[j].firstItem)
}

func (arr PartHeaders) Swap(i, j int) {
	//arr[i], arr[j] = arr[j], arr[i]  //??? 不明白为什么这里浅拷贝出问题了
	//temp := arr[i]
	//arr[i].itemsCount = arr[j].itemsCount
	//arr[i].firstItem =
	a := &arr[i]
	b := &arr[j]
	a.itemsCount, b.itemsCount = b.itemsCount, a.itemsCount
	a.blocksCount, b.blocksCount = b.blocksCount, a.blocksCount
	a.firstItem, b.firstItem = swapSlice(b.firstItem, a.firstItem)
	//a.firstItem, b.firstItem = b.firstItem, a.firstItem
	//a.lastItem, b.lastItem = b.lastItem, a.lastItem
	a.lastItem, b.lastItem = swapSlice(b.lastItem, a.lastItem)
}

func swapSlice(a, b []byte) ([]byte, []byte) {
	return b, a
}

func GetPartHeaders(tableDir string, partNames []string) []partHeader {
	phs := make([]partHeader, len(partNames))
	for i, partName := range partNames {
		phs[i].Reset()
		partDir := filepath.Join(tableDir, partName)
		phs[i].MustReadMetadata(partDir)
	}
	sort.Sort(PartHeaders(phs))
	return phs
}

// func findMinFirstItem(phs []partHeader, firstItem []byte)int{
// 	minIdx := -1
// 	for i:=range phs{
// 		ph := &phs[i]
// 		if string(ph.firstItem)<string(firstItem){
// 			minIdx = i
// 			firstItem = ph.firstItem
// 		}
// 	}
// 	return minIdx
// }

func showPhs(phs []partHeader) {
	//minFirstItem := phs[0].firstItem
	start := 0
	curIdx := 1
	for ; curIdx < len(phs); curIdx++ {
		cur := &phs[curIdx]
		prev := &phs[curIdx-1]
		if string(cur.firstItem) < string(prev.lastItem) {
			// 检查是否包含
			if string(cur.firstItem) <= string(prev.firstItem) &&
				string(cur.lastItem) <= string(prev.firstItem) {
				fmt.Printf("\t%d is in %d\n", curIdx, curIdx-1)
			} else {
				fmt.Printf("\t%d is behind %d\n", curIdx, curIdx-1)
			}
		} else {
			fmt.Printf("section: from %d to %d\n", start, curIdx-1)
			start = curIdx
		}
	}
	fmt.Printf("section: from %d to %d\n\n", start, len(phs)-1)
}

func CheckTree(tableDir string, partNames []string) {
	if !fs.IsPathExist(tableDir) {
		return
	}
	phs := GetPartHeaders(tableDir, partNames)
	showPhs(phs)
	//prefix := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	for idx, partName := range partNames {
		ph := &phs[idx]
		if idx > 0 {
			prevPh := &phs[idx-1]
			if string(ph.firstItem) < string(prevPh.lastItem) {
				logger.Errorf("part header no sorted: idx=%d", idx)
			}
		}
		// var ph partHeader
		partDir := filepath.Join(tableDir, partName)
		// ph.MustReadMetadata(partDir)
		//
		var zstdData []byte
		var metaIndexBin []byte
		var err error
		metaIndexBin, err = ReadFile(nil, filepath.Join(partDir, metaindexFilename))
		if err != nil {
			logger.Panicf("read metaindex.bin fail")
		}
		// metaIndexBin, err = encoding.DecompressZSTD(nil, zstdData)
		// if err != nil {
		// 	logger.Panicf("metaindex.bin DecompressZSTD fail")
		// }
		var metaindexRows []metaindexRow
		metaindexRows, err = unmarshalMetaindexRows(nil, bytes.NewReader(metaIndexBin))
		if err != nil {
			logger.Panicf("metaindex.bin unmarshalMetaindexRows error, err=%s", err.Error())
		}
		indexFilePath := filepath.Join(partDir, indexFilename)
		var indexFileBin []byte
		indexFileBin, err = ReadFile(nil, indexFilePath)
		if err != nil {
			logger.Panicf("read index.bin fail")
		}
		var bhsData []byte
		//
		itemsFile := fs.MustOpenReaderAt(filepath.Join(partDir, itemsFilename))
		defer itemsFile.MustClose()
		lensFile := fs.MustOpenReaderAt(filepath.Join(partDir, lensFilename))
		defer lensFile.MustClose()
		//
		for _, mr := range metaindexRows {
			if string(mr.firstItem) < string(ph.firstItem) {
				logger.Panicf("mr.firstItem error")
			}
			if string(mr.firstItem) > string(ph.lastItem) {
				logger.Panicf("mr.firstItem error")
			}
			zstdData = indexFileBin[mr.indexBlockOffset : mr.indexBlockOffset+uint64(mr.indexBlockSize)]
			bhsData, err = encoding.DecompressZSTD(nil, zstdData)
			if err != nil {
				logger.Panicf("index.bin DecompressZSTD fail")
			}
			bhs := make([]blockHeader, int(mr.blockHeadersCount))
			for j := 0; j < int(mr.blockHeadersCount); j++ {
				bhs[j].Reset()
				bhsData, err = bhs[j].UnmarshalNoCopy(bhsData)
				if err != nil {
					logger.Panicf("index.bin UnmarshalNoCopy fail")
				}
				// ctx.BlockHeaders must be sorted
			}
			var sb storageBlock
			var ib inmemoryBlock
			var prevLastItem []byte
			for _, bh := range bhs {
				if string(bh.firstItem) < string(mr.firstItem) {
					logger.Panicf("bh.first item error")
				}
				if string(bh.firstItem) > string(ph.lastItem) {
					logger.Panicf("bh.first item error")
				}
				sb.Reset()
				sb.itemsData = itemsFile.ReadByOffset(int64(bh.itemsBlockOffset), int64(bh.itemsBlockSize))
				sb.lensData = lensFile.ReadByOffset(int64(bh.lensBlockOffset), int64(bh.lensBlockSize))
				ib.Reset()
				err = ib.UnmarshalData(&sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType)
				if err != nil {
					logger.Panicf("ib.UnmarshalData fail")
				}
				items := ib.items
				data := ib.data
				firstIem := items[0].Bytes(data)
				if string(firstIem) != string(bh.firstItem) {
					logger.Panicf("bh.firstItem error")
				}
				if !ib.isSorted() {
					logger.Panicf("not sorted")
				}
				if len(prevLastItem) > 0 {
					// 后一个块的第一条，不能小于前一个块的最后一条
					if string(bh.firstItem) < string(prevLastItem) {
						fmt.Printf("\t\t\tmr:%X\n", mr.firstItem)
						fmt.Printf("\t\t\tbh:%X\n", bh.firstItem)
						fmt.Printf("\t\t\t   %X\n", prevLastItem)
						logger.Panicf("string(prevLastItem) error")
					}
				}
				prevLastItem = append(prevLastItem[:0], items[len(items)-1].Bytes(data)...)
				// 检查索引 1
				// for itemIndex, item := range items {
				// 	if bytes.Equal(prefix, item.Bytes(data)[:len(prefix)]) {
				// 		fmt.Printf("\t\tindex: %d %d %d\n", mrIndex, bhIndex, itemIndex)
				// 		fmt.Printf("\t\t\t%X\n", mr.firstItem)
				// 		fmt.Printf("\t\t\t%X\n", bh.firstItem)
				// 		fmt.Printf("\t\t\t%X\n", prevLastItem)
				// 		break
				// 	}
				// }
			}
		}

	}
}

// 索引 1 的结构
type TagIndex struct {
	AccountID   uint32
	ProjectID   uint32
	MetricGroup []byte
	Key         []byte
	Value       []byte
	MetricIDs   []uint64
}

func (t *TagIndex) Reset() {
	t.AccountID = 0
	t.ProjectID = 0
	t.MetricGroup = t.MetricGroup[:0]
	t.Key = t.Key[:0]
	t.Value = t.Value[:0]
	t.MetricIDs = t.MetricIDs[:0]
}

func (t *TagIndex) UnmarshalFromTag(data []byte) (err error) {
	tagType := data[0]
	data = data[1:]
	switch tagType {
	case 1: // MetricGroup -> MetricID
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found end of metric group")
			return
		}
		t.MetricGroup = data[:idx]
		data = data[idx+1:]
	case 0xfe: //  MetricGroup + Tag -> MetricID
		var nsize int
		_, nsize = encoding.UnmarshalVarUint64(data)
		if nsize <= 0 {
			err = fmt.Errorf("not found metric group len, err=%w", err)
			return
		}
		t.MetricGroup = data[:nsize]
		data = data[nsize:]
		data, err = t.parseKV(data)
		if err != nil {
			return
		}
	default: // Tag -> MetricID
		data, err = t.parseKV(data)
		if err != nil {
			return
		}
	}
	if len(data) < 8 || len(data)%8 != 0 {
		err = fmt.Errorf("metric id format error")
		return
	}
	if len(data)/8 > 10000 {
		logger.Panicf("error")
	}
	for i := 0; i < len(data); i += 8 {
		t.MetricIDs = append(t.MetricIDs, encoding.UnmarshalUint64(data[i:i+8]))
	}
	return nil
}

func (t *TagIndex) Unmarshal(data []byte) (err error) {
	if len(data) < 20 {
		err = fmt.Errorf("length error")
		return
	}
	if data[0] != 1 {
		err = fmt.Errorf("index type error")
		return
	}
	t.AccountID = encoding.UnmarshalUint32(data[1:])
	t.ProjectID = encoding.UnmarshalUint32(data[5:])
	data = data[9:]
	return t.UnmarshalFromTag(data)
}

func (t *TagIndex) parseKV(data []byte) (left []byte, err error) {
	idx := bytes.IndexByte(data, 1)
	if idx < 0 {
		err = fmt.Errorf("not found end of metric tag")
		return
	}
	t.Key = data[:idx]
	data = data[idx+1:]
	idx = bytes.IndexByte(data, 1)
	if idx < 0 {
		err = fmt.Errorf("not found end of metric value")
		return
	}
	t.Value = data[:idx]
	left = data[idx+1:]
	return
}

// 提供在 index table 搜索的工具
func GetTableSearchFromDirs(dirs []string) *TableSearch {
	pws := make([]*partWrapper, 0, len(dirs))
	for _, dir := range dirs {
		p := mustOpenFilePart(dir)
		pw := &partWrapper{
			p:  p,
			mp: nil,
		}
		pw.incRef()
		pws = append(pws, pw)
	}

	ts := &TableSearch{}
	ts.reset()
	ts.tb = nil
	ts.needClosing = false

	ts.pws = pws

	// Initialize the psPool.
	if n := len(ts.pws) - cap(ts.psPool); n > 0 {
		ts.psPool = append(ts.psPool[:cap(ts.psPool)], make([]partSearch, n)...)
	}
	ts.psPool = ts.psPool[:len(ts.pws)]
	for i, pw := range ts.pws {
		ts.psPool[i].Init(pw.p)
	}
	return ts
}

func ReleaseTableSearch(ts *TableSearch) {
	for _, pw := range ts.pws {
		pw.p.MustClose()
		pw.p = nil
	}
	ts.pws = nil
	ts.psHeap = nil
	for _, ps := range ts.psPool {
		ps.reset()
	}
	ts.psPool = nil
}
