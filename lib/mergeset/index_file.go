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
