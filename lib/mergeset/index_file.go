package mergeset

// utilities for operator tsdb index file directly

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

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

func IterateAllIndexesFromPartDir(ctx *IterateContext, partDir string, prefix []byte, callback func(data []byte) (isStop bool)) (isStop bool, err error) {
	ctx.PartHeader.Reset()
	ctx.PartHeader.MustReadMetadata(partDir)
	if len(prefix) > 0 && (Less(prefix, ctx.PartHeader.firstItem) || Less(ctx.PartHeader.lastItem, prefix)) {
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
	for i := range ctx.MetaIndexRows { // each index block
		row := &ctx.MetaIndexRows[i]
		if len(prefix) > 0 && Less(prefix, row.firstItem) {
			continue
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
		for j := range ctx.BlockHeaders { // each block // todo: 这里做二分查找
			bh := &ctx.BlockHeaders[j]
			if len(prefix) > 0 && Less(prefix, bh.firstItem) {
				continue
			}
			ctx.StorageBlock.Reset()
			ctx.StorageBlock.itemsData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.itemsData, int(bh.itemsBlockSize))
			itemsFile.MustReadAt(ctx.StorageBlock.itemsData, int64(bh.itemsBlockOffset))

			ctx.StorageBlock.lensData = bytesutil.ResizeNoCopyMayOverallocate(ctx.StorageBlock.lensData, int(bh.lensBlockSize))
			lensFile.MustReadAt(ctx.StorageBlock.lensData, int64(bh.lensBlockOffset))

			ctx.InmemoryBlock.Reset()
			if err = ctx.InmemoryBlock.UnmarshalData(&ctx.StorageBlock, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
				err = fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
				return
			}
			// todo: 这里缓存 ib 对象
			for _, idx := range ctx.InmemoryBlock.items { // ib.items 是排序的
				if callback(idx.Bytes(ctx.InmemoryBlock.data)) {
					isStop = true
					return
				}
			}
		} //end of each block
	} //end of each index block
	return
}

type BlockHeaderSlice []blockHeader

func (arr BlockHeaderSlice) Less(i, j int) bool {
	return string(arr[i].firstItem) < string(arr[j].firstItem)
}
