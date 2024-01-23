package mergeset

import (
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type partSearch struct {
	// Item contains the last item found after the call to NextItem.
	//
	// The Item content is valid until the next call to NextItem.
	Item []byte

	// p is a part to search.
	p *part

	// The remaining metaindex rows to scan, obtained from p.mrs.
	mrs []metaindexRow

	// The remaining block headers to scan in the current metaindexRow.
	bhs []blockHeader

	// err contains the last error.
	err error

	indexBuf           []byte
	compressedIndexBuf []byte

	sb storageBlock

	ib        *inmemoryBlock
	ibItemIdx int
	//
	loadBlockCount uint64
}

func (ps *partSearch) reset() {
	ps.Item = nil
	ps.p = nil
	ps.mrs = nil
	ps.bhs = nil
	ps.err = nil

	ps.indexBuf = ps.indexBuf[:0]
	ps.compressedIndexBuf = ps.compressedIndexBuf[:0]

	ps.sb.Reset()

	ps.ib = nil
	ps.ibItemIdx = 0
	//
	ps.loadBlockCount = 0
}

// Init initializes ps for search in the p.
//
// Use Seek for search in p.
func (ps *partSearch) Init(p *part) {
	ps.reset()

	ps.p = p
}

func checkSorted(mrs []metaindexRow) {
	prev := mrs[0]
	for i := 1; i < len(mrs); i++ {
		if string(prev.firstItem) > string(mrs[i].firstItem) {
			logger.Panicf("not sorted")
		}
		prev = mrs[i]
	}
	fmt.Printf("\n\n")
	for idx, mr := range mrs {
		fmt.Printf("%d %X\n", idx, string(mr.firstItem))
	}
	fmt.Printf("\n\n")
}

func checkBlockHeaderSorted(bhs []blockHeader) {
	prev := bhs[0]
	for i := 1; i < len(bhs); i++ {
		cur := bhs[i]
		if string(prev.firstItem) > string(cur.firstItem) {
			logger.Panicf("not sorted")
		}
		prev = cur
	}
}

// Seek seeks for the first item greater or equal to k in ps.
func (ps *partSearch) Seek(k []byte) { // part 里面搜索的逻辑
	if err := ps.Error(); err != nil {
		// Do nothing on unrecoverable error.
		return
	}
	ps.err = nil

	if string(k) > string(ps.p.ph.lastItem) {
		// Not matching items in the part.
		ps.err = io.EOF
		return
	}

	if ps.tryFastSeek(k) { // 在 ib 里面搜索。第一次搜索肯定没有 ib
		return
	}

	ps.Item = nil
	ps.mrs = ps.p.mrs // 头信息赋值进去
	ps.bhs = nil      // 一开始还没有 block 数组的信息

	ps.indexBuf = ps.indexBuf[:0]
	ps.compressedIndexBuf = ps.compressedIndexBuf[:0]

	ps.sb.Reset()

	ps.ib = nil
	ps.ibItemIdx = 0

	if string(k) <= string(ps.p.ph.firstItem) { // 说明从第一个块开始就有所需要的数据
		// The first item in the first block matches.
		ps.err = ps.nextBlock()
		return
	}

	// Locate the first metaindexRow to scan.
	if len(ps.mrs) == 0 {
		logger.Panicf("BUG: part without metaindex rows passed to partSearch")
	}
	//sort.IsSorted(MetaIndexRows(ps.mrs))
	checkSorted(ps.mrs)
	n := sort.Search(len(ps.mrs), func(i int) bool {
		return string(k) <= string(ps.mrs[i].firstItem)
	})
	if n > 0 {
		// The given k may be located in the previous metaindexRow, so go to it.
		n--
	}
	logger.Infof("mrs, from %d to %d", len(ps.mrs), len(ps.mrs)-n)
	ps.mrs = ps.mrs[n:] // 二分查找，找到所需要的块

	// Read block headers for the found metaindexRow.
	if err := ps.nextBHS(); err != nil {
		ps.err = err
		return
	}

	// Locate the first block to scan.
	checkBlockHeaderSorted(ps.bhs)
	n = sort.Search(len(ps.bhs), func(i int) bool {
		return string(k) <= string(ps.bhs[i].firstItem)
	})
	if n > 0 {
		// The given k may be located in the previous block, so go to it.
		n--
	}
	logger.Infof("bhs, from %d to %d", len(ps.bhs), len(ps.bhs)-n)
	ps.bhs = ps.bhs[n:] // 二分查找，找到需要的块

	// Read the block.
	if err := ps.nextBlock(); err != nil {
		ps.err = err
		return
	}

	// Locate the first item to scan in the block.
	items := ps.ib.items
	data := ps.ib.data
	cpLen := commonPrefixLen(ps.ib.commonPrefix, k)       // 在 block 的 commonPrefix 和要搜索的 key 之前，找公共长度  //??? 不理解为什么
	ps.ibItemIdx = binarySearchKey(data, items, k, cpLen) // 通过二分查找，找到了位置
	if ps.ibItemIdx < len(items) {
		// The item has been found.
		return
	}

	// Nothing found in the current block. Proceed to the next block.
	// The item to search must be the first in the next block.
	if err := ps.nextBlock(); err != nil {
		ps.err = err
		return
	}
}

func (ps *partSearch) tryFastSeek(k []byte) bool { // part 内的快速搜索  // ??? 怎么个快速法
	if ps.ib == nil { // 如果有 inmomery block, 就在 ib 里面搜索
		return false
	}
	items := ps.ib.items
	idx := ps.ibItemIdx
	if idx >= len(items) {
		// The ib is exhausted.
		return false
	}
	cpLen := commonPrefixLen(ps.ib.commonPrefix, k)
	suffix := k[cpLen:]
	it := items[len(items)-1]
	it.Start += uint32(cpLen)
	data := ps.ib.data
	if string(suffix) > it.String(data) {
		// The item is located in next blocks.
		return false
	}

	// The item is located either in the current block or in previous blocks.
	if idx > 0 {
		idx--
	}
	it = items[idx]
	it.Start += uint32(cpLen)
	if string(suffix) < it.String(data) {
		items = items[:idx]
		if len(items) == 0 {
			return false
		}
		it = items[0]
		it.Start += uint32(cpLen)
		if string(suffix) < it.String(data) {
			// The item is located in previous blocks.
			return false
		}
		idx = 0
	}

	// The item is located in the current block
	ps.ibItemIdx = idx + binarySearchKey(data, items[idx:], k, cpLen)
	return true
}

// NextItem advances to the next Item.
//
// Returns true on success.
func (ps *partSearch) NextItem() bool {
	if ps.err != nil {
		return false
	}

	items := ps.ib.items
	if ps.ibItemIdx < len(items) {
		// Fast path - the current block contains more items.
		// Proceed to the next item.
		ps.Item = items[ps.ibItemIdx].Bytes(ps.ib.data)
		ps.ibItemIdx++
		return true
	}

	// The current block is over. Proceed to the next block.
	if err := ps.nextBlock(); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("error in %q: %w", ps.p.path, err)
		}
		ps.err = err
		return false
	}

	// Invariant: len(ps.ib.items) > 0 after nextBlock.
	ps.Item = ps.ib.items[0].Bytes(ps.ib.data)
	ps.ibItemIdx++
	return true
}

// Error returns the last error occurred in the ps.
func (ps *partSearch) Error() error {
	if ps.err == io.EOF {
		return nil
	}
	return ps.err
}

//var loadCount uint64

func (ps *partSearch) nextBlock() error { // 在当前 part 遍历数据
	if len(ps.bhs) == 0 {
		// The current metaindexRow is over. Proceed to the next metaindexRow.
		if err := ps.nextBHS(); err != nil { // 在还没有 block 数组的情况下，先装载 block 数组
			return err
		}
	}
	bh := &ps.bhs[0] // 消费一个 blockHeader
	ps.bhs = ps.bhs[1:]
	ib, err := ps.getInmemoryBlock(bh) // 根据 blockHeader，把整个 block 加载到内存
	ps.loadBlockCount++
	if err != nil { // 在上一层做了二分查找，这里把当前 blockHeader 对应的块加载到内存
		return err
	}
	ps.ib = ib       // 这下有了内存对象
	ps.ibItemIdx = 0 // ib 内的索引
	return nil
}

func (ps *partSearch) nextBHS() error { // 装载 block 数组
	if len(ps.mrs) == 0 {
		return io.EOF
	}
	mr := &ps.mrs[0] // 消费一个 metaindexRow
	ps.mrs = ps.mrs[1:]
	idxbKey := blockcache.Key{
		Part:   ps.p,
		Offset: mr.indexBlockOffset,
	}
	b := idxbCache.GetBlock(idxbKey)
	if b == nil { // 一开始也没有 cache
		idxb, err := ps.readIndexBlock(mr) // 从一个 metaindexRow 读出 block 数组
		if err != nil {
			return fmt.Errorf("cannot read index block: %w", err)
		}
		b = idxb
		idxbCache.PutBlock(idxbKey, b)
	}
	idxb := b.(*indexBlock)
	ps.bhs = idxb.bhs // 这下得到了 block 数组
	return nil
}

func (ps *partSearch) readIndexBlock(mr *metaindexRow) (*indexBlock, error) { // 从一个 metaindexRow 读出 block 数组
	ps.compressedIndexBuf = bytesutil.ResizeNoCopyMayOverallocate(ps.compressedIndexBuf, int(mr.indexBlockSize))
	ps.p.indexFile.MustReadAt(ps.compressedIndexBuf, int64(mr.indexBlockOffset))

	var err error
	ps.indexBuf, err = encoding.DecompressZSTD(ps.indexBuf[:0], ps.compressedIndexBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress index block: %w", err)
	}
	idxb := &indexBlock{
		buf: append([]byte{}, ps.indexBuf...), //??? 不懂为什么要这么拷贝
	}
	idxb.bhs, err = unmarshalBlockHeadersNoCopy(idxb.bhs[:0], idxb.buf, int(mr.blockHeadersCount))
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal block headers from index block (offset=%d, size=%d): %w", mr.indexBlockOffset, mr.indexBlockSize, err)
	}
	return idxb, nil
}

func (ps *partSearch) getInmemoryBlock(bh *blockHeader) (*inmemoryBlock, error) { // 根据 blockHeader，把一个块加载到内存
	ibKey := blockcache.Key{
		Part:   ps.p,
		Offset: bh.itemsBlockOffset,
	}
	b := ibCache.GetBlock(ibKey)
	if b == nil {
		ib, err := ps.readInmemoryBlock(bh) // 从磁盘读取一个块
		if err != nil {
			return nil, err
		}
		b = ib
		ibCache.PutBlock(ibKey, b)
	}
	ib := b.(*inmemoryBlock)
	return ib, nil
}

func (ps *partSearch) readInmemoryBlock(bh *blockHeader) (*inmemoryBlock, error) { // 从磁盘读取一个块
	ps.sb.Reset()

	ps.sb.itemsData = bytesutil.ResizeNoCopyMayOverallocate(ps.sb.itemsData, int(bh.itemsBlockSize))
	ps.p.itemsFile.MustReadAt(ps.sb.itemsData, int64(bh.itemsBlockOffset))

	ps.sb.lensData = bytesutil.ResizeNoCopyMayOverallocate(ps.sb.lensData, int(bh.lensBlockSize))
	ps.p.lensFile.MustReadAt(ps.sb.lensData, int64(bh.lensBlockOffset))

	ib := getInmemoryBlock()
	if err := ib.UnmarshalData(&ps.sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
		return nil, fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
	}

	return ib, nil
}

func binarySearchKey(data []byte, items []Item, k []byte, cpLen int) int { // 在 ib 的 items 数组中做二分查找
	if len(items) == 0 {
		return 0
	}
	suffix := k[cpLen:] // 找到公共前缀后，可以少比较一些字符。 只要比较后缀就行了。  !!! 精彩
	it := items[0]
	it.Start += uint32(cpLen) // it.Bytes(data)[cpLen:]  // 换个写法也行的
	if string(suffix) <= it.String(data) {
		// Fast path - the item is the first.
		return 0
	}
	items = items[1:]
	offset := uint(1)

	// This has been copy-pasted from https://golang.org/src/sort/search.go
	n := uint(len(items))
	i, j := uint(0), n
	for i < j {
		h := uint(i+j) >> 1
		it := items[h]
		it.Start += uint32(cpLen)
		if h >= 0 && h < uint(len(items)) && string(suffix) > it.String(data) {
			i = h + 1
		} else {
			j = h
		}
	}
	return int(i + offset) //??? 到底找的是开始位置还是结束位置?
}
