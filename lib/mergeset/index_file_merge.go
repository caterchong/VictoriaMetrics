package mergeset

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type IndexChecker func(data []byte) (isSkip bool)

func MergeParts(tableDir string, partNames []string, dstPartPath string,
	prepareBlock PrepareBlockCallback,
	indexChecker IndexChecker) error {
	bsrs := make([]*blockStreamReader, 0, len(partNames))
	for _, partName := range partNames {
		partDir := filepath.Join(tableDir, partName)
		bsr := getBlockStreamReader()
		bsr.MustInitFromFilePart(partDir)
		bsrs = append(bsrs, bsr)
	}
	compressLevel := 4
	bsw := getBlockStreamWriter()
	bsw.MustInitFromFilePart(dstPartPath, true, compressLevel)
	err := mergePartsInternal(dstPartPath, bsw, bsrs, prepareBlock, indexChecker)
	putBlockStreamWriter(bsw)
	for _, bsr := range bsrs {
		putBlockStreamReader(bsr)
	}
	if err != nil {
		return err
	}
	//如果在原路径进行 merge, 则删除原来的 part 文件夹
	if strings.HasPrefix(dstPartPath, tableDir) {
		for _, partName := range partNames {
			partDir := filepath.Join(tableDir, partName)
			fs.MustRemoveAll(partDir)
		}
		os.Remove(filepath.Join(tableDir, "parts.json"))
	}
	fs.MustSyncPath(dstPartPath)
	return nil
}

func MergePartsByPartDirs(tableDir string, partDirs []string, dstPartPath string,
	prepareBlock PrepareBlockCallback,
	indexChecker IndexChecker) error {
	bsrs := make([]*blockStreamReader, 0, len(partDirs))
	for _, partDir := range partDirs {
		//partDir := filepath.Join(tableDir, partName)
		bsr := getBlockStreamReader()
		bsr.MustInitFromFilePart(partDir)
		bsrs = append(bsrs, bsr)
	}
	compressLevel := 4
	bsw := getBlockStreamWriter()
	bsw.MustInitFromFilePart(dstPartPath, true, compressLevel)
	err := mergePartsInternal(dstPartPath, bsw, bsrs, prepareBlock, indexChecker)
	putBlockStreamWriter(bsw)
	for _, bsr := range bsrs {
		putBlockStreamReader(bsr)
	}
	if err != nil {
		return err
	}
	//如果在原路径进行 merge, 则删除原来的 part 文件夹
	// if strings.HasPrefix(dstPartPath, tableDir) {
	// 	for _, partDir := range partDirs {
	// 		//partDir := filepath.Join(tableDir, partName)
	// 		fs.MustRemoveAll(partDir)
	// 	}
	// 	os.Remove(filepath.Join(tableDir, "parts.json"))
	// }
	fs.MustSyncPath(dstPartPath)
	return nil
}

func mergePartsInternal(dstPartPath string, bsw *blockStreamWriter, bsrs []*blockStreamReader,
	prepareBlock PrepareBlockCallback,
	indexChecker IndexChecker) error {
	var ph partHeader
	var itemsMerged atomic.Uint64
	stopCh := make(chan struct{})
	err := mergeBlockStreamsWithChecker(&ph, bsw, bsrs, prepareBlock, stopCh, &itemsMerged, indexChecker)
	if err != nil {
		logger.Errorf("mergeBlockStreams error, err=%w", err)
		return err
	}
	ph.MustWriteMetadata(dstPartPath)
	return nil
}

func mergeBlockStreamsWithChecker(ph *partHeader, bsw *blockStreamWriter,
	bsrs []*blockStreamReader,
	prepareBlock PrepareBlockCallback,
	stopCh <-chan struct{},
	itemsMerged *atomic.Uint64, indexChecker IndexChecker) error {
	bsm := bsmPool.Get().(*blockStreamMerger)
	if err := bsm.Init(bsrs, prepareBlock); err != nil {
		return fmt.Errorf("cannot initialize blockStreamMerger: %w", err)
	}
	bsm.checker = indexChecker
	err := bsm.Merge(bsw, ph, stopCh, itemsMerged)
	bsm.reset()
	bsmPool.Put(bsm)
	bsw.MustClose()
	if err == nil {
		return nil
	}
	return fmt.Errorf("cannot merge %d block streams: %s: %w", len(bsrs), bsrs, err)
}
