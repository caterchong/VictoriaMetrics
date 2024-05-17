package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

const (
	BestZstdCompressLevel = 4
)

// 从多个 part 目录打开数据文件
// partDirs 一定要传入同一个月份分区的数据
func Merge(partDirs []string, dstPartPath string, retentionDeadline int64) error {
	bsrs := make([]*blockStreamReader, 0, len(partDirs))
	for _, p := range partDirs {
		bsr := getBlockStreamReader()
		bsr.MustInitFromFilePart(p)
		bsrs = append(bsrs, bsr)
	}
	bsw := getBlockStreamWriter()
	bsw.MustInitFromFilePart(dstPartPath, true, BestZstdCompressLevel)
	//
	stopCh := make(chan struct{})
	ph, err := mergePartsInternalForFile(dstPartPath, bsw, bsrs, stopCh, retentionDeadline)
	putBlockStreamWriter(bsw)
	for _, bsr := range bsrs {
		putBlockStreamReader(bsr)
	}
	if err != nil {
		return err
	}
	if ph.RowsCount > 0 {
		fs.MustSyncPath(dstPartPath)
		logger.Infof("%+v", ph)
	} else {
		fs.MustRemoveAll(dstPartPath)
		logger.Infof("no data, so remove dir %s", dstPartPath)
	}

	// if no data, the ph info will crash storage when reading
	return nil
}

func mergePartsInternalForFile(dstPartPath string, bsw *blockStreamWriter,
	bsrs []*blockStreamReader,
	stopCh <-chan struct{}, retentionDeadline int64) (*partHeader, error) {
	var ph partHeader
	var rowsMerged atomic.Uint64
	var rowsDeleted atomic.Uint64
	//retentionDeadline := time.Now().UnixMilli() - 1000*60*60*24*365*50 // 50 年
	err := mergeBlockStreamsForFile(&ph, bsw, bsrs, stopCh, retentionDeadline, &rowsMerged, &rowsDeleted)
	if err != nil {
		return nil, fmt.Errorf("cannot merge %d parts to %s: %w", len(bsrs), dstPartPath, err)
	} else {
		logger.Infof("merge %d parts to %s, rowsMerged=%d, rowsDeleted=%d", len(bsrs), dstPartPath, rowsMerged, rowsDeleted)
	}

	ph.MustWriteMetadata(dstPartPath)

	return &ph, nil
}

func mergeBlockStreamsForFile(ph *partHeader, bsw *blockStreamWriter, bsrs []*blockStreamReader,
	stopCh <-chan struct{}, retentionDeadline int64,
	rowsMerged, rowsDeleted *atomic.Uint64) error {
	ph.Reset()

	bsm := bsmPool.Get().(*blockStreamMerger)
	bsm.Init(bsrs, retentionDeadline)
	err := mergeBlockStreamsInternalForFile(ph, bsw, bsm, stopCh, rowsMerged, rowsDeleted)
	bsm.reset()
	bsmPool.Put(bsm)
	bsw.MustClose()
	if err != nil {
		return fmt.Errorf("cannot merge %d streams: %s: %w", len(bsrs), bsrs, err)
	}
	return nil
}

func mergeBlockStreamsInternalForFile(ph *partHeader, bsw *blockStreamWriter, bsm *blockStreamMerger,
	stopCh <-chan struct{}, rowsMerged, rowsDeleted *atomic.Uint64) error {
	pendingBlockIsEmpty := true
	pendingBlock := getBlock()
	defer putBlock(pendingBlock)
	tmpBlock := getBlock()
	defer putBlock(tmpBlock)
	for bsm.NextBlock() {
		select {
		case <-stopCh:
			return errForciblyStopped
		default:
		}
		b := bsm.Block
		// 不考虑删除的 metric id
		// if dmis.Has(b.bh.TSID.MetricID) {
		// 	// Skip blocks for deleted metrics.
		// 	atomic.AddUint64(rowsDeleted, uint64(b.bh.RowsCount))
		// 	continue
		// }
		retentionDeadline := bsm.getRetentionDeadline(&b.bh)
		if b.bh.MaxTimestamp < retentionDeadline {
			// Skip blocks out of the given retention.
			rowsDeleted.Add(uint64(b.bh.RowsCount))
			continue
		}
		if pendingBlockIsEmpty {
			// Load the next block if pendingBlock is empty.
			pendingBlock.CopyFrom(b)
			pendingBlockIsEmpty = false
			continue
		}

		// Verify whether pendingBlock may be merged with b (the current block).
		if pendingBlock.bh.TSID.MetricID != b.bh.TSID.MetricID {
			// Fast path - blocks belong to distinct time series.
			// Write the pendingBlock and then deal with b.
			if b.bh.TSID.Less(&pendingBlock.bh.TSID) {
				logger.Panicf("BUG: the next TSID=%+v is smaller than the current TSID=%+v", &b.bh.TSID, &pendingBlock.bh.TSID)
			}
			bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
			pendingBlock.CopyFrom(b)
			continue
		}
		if pendingBlock.tooBig() && pendingBlock.bh.MaxTimestamp <= b.bh.MinTimestamp {
			// Fast path - pendingBlock is too big and it doesn't overlap with b.
			// Write the pendingBlock and then deal with b.
			bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
			pendingBlock.CopyFrom(b)
			continue
		}

		// Slow path - pendingBlock and b belong to the same time series,
		// so they must be merged.
		if err := unmarshalAndCalibrateScale(pendingBlock, b); err != nil {
			return fmt.Errorf("cannot unmarshal and calibrate scale for blocks to be merged: %w", err)
		}
		tmpBlock.Reset()
		tmpBlock.bh.TSID = b.bh.TSID
		tmpBlock.bh.Scale = b.bh.Scale
		tmpBlock.bh.PrecisionBits = minUint8(pendingBlock.bh.PrecisionBits, b.bh.PrecisionBits)
		mergeBlocks(tmpBlock, pendingBlock, b, retentionDeadline, rowsDeleted)
		if len(tmpBlock.timestamps) <= maxRowsPerBlock {
			// More entries may be added to tmpBlock. Swap it with pendingBlock,
			// so more entries may be added to pendingBlock on the next iteration.
			if len(tmpBlock.timestamps) > 0 {
				tmpBlock.fixupTimestamps()
			} else {
				pendingBlockIsEmpty = true
			}
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}

		// Write the first maxRowsPerBlock of tmpBlock.timestamps to bsw,
		// leave the rest in pendingBlock.
		tmpBlock.nextIdx = maxRowsPerBlock
		pendingBlock.CopyFrom(tmpBlock)
		pendingBlock.fixupTimestamps()
		tmpBlock.nextIdx = 0
		tmpBlock.timestamps = tmpBlock.timestamps[:maxRowsPerBlock]
		tmpBlock.values = tmpBlock.values[:maxRowsPerBlock]
		tmpBlock.fixupTimestamps()
		bsw.WriteExternalBlock(tmpBlock, ph, rowsMerged)
	}
	if err := bsm.Error(); err != nil {
		return fmt.Errorf("cannot read block to be merged: %w", err)
	}
	if !pendingBlockIsEmpty {
		bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
	}
	return nil
}
