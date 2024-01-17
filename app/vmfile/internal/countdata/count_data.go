package countdata

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func CountData(storageDataPath string, dataPartitionFlag int8) {
	var minTsGlobal int64 = 0x7fffffffffffffff
	var maxTsGlobal int64 = -1
	var totalBlockCount uint64
	var totalItemsCount uint64
	metricIDSet := metricidset.NewMetricIDSet()
	metricIDs := make(map[uint64]uint64, 500)
	var totalRowCount uint64
	storage.EachPartition(storageDataPath, dataPartitionFlag,
		func(partitionType int8, path string) (isStop bool, err error) {
			fmt.Printf("partition:%s\n", filepath.Base(path))
			storage.EachMonthlyPartition(partitionType, path, func(partitionType int8, path string) (isStop bool, err error) {
				fmt.Printf("\tmonthly partition:%s\n", filepath.Base(path))
				storage.EachPart(partitionType, path, func(partitionType int8, path string) (isStop bool, err error) {
					fmt.Printf("\t\tpart:%s\n", filepath.Base(path))
					itemsCount, blocksCount, minTs, maxTs := storage.GetPartHeader(path)
					fmt.Printf("\t\t\tcount=%d, blocks=%d, from %s to %s\n", itemsCount, blocksCount,
						time.UnixMilli(minTs).Format("2006-01-02 15:04:05"), time.UnixMilli(maxTs).Format("2006-01-02 15:04:05"))
					//
					if minTs < minTsGlobal {
						minTsGlobal = minTs
					}
					if maxTs > maxTsGlobal {
						maxTsGlobal = maxTs
					}
					totalBlockCount += blocksCount
					totalItemsCount += itemsCount
					//

					err1 := storage.EachMetaIndexRow(partitionType, path, func(AccountID, ProjectID uint32, MetricID uint64, BlockHeadersCount uint32, IndexBlockOffset uint64, IndexBlockSize uint32, data []byte) {
						//metricIDSet.Add(AccountID, ProjectID, MetricID)
						//metricIDs[MetricID]++

						err3 := storage.GetBlocksData(data, int(BlockHeadersCount), func(AccountID, ProjectID uint32, MetricID uint64, RowsCount uint32) {
							metricIDSet.Add(AccountID, ProjectID, MetricID)
							metricIDs[MetricID]++
							totalRowCount += uint64(RowsCount)
						})
						if err3 != nil {
							logger.Panicf("GetBlocksData error,err=%w", err3)
						}
					})
					if err1 != nil {
						logger.Panicf("EachMetaIndexRow error,err=%w", err1)
					}
					return
				})
				return
			})
			return
		})
	//
	// if e := storage.IterateAllDatas(storageDataPath, 3, func(tsid *storage.TSID, rowCount uint32) (isStop bool, err error) {
	// 	metricIDSet.Add(tsid.AccountID, tsid.ProjectID, tsid.MetricID)
	// 	metricIDs[tsid.MetricID]++
	// 	totalRowCount += uint64(rowCount)
	// 	return
	// }); e != nil {
	// 	logger.Panicf("IterateAllDatas error, err=%w", e)
	// }
	//
	fmt.Printf("total items count:%d\n", totalItemsCount)
	fmt.Printf("total blocks count:%d\n", totalBlockCount)
	fmt.Printf("total metric id count:%d\n", metricIDSet.Len())
	fmt.Printf("\tfrom %s to %s\n", time.UnixMilli(minTsGlobal).Format("2006-01-02 15:04:05"), time.UnixMilli(maxTsGlobal).Format("2006-01-02 15:04:05"))
	//fmt.Printf("\n\t%+v\n", metricIDs)
	fmt.Printf("metric id map len:%d\n", len(metricIDs))
	fmt.Printf("row count:%d\n", totalRowCount)
	fmt.Println("ok")
}
