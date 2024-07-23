package compare

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func Compare(a string, b string) {
	if !fs.IsPathExist(a) {
		logger.Panicf("path %s not exists", a)
	}
	if !fs.IsPathExist(b) {
		logger.Panicf("path %s not exists", a)
	}
	metricIdOfA := GetAllMetricIDOfCurrentTable(a)
	metricIdOfB := GetAllMetricIDOfCurrentTable(b)
	fmt.Printf("metric id count:%d %d\n", metricIdOfA.Len(), metricIdOfB.Len())
	fmt.Printf("\tsame metric id=%d\n", metricidset.CountSameMetricID(metricIdOfA, metricIdOfB))
}

func GetAllMetricIDOfCurrentTable(storageDataPath string) *metricidset.MetricIDSet {
	metricIDSet := metricidset.NewMetricIDSet()
	var prefix = [1]byte{storage.NsPrefixMetricIDToTSID}
	callback := func(data []byte) (isStop bool) {
		if data[0] != storage.NsPrefixMetricIDToTSID {
			logger.Panicf("data is not 2")
		}
		curAccountID := encoding.UnmarshalUint32(data[1:])
		curProjectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		metricIDSet.Add(curAccountID, curProjectID, metricID)
		return
	}
	err := storage.SearchIndexes(storageDataPath, prefix[:], storage.IndexTableFlagOfCurr|storage.IndexTableFlagOfPrev, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	return metricIDSet
}
