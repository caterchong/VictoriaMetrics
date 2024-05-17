package exportmetrics

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func ExportMetricsToFile(storageDataPath string, indexTableFlag int8, output string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	outputDir := filepath.Dir(output)
	if !fs.IsPathExist(outputDir) {
		logger.Panicf("output dir [%s] not exists", outputDir)
	}
	if indexTableFlag < 1 || indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", indexTableFlag)
	}
	//
	outputFile, err := os.Create(output)
	if err != nil {
		logger.Panicf("create file %s error, err=%w", output, err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	var mn storage.MetricName
	metricText := make([]byte, 0, 1024)
	metricIDSet := metricidset.NewMetricIDSet()
	//indexPrefix := [1]byte{storage.NsPrefixMetricIDToMetricName}
	var cnt uint64
	callback := func(
		tableType int,
		partDir string,
		metaIndexRowNo int,
		blockHeaderNo int,
		itemNo int,
		data []byte) (isStop bool) {
		if data[0] == storage.NsPrefixMetricIDToMetricName {
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			metricID := encoding.UnmarshalUint64(data[9:])
			if metricIDSet.Add(accountID, projectID, metricID) {
				return // 不添加重复的 metric
			}
			mn.Reset()
			if err := mn.Unmarshal(data[17:]); err != nil {
				logger.Panicf("mn.Unmarshal error, err=%w", err)
			}
			metricText = mn.MarshalToMetricText(metricText[:0])
			writer.Write(metricText)
			cnt++
		}
		return false
	}
	err = storage.IterateAllIndexes(storageDataPath, indexTableFlag, callback)
	if err != nil {
		logger.Panicf("storage.IterateAllIndexes error, err=%w", err)
	}
	fmt.Printf("ok, export %d metrics to file\n", cnt)
}

func ExportMetricsToFileByTenant(storageDataPath string, indexTableFlag int8, output string, accountID, projectID uint32) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	outputDir := filepath.Dir(output)
	if !fs.IsPathExist(outputDir) {
		logger.Panicf("output dir [%s] not exists", outputDir)
	}
	if indexTableFlag < 1 || indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", indexTableFlag)
	}
	//
	outputFile, err := os.Create(output)
	if err != nil {
		logger.Panicf("create file %s error, err=%w", output, err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	var mn storage.MetricName
	metricText := make([]byte, 0, 1024)
	metricIDSet := metricidset.NewMetricIDSet()
	//indexPrefix := [1]byte{storage.NsPrefixMetricIDToMetricName}
	prefix := make([]byte, 0, 128)
	prefix = append(prefix, storage.NsPrefixMetricIDToMetricName)
	prefix = encoding.MarshalUint32(prefix, accountID)
	prefix = encoding.MarshalUint32(prefix, projectID)
	var cnt uint64
	var lessCount uint64
	var greatCount uint64
	callback := func(data []byte) (isStop bool) {
		if len(data) < len(prefix) {
			logger.Panicf("data len is less prefix")
		}
		if string(prefix) < string(data[:len(prefix)]) {
			lessCount++
		}
		if string(prefix) > string(data[:len(prefix)]) {
			greatCount++
		}

		if data[0] != storage.NsPrefixMetricIDToMetricName {
			logger.Panicf("data[0] error")
		}
		curAccountID := encoding.UnmarshalUint32(data[1:])
		curProjectID := encoding.UnmarshalUint32(data[5:])
		if curAccountID != accountID || curProjectID != projectID {
			logger.Panicf("accountID/projectID error")
		}
		metricID := encoding.UnmarshalUint64(data[9:])
		if metricIDSet.Add(accountID, projectID, metricID) {
			return // 不添加重复的 metric
		}
		mn.Reset()
		if err := mn.Unmarshal(data[17:]); err != nil {
			logger.Panicf("mn.Unmarshal error, err=%w", err)
		}
		metricText = mn.MarshalToMetricText(metricText[:0])
		writer.Write(metricText)
		cnt++

		return false
	}
	err = storage.SearchIndexes(storageDataPath, prefix, indexTableFlag, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	fmt.Printf("ok, export %d metrics to file\n", cnt)
	fmt.Printf("less=%d, great=%d\n", lessCount, greatCount) //todo: delete
}
