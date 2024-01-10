package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	storageDataPath = flag.String("storageDataPath", "", "Path to storage data(must offline)")
	action          = flag.String("action", "", "export_metrics: export all metrics to a file")
	output          = flag.String("output", "", "output file path")
	indexTableFlag  = flag.Int("index_table_flag", 3, "bit 0:curr, bit 1:prev")
	cpuProfile      = flag.String("cpuprofile", "", "if set, app will output a cpu.prof file")
)

func main() {
	runtime.GOMAXPROCS(1) // todo:
	flag.Parse()
	logger.Init()

	if len(*cpuProfile) > 0 {
		cpuFile, err := os.Create(*cpuProfile)
		if err != nil {
			panic(err)
		}
		defer cpuFile.Close()
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}
	//
	switch *action {
	case "export_metrics":
		exportMetricsToFile()
	default:
		logger.Panicf("unknown action:%s", *action)
	}
}

func exportMetricsToFile() {
	if !fs.IsPathExist(*storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", *storageDataPath)
	}
	outputDir := filepath.Dir(*output)
	if !fs.IsPathExist(outputDir) {
		logger.Panicf("output dir [%s] not exists", outputDir)
	}
	if *indexTableFlag < 1 || *indexTableFlag > 3 {
		logger.Panicf("index_table_flag=%d, must 1~3", *indexTableFlag)
	}
	//
	outputFile, err := os.Create(*output)
	if err != nil {
		logger.Panicf("create file %s error, err=%w", *output, err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	var mn storage.MetricName
	metricText := make([]byte, 0, 1024)
	metricIDSet := metricidset.NewMetricIDSet()
	indexPrefix := [1]byte{storage.NsPrefixMetricIDToMetricName}
	var cnt uint64
	err = storage.IterateAllIndexes(*storageDataPath, indexPrefix[:], int8(*indexTableFlag), func(data []byte) (isStop bool) {
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
	})
	if err != nil {
		logger.Panicf("storage.IterateAllIndexes error, err=%w", err)
	}
	fmt.Printf("ok, export %d metrics to file\n", cnt)
}
