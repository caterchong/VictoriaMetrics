package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/countdata"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/countindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/exportmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var (
	storageDataPath = flag.String("storageDataPath", "", "Path to storage data(must offline)")
	action          = flag.String("action", "", "export_metrics: export all metrics to a file")
	output          = flag.String("output", "", "output file path")
	indexTableFlag  = flag.Int("index_table_flag", 3, "bit 0:curr, bit 1:prev")
	cpuProfile      = flag.String("cpuprofile", "", "if set, app will output a cpu.prof file")
	accountID       = flag.Int("account_id", 0, "tenant account id")
	projectID       = flag.Int("project_id", 0, "tenant project id")
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
		exportmetrics.ExportMetricsToFile(*storageDataPath, int8(*indexTableFlag), *output)
	case "export_metrics_by_tenant":
		exportmetrics.ExportMetricsToFileByTenant(*storageDataPath, int8(*indexTableFlag), *output, uint32(*accountID), uint32(*projectID))
	case "count_index":
		countindex.CountIndex(*storageDataPath, int8(*indexTableFlag))
	case "reindex":
		reindex.Reindex(*storageDataPath, int8(*indexTableFlag), *output)
	case "count_data":
		countdata.CountData(*storageDataPath, 3)
	default:
		logger.Panicf("unknown action:%s", *action)
	}
}
