package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/compare"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/countdata"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/countindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/exportmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/mergedatav2"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/mergev1"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/mergev2"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/mergev3"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/rebuild"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/rebuilddata"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/reindex"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/simplemerge"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/tablesearch"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	storageDataPath   = flag.String("storageDataPath", "", "Path to storage data(must offline)")
	action            = flag.String("action", "", "export_metrics: export all metrics to a file")
	output            = flag.String("output", "", "output file path")
	indexTableFlag    = flag.Int("index_table_flag", 3, "bit 0:curr, bit 1:prev")
	cpuProfile        = flag.String("cpuprofile", "", "if set, app will output a cpu.prof file")
	accountID         = flag.Int("account_id", 0, "tenant account id")
	projectID         = flag.Int("project_id", 0, "tenant project id")
	minScrapeInterval = flag.Duration("dedup.minScrapeInterval", 0, "Leave only the last sample in every time series per each discrete interval "+
		"equal to -dedup.minScrapeInterval > 0. See https://docs.victoriametrics.com/#deduplication for details")
	comparePathA    = flag.String("compare_a", "", "")
	comparePathB    = flag.String("compare_b", "", "")
	simpleMergeFrom = flag.String("simple_merge_from", "", "Multiple storage paths separated by commas")
	simpleMergeTo   = flag.String("simple_merge_to", "", "")
	simpleMergeFlag = flag.Int("simple_merge_flag", 3, "")
	//
	mergev1From = flag.String("mergev1_from", "", "Multiple storage paths separated by commas")
	mergev1To   = flag.String("mergev1_to", "", "")
	//
	mergeDataV2From = flag.String("merge_data_v2_from", "", "Multiple storage paths separated by commas")
	mergeDataV2To   = flag.String("merge_data_v2_to", "", "")
	//
	mergev2From = flag.String("mergev2_from", "", "Multiple storage paths separated by commas")
	mergev2To   = flag.String("mergev2_to", "", "")
	//
	mergev3From = flag.String("mergev3_from", "", "Multiple storage paths separated by commas")
	mergev3To   = flag.String("mergev3_to", "", "")
	cpu         = flag.Int("cpu", 1, "cpu count")

	// -action=mergeV2的参数列表，合并的时候，retention_deadline之前的数据会被丢弃，为空则不起作用
	retentionDeadline = flag.String("retention_deadline", "", `no limit if empty, should be a timestamp like "2021-07-01 00:00:00"`)
)

func main() {
	if *cpu < 1 {
		*cpu = 1
	}
	runtime.GOMAXPROCS(*cpu) //  默认 1 个核
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
	case "rebuild_data":
		storage.SetDedupInterval(*minScrapeInterval)
		rebuilddata.RebuildData(*storageDataPath, *output)
	case "rebuild":
		storage.SetDedupInterval(*minScrapeInterval)
		rebuild.Rebuild(*storageDataPath)
	case "compare":
		compare.Compare(*comparePathA, *comparePathB)
	case "simple_merge":
		if len(*simpleMergeFrom) == 0 {
			logger.Errorf("must set -simple_merge_from")
			return
		}
		if len(*simpleMergeTo) == 0 {
			logger.Errorf("must set -simple_merge_to")
			return
		}
		simplemerge.SimpleMerge(strings.Split(*simpleMergeFrom, ","), *simpleMergeTo, *simpleMergeFlag)
	case "use_storage":
		useStorage()
	case "offline_index_merge": // 使用 vm 本身提供的 merge 功能来进行 merge
		reindex.OfflineIndexMerge(*storageDataPath)
	case "merge_index_curr_and_prev":
		reindex.MergeIndexWithPrevAndCurr(*storageDataPath, *output)
	case "merge_v1":
		mergev1.Merge(strings.Split(*mergev1From, ","), *mergev1To)
	case "merge_data_v2":
		mergedatav2.MergeDataV2(strings.Split(*mergeDataV2From, ","), *mergeDataV2To)
	case "merge_v2":
		storage.SetDedupInterval(*minScrapeInterval)
		mergev2.Merge(strings.Split(*mergev2From, ","), *mergev2To, *retentionDeadline)
	case "merge_v3":
		storage.SetDedupInterval(*minScrapeInterval)
		mergev3.Merge(strings.Split(*mergev3From, ","), *mergev3To)
	case "table_search":
		tablesearch.TableSearch(strings.Split(*mergev3From, ","))
	default:
		logger.Panicf("unknown action:%s", *action)
	}
}

func useStorage() {
	s := storage.MustOpenStorage(*storageDataPath, time.Duration(24*365)*time.Hour, 0, 0)
	n, err := s.GetSeriesCount(0, 0, uint64(time.Now().Unix())+60)
	if err != nil {
		logger.Panicf("GetSeriesCount error, err=%w", err)
	}
	fmt.Printf("\tSeriesCount=%d\n", n)
	//s.GetTSDBStatus()
	storage.Show(s)
	s.MustClose()
}
