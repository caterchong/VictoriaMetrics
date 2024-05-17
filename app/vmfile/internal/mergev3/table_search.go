package mergev3

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

// 测试在表格中搜索
func GetIndexDbPartDirs(fromPaths []string) []string {
	dirs := make([]string, 0, 200)
	for _, p := range fromPaths {
		indexdbDir := filepath.Join(p, storage.IndexdbDirname)
		if !fs.IsPathExist(indexdbDir) {
			logger.Panicf("indexdb [%s] not exists", indexdbDir)
		}
		_, indexTableCurr, indexTablePrev := storage.GetIndexDBTableNames(indexdbDir)
		// 找到 part names
		currPartNames := storage.ReadPartsNameOfIndexTable(indexTableCurr)
		if len(currPartNames) == 0 {
			logger.Infof("current partition empty: %s", indexTableCurr)
		}
		prevPartNames := storage.ReadPartsNameOfIndexTable(indexTablePrev)
		if len(prevPartNames) == 0 {
			logger.Infof("prev partition empty: %s", indexTablePrev)
		}
		for _, partName := range currPartNames {
			partDir := filepath.Join(indexTableCurr, partName)
			dirs = append(dirs, partDir)
		}
		for _, partName := range prevPartNames {
			partDir := filepath.Join(indexTablePrev, partName)
			dirs = append(dirs, partDir)
		}
	}
	return dirs
}

func GetDupIndexInfo(fromPaths []string) (mainid map[uint64][]uint64, set *uint64set.Set, err error) {
	// if len(fromPaths) < 1 {
	// 	logger.Errorf("from path count must great 0")
	// 	err = errors.New("fromPaths empty")
	// 	return
	// }
	// for _, p := range fromPaths {
	// 	if !fs.IsPathExist(p) {
	// 		logger.Errorf("from path %s not exists", p)
	// 		err = fmt.Errorf("path %s not exists", p)
	// 		return
	// 	}
	// }
	// 所有数据的所有 part 目录
	dirs := GetIndexDbPartDirs(fromPaths)
	if len(dirs) == 0 {
		logger.Errorf("not have any dir to merge")
		err = fmt.Errorf("not find any part dir")
		return
	}
	//
	finder := NewDupMetricNameFinderFromPartDirs(dirs)
	finder.Do()
	mainid = finder.dupIdMap
	set = finder.dupIds
	finder.dupIdMap = nil
	finder.dupIds = nil
	mergeset.ReleaseTableSearch(finder.TS)
	finder.TS = nil
	finder = nil
	return
	//finder.Show()
}

func testDirs(dirs []string) {
	ts := mergeset.GetTableSearchFromDirs(dirs)
	prefix := []byte{storage.NsPrefixDateMetricNameToTSID}
	ts.Seek(prefix)
	var prev []byte
	var last []byte
	var count uint64
	for ts.NextItem() {
		count++
		if ts.Item[0] != storage.NsPrefixDateMetricNameToTSID {
			logger.Panicf("not index 7")
			prev = nil
			last = append(last[:0], ts.Item...)
			continue
		}
		if len(last) > 0 && string(last) > string(ts.Item) {
			logger.Panicf("not ordered")
		}
		last = append(last[:0], ts.Item...)
		//
		data := ts.Item[metricNameStartAt:]
		loc := bytes.IndexByte(data, 2)
		if loc < 0 {
			logger.Panicf("date+metric index format error, tag end not found")
		}
		toCompare := data[:loc+1] //
		if len(prev) == 0 {
			prev = toCompare
			continue
		}
		if !bytes.Equal(prev, toCompare) {
			prev = toCompare
			continue
		}
		data = data[loc+1:]
		if len(data) < 8 || len(data)%8 != 0 {
			logger.Panicf("metric data format error")
		}
		// 1个 metricName 指向 多个 tsid

	}
	fmt.Printf("ok, count=%d\n", count)
}

//const metricNameStartAt = 17

// DupMetricNameFinder 用于找到索引中重复的 metric 名字
type DupMetricNameFinder struct {
	TS             *mergeset.TableSearch
	prevMetricName []byte
	dupIds         *uint64set.Set
	mainID         uint64
	dupIdMap       map[uint64][]uint64
	metricNameMap  map[string]uint64
	recordCount    uint64
	prevTsid       storage.TSID
	currTsid       storage.TSID
}

func NewDupMetricNameFinderFromPartDirs(dirs []string) *DupMetricNameFinder {
	inst := &DupMetricNameFinder{
		dupIds:        &uint64set.Set{}, // todo: 并未考虑多租户的情况
		dupIdMap:      make(map[uint64][]uint64, 50000),
		metricNameMap: make(map[string]uint64, 10000),
	}
	inst.TS = mergeset.GetTableSearchFromDirs(dirs)
	return inst
}

func (d *DupMetricNameFinder) Do() {
	prefix := []byte{storage.NsPrefixDateMetricNameToTSID}
	d.TS.Seek(prefix)
	for d.TS.NextItem() {
		d.each(d.TS.Item)
		d.recordCount++
	}
}

func (d *DupMetricNameFinder) each(data []byte) {
	if data[0] != storage.NsPrefixDateMetricNameToTSID {
		logger.Panicf("not index 7")
	}
	data = data[metricNameStartAt:]
	loc := bytes.IndexByte(data, 2)
	if loc < 0 {
		logger.Panicf("date+metric index format error, tag end not found")
	}
	metricName := data[:loc] //
	data = data[loc+1:]
	if len(data) != tsidSize {
		logger.Panicf("tsid data format error")
	}
	// if len(data) < tsidSize || len(data)%tsidSize != 0 {
	// 	logger.Panicf("tsid data format error")
	// }
	_, err := d.currTsid.Unmarshal(data)
	if err != nil {
		logger.Panicf("tsid data format error")
	}

	// d.MetricIDs = d.MetricIDs[:0]
	// for i := 0; i < len(data); i += 8 {
	// 	id := encoding.UnmarshalUint64(data[i : i+8])
	// 	if id > 0 {
	// 		d.MetricIDs = append(d.MetricIDs, id)
	// 	}
	// }
	d.eachMetric(metricName)
}

const tsidSize = 32

func (d *DupMetricNameFinder) eachMetric(metricName []byte) {
	if len(d.prevMetricName) == 0 || !bytes.Equal(d.prevMetricName, metricName) || d.prevTsid.MetricID == d.currTsid.MetricID {
		d.prevMetricName = append(d.prevMetricName[:0], metricName...)
		//d.prevMetricIDs, d.MetricIDs = swapSlice(metricIDs, d.prevMetricIDs)
		d.prevTsid = d.currTsid
		d.mainID = 0
		return
	}
	// 把曾经重复过的 id 记录下来
	d.dupIds.Add(d.currTsid.MetricID)
	d.dupIds.Add(d.prevTsid.MetricID)
	if d.mainID == 0 {
		id, ok := d.metricNameMap[string(metricName)]
		if ok {
			d.mainID = id
		} else {
			d.mainID = d.currTsid.MetricID
			d.metricNameMap[string(metricName)] = d.mainID
			if _, ok = d.dupIdMap[d.mainID]; ok {
				fmt.Printf("\tmainid dup: %016X\n", d.mainID)
			}
		}
	}
	arr, has := d.dupIdMap[d.mainID]
	if !has {
		arr = make([]uint64, 0, 10)
	}
	arr = append(arr, d.prevTsid.MetricID)
	d.dupIdMap[d.mainID] = arr
}

func (d *DupMetricNameFinder) Show() {
	fmt.Printf("dup metric id count:%d\n", d.dupIds.Len())
	fmt.Printf("main id count:%d, metricName count=%d\n", len(d.dupIdMap), len(d.metricNameMap))
	var total uint64
	for _, arr := range d.dupIdMap {
		tempMap := make(map[uint64]struct{}, len(arr))
		for _, id := range arr {
			tempMap[id] = struct{}{}
		}
		total += uint64(len(tempMap))
	}
	fmt.Printf("sub id count:%d\n", total)
	fmt.Printf("record count:%d\n", d.recordCount)
}

func swapSlice(a, b []uint64) ([]uint64, []uint64) {
	return b, a
}
