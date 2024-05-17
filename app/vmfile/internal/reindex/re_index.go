package reindex

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmfile/internal/metricidset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func showTagIndex(data []byte, tableType int, curTableMetricID *metricidset.MetricIDSet) {
	t := TagIndex{}
	err := ParseTagIndex(data, &t)
	if err != nil {
		fmt.Printf("\n\n%X\n\n", data)
		logger.Panicf("tag index format error, err=%w", err)
	}
	if tableType != storage.IndexTableFlagOfPrev {
		return
	}
	// todo: 判断所有的 metrics id， 把重复的 metrics id 进行删除
	if len(t.MetricGroupName) > 0 {
		if len(t.Key) > 0 {
			fmt.Printf("\t\t%s{%s=\"%s\"} %d(%d)\n", string(t.MetricGroupName), string(t.Key), string(t.Value), t.MetricIDs[0], len(t.MetricIDs))
		} else {
			fmt.Printf("\t\t%s{} %d(%d)\n", string(t.MetricGroupName), t.MetricIDs[0], len(t.MetricIDs))
		}
	} else {
		fmt.Printf("\t\t{%s=\"%s\"} %d(%d)\n", string(t.Key), string(t.Value), t.MetricIDs[0], len(t.MetricIDs))
	}
}

func MustRemoveAllExcept(dir string, names []string) {
	s, err := os.Stat(dir)
	if err != nil {
		logger.Panicf("stat [%s] error, err=%w", dir, err)
	}
	if !s.IsDir() {
		logger.Panicf("path [%s] not dir", dir)
	}
	des := fs.MustReadDir(dir)
	nameMap := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameMap[n] = struct{}{}
	}
	for _, de := range des {
		if _, has := nameMap[de.Name()]; has {
			continue
		}
		if de.IsDir() {
			os.RemoveAll(filepath.Join(dir, de.Name()))
		} else {
			os.Remove(filepath.Join(dir, de.Name()))
		}
	}
}

func getDifferenceSetOfMetricID(storageDataPath string) *metricidset.MetricIDSet {
	notInCurr := metricidset.NewMetricIDSet()
	//
	metricIDSet := GetAllMetricIDOfCurrentTable(storageDataPath)
	var prefix = [1]byte{storage.NsPrefixMetricIDToTSID}
	callback := func(data []byte) (isStop bool) {
		if data[0] != storage.NsPrefixMetricIDToTSID {
			logger.Panicf("data is not 2")
		}
		curAccountID := encoding.UnmarshalUint32(data[1:])
		curProjectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		if !metricIDSet.Has(curAccountID, curProjectID, metricID) {
			notInCurr.Add(curAccountID, curProjectID, metricID)
		}
		return
	}
	err := storage.SearchIndexes(storageDataPath, prefix[:], storage.IndexTableFlagOfPrev, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	return notInCurr
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
	err := storage.SearchIndexes(storageDataPath, prefix[:], storage.IndexTableFlagOfCurr, callback)
	if err != nil {
		logger.Panicf("storage.SearchIndexes error, err=%w", err)
	}
	return metricIDSet
}

type TagIndex struct {
	AccountID       uint32
	ProjectID       uint32
	MetricIDs       []uint64
	MetricGroupName []byte
	Key             []byte
	Value           []byte
}

func ParseTagIndex(data []byte, out *TagIndex) (err error) { // 索引 1 的格式
	out.AccountID = encoding.UnmarshalUint32(data[1:])
	out.ProjectID = encoding.UnmarshalUint32(data[5:])
	data = data[9:]
	switch data[0] {
	case 1:
		// metricGroupName -> metric id
		data = data[1:]
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(metricGroupName -> metric id)")
			return
		}
		out.MetricGroupName = data[:idx]
		data = data[idx+1:]
	case 0xfe:
		// metricGroupName + tag -> metric id
		data = data[1:]
		var nameLen uint64
		data, nameLen, err = encoding.UnmarshalVarUint64(data)
		if err != nil {
			err = fmt.Errorf("name len error(index 3/fe), err=%w", err)
			return
		}
		out.MetricGroupName = data[:nameLen]
		data = data[nameLen:]
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(index 3/fe)")
			return
		}
		out.Key = data[:idx]
		data = data[idx+1:]
		idx = bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag value(index 3/fe)")
			return
		}
		out.Value = data[:idx]
		data = data[idx+1:]
	default:
		// tag -> metric id
		idx := bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag key(index 3/)")
			return
		}
		out.Key = data[:idx]
		data = data[idx+1:]
		idx = bytes.IndexByte(data, 1)
		if idx < 0 {
			err = fmt.Errorf("not found tag value(index 3/)")
			return
		}
		out.Value = data[:idx]
		data = data[idx+1:]
	}
	if len(data) < 8 || len(data)%8 != 0 {
		err = fmt.Errorf("metric id format error(metricGroupName -> metric id)")
		return
	}
	for len(data) > 0 {
		out.MetricIDs = append(out.MetricIDs, encoding.UnmarshalUint64(data))
		data = data[8:]
	}
	return
}

// 使用 vm 本身提供的 merge 功能来进行 merge
func OfflineIndexMerge(storageDataPath string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	indexdbDir := filepath.Join(storageDataPath, storage.IndexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		logger.Panicf("indexdb [%s] not exists", indexdbDir)
	}
	_, indexTableCurr, indexTablePrev := storage.GetIndexDBTableNames(indexdbDir)
	ts := time.Now().UnixNano()
	dstPath := filepath.Join(indexTableCurr, fmt.Sprintf("%016X", ts))
	ts++
	mergeset.MergeParts(indexTableCurr, storage.ReadPartsNameOfIndexTable(indexTableCurr),
		dstPath, nil, nil)
	//
	dstPathPrev := filepath.Join(indexTablePrev, fmt.Sprintf("%016X", ts))
	ts++
	mergeset.MergeParts(indexTablePrev, storage.ReadPartsNameOfIndexTable(indexTablePrev),
		dstPathPrev, nil, nil)
}

// 把 prev 的索引 merge 到 curr，并丢弃重复项
func MergeIndexWithPrevAndCurr(storageDataPath string, outputDir string) {
	if !fs.IsPathExist(storageDataPath) {
		logger.Panicf("storageDataPath [%s] not exists", storageDataPath)
	}
	if len(outputDir) > 0 {
		// if !fs.IsPathExist(filepath.Dir(outputDir)) {
		// 	logger.Panicf("outputDir parent path [%s] not exists", filepath.Dir(outputDir))
		// }
		fs.MustMkdirIfNotExist(outputDir)
	} else {
		outputDir = storageDataPath
	}
	indexdbDir := filepath.Join(storageDataPath, storage.IndexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		logger.Panicf("indexdb [%s] not exists", indexdbDir)
	}
	//curTableMetricID := GetAllMetricIDOfCurrentTable(storageDataPath) // 得到 current 分区的所有 id
	//addedMetricIDForIndex2 := metricidset.NewMetricIDSet()
	//addedMetricIDForIndex3 := metricidset.NewMetricIDSet()
	//
	_, indexTableCurr, indexTablePrev := storage.GetIndexDBTableNames(indexdbDir)
	ts := time.Now().UnixNano()
	dstPath := filepath.Join(outputDir, storage.IndexdbDirname,
		filepath.Base(indexTableCurr), fmt.Sprintf("%016X", ts))
	ts++
	// 找到 part names
	currPartNames := storage.ReadPartsNameOfIndexTable(indexTableCurr)
	prevPartNames := storage.ReadPartsNameOfIndexTable(indexTablePrev)
	dirs := make([]string, 0, len(currPartNames)+len(prevPartNames))
	for _, partName := range currPartNames {
		partDir := filepath.Join(indexTableCurr, partName)
		dirs = append(dirs, partDir)
	}
	for _, partName := range prevPartNames {
		partDir := filepath.Join(indexTablePrev, partName)
		dirs = append(dirs, partDir)
	}
	//
	// var addedCount uint64
	// var skipCount uint64
	// skipItemCallback := func(data []byte) (isSkip bool) {
	// 	indexType := data[0]
	// 	//
	// 	switch indexType {
	// 	// case storage.NsPrefixMetricIDToTSID:
	// 	// 	accountID := encoding.UnmarshalUint32(data[1:])
	// 	// 	projectID := encoding.UnmarshalUint32(data[5:])
	// 	// 	metricID := encoding.UnmarshalUint64(data[9:])
	// 	// 	if addedMetricIDForIndex2.Add(accountID, projectID, metricID) {
	// 	// 		skipCount++
	// 	// 		return true
	// 	// 	}
	// 	// 	addedCount++
	// 	case storage.NsPrefixMetricIDToMetricName:
	// 		accountID := encoding.UnmarshalUint32(data[1:])
	// 		projectID := encoding.UnmarshalUint32(data[5:])
	// 		metricID := encoding.UnmarshalUint64(data[9:])
	// 		if addedMetricIDForIndex3.Has(accountID, projectID, metricID) {
	// 			skipCount++
	// 			return true
	// 		}
	// 		addedMetricIDForIndex3.Add(accountID, projectID, metricID)
	// 		addedCount++
	// 	}
	// 	return false
	// }
	//indexWithTag := mergeset.TagIndex{}
	//indexWithTag.Reset()
	// addedMetricIDForIndex1MetricGroup := metricidset.NewMetricIDSet()
	// addedMetricIDForIndex1GroupAndTag := metricidset.NewMetricIDSet()
	// addedMetricIDForIndex1Tag := metricidset.NewMetricIDSet()
	// var addedCountOfIndex1 uint64
	// var skipCountOfIndex1 uint64
	// checkIndexType1 := func(cur []byte) bool {
	// 	indexWithTag.Reset()
	// 	if err := indexWithTag.Unmarshal(cur); err != nil {
	// 		logger.Panicf("tag index format error, err=%w", err)
	// 	}
	// 	// 处理三种索引
	// 	var set *metricidset.MetricIDSet
	// 	if len(indexWithTag.MetricGroup) > 0 {
	// 		if len(indexWithTag.Key) > 0 {
	// 			set = addedMetricIDForIndex1GroupAndTag
	// 		} else {
	// 			set = addedMetricIDForIndex1MetricGroup
	// 		}
	// 	} else {
	// 		set = addedMetricIDForIndex1Tag
	// 	}
	// 	//检查 metric id 去重
	// 	leftMetricIDs := compareMetricIDs(indexWithTag.AccountID, indexWithTag.ProjectID,
	// 		indexWithTag.MetricIDs, set)
	// 	return len(leftMetricIDs) == 0
	// }
	// if checkIndexType1 != nil {
	// 	checkIndexType1 = nil
	// }
	//
	index1Filter := NewFilterForTagIndex()
	index2Filter := NewFilterForMetricIDIndex()
	index3Filter := NewFilterForMetricIDIndex()
	index5Filter := NewFilterForDateToMetricIDIndex()
	index6Filter := NewFilterForDateAndTagToMetricIDIndex()
	index7Filter := NewFilterForDateAndMetricNameIndex()

	prepareBlockCallback := func(alldata []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
		idx := 0
		for idx < len(items) {
			cur := items[idx].Bytes(alldata)
			indexType := cur[0]
			//
			switch indexType {
			case storage.NsPrefixTagToMetricIDs:
				if index1Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index1Filter.SkipCount++
					continue
				}
				index1Filter.AddCount++
			case storage.NsPrefixMetricIDToTSID:
				if index2Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index2Filter.SkipCount++
					continue
				}
				index2Filter.AddCount++
			case storage.NsPrefixMetricIDToMetricName:
				if index3Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index3Filter.SkipCount++
					continue
				}
				index3Filter.AddCount++
			case storage.NsPrefixDateToMetricID:
				if index5Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index5Filter.SkipCount++
					continue
				}
				index5Filter.AddCount++
			case storage.NsPrefixDateTagToMetricIDs:
				if index6Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index6Filter.SkipCount++
					continue
				}
				index6Filter.AddCount++
			case storage.NsPrefixDateMetricNameToTSID:
				if index7Filter.IsSkip(cur) {
					items = append(items[:idx], items[idx+1:]...)
					index7Filter.SkipCount++
					continue
				}
				index7Filter.AddCount++
			}
			idx++
		}
		return alldata, items
	}
	mergeset.MergePartsByPartDirs(indexTableCurr, dirs,
		dstPath,
		prepareBlockCallback,
		//nil,
		//skipItemCallback, // 业务逻辑写在这里，得不到正确的结果. //??? 为什么
		nil,
	)
	//
	if strings.HasPrefix(outputDir, storageDataPath) {
		// 同一文件夹的时候，删除不需要的文件
		os.Remove(filepath.Join(indexTablePrev, "parts.json"))
	} else {
		prevTablePath := filepath.Join(outputDir, storage.IndexdbDirname,
			filepath.Base(indexTablePrev))
		fs.MustMkdirIfNotExist(prevTablePath)
	}
	//fmt.Printf("ok. addedCount=%d, skipCount=%d\n", addedCount, skipCount)
	//fmt.Printf("\tmetric id, index2=%d, index3=%d\n", addedMetricIDForIndex2.Len(), addedMetricIDForIndex3.Len())
	fmt.Printf("OK\n")
	fmt.Printf("\tindex 1: add %d, skip %d\n", index1Filter.AddCount, index1Filter.SkipCount)
	fmt.Printf("\tindex 2: add %d, skip %d\n", index2Filter.AddCount, index2Filter.SkipCount)
	fmt.Printf("\tindex 3: add %d, skip %d\n", index3Filter.AddCount, index3Filter.SkipCount)
	fmt.Printf("\tindex 5: add %d, skip %d\n", index5Filter.AddCount, index5Filter.SkipCount)
	fmt.Printf("\tindex 6: add %d, skip %d\n", index6Filter.AddCount, index6Filter.SkipCount)
	fmt.Printf("\tindex 7: add %d, skip %d\n", index7Filter.AddCount, index7Filter.SkipCount)
}

// func compareMetricIDs(accountID, projectID uint32, metricIDs []uint64, set *metricidset.MetricIDSet) []uint64 {
// 	c := 0
// 	i := len(metricIDs) - 1
// 	for i >= 0 {
// 		c++
// 		if c > 10000 {
// 			logger.Panicf("error")
// 		}
// 		id := metricIDs[i]
// 		if !set.Add(accountID, projectID, id) {
// 			i--
// 			continue
// 		}
// 		metricIDs[i] = metricIDs[len(metricIDs)-1]
// 		metricIDs = metricIDs[:len(metricIDs)-1]
// 		i--
// 	}
// 	return metricIDs
// }

// FilterForTagIndex 索引 1 的过滤器
type FilterForTagIndex struct {
	IndexInfo               mergeset.TagIndex
	MetricIDsForGroup       *metricidset.MetricIDSet
	MetricIDsForGroupAndTag *metricidset.MetricIDSet
	MetricIDsForTag         *metricidset.MetricIDSet
	AddCount                uint64
	SkipCount               uint64
	tempMetricIDs           []uint64
}

func NewFilterForTagIndex() *FilterForTagIndex {
	return &FilterForTagIndex{
		MetricIDsForGroup:       metricidset.NewMetricIDSet(),
		MetricIDsForGroupAndTag: metricidset.NewMetricIDSet(),
		MetricIDsForTag:         metricidset.NewMetricIDSet(),
		tempMetricIDs:           make([]uint64, 0, 100),
	}
}

func (f *FilterForTagIndex) IsSkip(data []byte) bool {
	f.IndexInfo.Reset()
	if err := f.IndexInfo.Unmarshal(data); err != nil {
		logger.Panicf("tag index format error, err=%w", err)
	}
	// 处理三种索引
	var set *metricidset.MetricIDSet
	if len(f.IndexInfo.MetricGroup) > 0 {
		if len(f.IndexInfo.Key) > 0 {
			set = f.MetricIDsForGroupAndTag
		} else {
			set = f.MetricIDsForGroup
		}
	} else {
		set = f.MetricIDsForTag
	}
	//检查 metric id 去重
	leftMetricIDs := f.compareMetricIDs(set)
	return len(leftMetricIDs) == 0
}

func (f *FilterForTagIndex) compareMetricIDs(set *metricidset.MetricIDSet) []uint64 {
	f.tempMetricIDs = append(f.tempMetricIDs[:0], f.IndexInfo.MetricIDs...)
	ids := f.tempMetricIDs
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		if set.Add(f.IndexInfo.AccountID, f.IndexInfo.ProjectID, id) {
			ids[i] = ids[len(ids)-1]
			ids = ids[:len(ids)-1]
		}
	}
	return ids
}

// 删除重复的 metric id
func (f *FilterForTagIndex) DeleteDupMetricID(mainid map[uint64][]uint64, setOfDupID *uint64set.Set, alldata []byte, items []mergeset.Item, idx int) []mergeset.Item {
	f.tempMetricIDs = append(f.tempMetricIDs[:0], f.IndexInfo.MetricIDs...)
	ids := f.tempMetricIDs
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		if !setOfDupID.Has(id) {
			continue
		}
		if _, has := mainid[id]; has { // main id 不丢弃
			continue
		}
		ids[i] = ids[len(ids)-1]
		ids = ids[:len(ids)-1]
	}
	if len(ids) == 0 {
		items = append(items[:idx], items[idx+1:]...)
		return items
	}
	// todo: 修改这条记录 (好麻烦……)
	return items
}

type FilterForMetricIDIndex struct {
	MetricIDs *metricidset.MetricIDSet
	AddCount  uint64
	SkipCount uint64
}

func NewFilterForMetricIDIndex() *FilterForMetricIDIndex {
	return &FilterForMetricIDIndex{
		MetricIDs: metricidset.NewMetricIDSet(),
	}
}

func (f *FilterForMetricIDIndex) IsSkip(data []byte) bool {
	accountID := encoding.UnmarshalUint32(data[1:])
	projectID := encoding.UnmarshalUint32(data[5:])
	metricID := encoding.UnmarshalUint64(data[9:])
	return f.MetricIDs.Add(accountID, projectID, metricID)
}

type FilterForDateToMetricIDIndex struct {
	Dates     map[uint64]*metricidset.MetricIDSet
	MetricIDs []uint64
	AddCount  uint64
	SkipCount uint64
}

func NewFilterForDateToMetricIDIndex() *FilterForDateToMetricIDIndex {
	return &FilterForDateToMetricIDIndex{
		Dates:     make(map[uint64]*metricidset.MetricIDSet, 100),
		MetricIDs: make([]uint64, 0, 100),
	}
}

func (f *FilterForDateToMetricIDIndex) IsSkip(data []byte) bool {
	accountID := encoding.UnmarshalUint32(data[1:])
	projectID := encoding.UnmarshalUint32(data[5:])
	date := encoding.UnmarshalUint64(data[9:])
	data = data[17:]
	if len(data) < 8 || len(data)%8 != 0 {
		logger.Panicf("date index format error")
	}
	m, ok := f.Dates[date]
	if !ok {
		m = metricidset.NewMetricIDSet()
		f.Dates[date] = m
	}
	f.MetricIDs = f.MetricIDs[:0]
	for i := 0; i < len(data); i += 8 {
		f.MetricIDs = append(f.MetricIDs, encoding.UnmarshalUint64(data[i:i+8]))
	}
	// 判断是否都在
	for i := 0; i < len(f.MetricIDs); i++ {
		id := f.MetricIDs[i]
		if m.Add(accountID, projectID, id) {
			f.MetricIDs[i] = f.MetricIDs[len(f.MetricIDs)-1]
			f.MetricIDs = f.MetricIDs[:len(f.MetricIDs)-1]
		}
	}
	return len(f.MetricIDs) == 0
}

type FilterForDateAndTagToMetricIDIndex struct { // 索引 6
	DatesForGroup       map[uint64]*metricidset.MetricIDSet
	DatesForGroupAndTag map[uint64]*metricidset.MetricIDSet
	DatesForTag         map[uint64]*metricidset.MetricIDSet
	IndexInfo           mergeset.TagIndex
	//MetricIDs           []uint64
	AddCount  uint64
	SkipCount uint64
}

func NewFilterForDateAndTagToMetricIDIndex() *FilterForDateAndTagToMetricIDIndex {
	return &FilterForDateAndTagToMetricIDIndex{
		DatesForGroup:       make(map[uint64]*metricidset.MetricIDSet, 100),
		DatesForGroupAndTag: make(map[uint64]*metricidset.MetricIDSet, 100),
		DatesForTag:         make(map[uint64]*metricidset.MetricIDSet, 100),
		//MetricIDs:           make([]uint64, 0, 100),
	}
}

func (f *FilterForDateAndTagToMetricIDIndex) IsSkip(data []byte) bool {
	accountID := encoding.UnmarshalUint32(data[1:])
	projectID := encoding.UnmarshalUint32(data[5:])
	date := encoding.UnmarshalUint64(data[9:])
	data = data[17:]
	f.IndexInfo.Reset()
	if err := f.IndexInfo.UnmarshalFromTag(data); err != nil {
		logger.Panicf("err=%w", err)
	}
	var set *metricidset.MetricIDSet
	var has bool
	if len(f.IndexInfo.MetricGroup) > 0 {
		if len(f.IndexInfo.Key) > 0 {
			set, has = f.DatesForGroupAndTag[date]
			if !has {
				set = metricidset.NewMetricIDSet()
				f.DatesForGroupAndTag[date] = set
			}
		} else {
			set, has = f.DatesForGroup[date]
			if !has {
				set = metricidset.NewMetricIDSet()
				f.DatesForGroup[date] = set
			}
		}
	} else {
		set, has = f.DatesForTag[date]
		if !has {
			set = metricidset.NewMetricIDSet()
			f.DatesForTag[date] = set
		}
	}
	// for _, metricID := range f.IndexInfo.MetricIDs {
	// 	set.Add(accountID, projectID, metricID)
	// }
	// 判断是否都在
	metricIDs := f.IndexInfo.MetricIDs
	for i := 0; i < len(metricIDs); i++ {
		id := metricIDs[i]
		if set.Add(accountID, projectID, id) {
			metricIDs[i] = metricIDs[len(metricIDs)-1]
			metricIDs = metricIDs[:len(metricIDs)-1]
		}
	}
	return len(metricIDs) == 0
}

type FilterForDateAndMetricNameIndex struct {
	Dates     map[uint64]*metricidset.MetricIDSet
	MetricIDs []uint64
	AddCount  uint64
	SkipCount uint64
}

func NewFilterForDateAndMetricNameIndex() *FilterForDateAndMetricNameIndex {
	return &FilterForDateAndMetricNameIndex{
		Dates:     make(map[uint64]*metricidset.MetricIDSet, 100),
		MetricIDs: make([]uint64, 0, 100),
	}
}

func (f *FilterForDateAndMetricNameIndex) IsSkip(data []byte) bool {
	date := encoding.UnmarshalUint64(data[1:])
	data = data[9:]
	accountID := encoding.UnmarshalUint32(data[0:])
	projectID := encoding.UnmarshalUint32(data[4:])
	data = data[8:]
	idx := bytes.IndexByte(data, 1)
	if idx < 0 {
		logger.Panicf("date+metric index format error")
	}
	//metricGroupName := data[:idx]
	data = data[idx+1:]
	idx = bytes.IndexByte(data, 2)
	if idx < 0 {
		logger.Panicf("date+metric index format error, tag end not found")
	}
	data = data[idx+1:]
	if len(data) < 8 || len(data)%8 != 0 {
		logger.Panicf("date index format error")
	}
	set, ok := f.Dates[date]
	if !ok {
		set = metricidset.NewMetricIDSet()
		f.Dates[date] = set
	}
	f.MetricIDs = f.MetricIDs[:0]
	for i := 0; i < len(data); i += 8 {
		f.MetricIDs = append(f.MetricIDs, encoding.UnmarshalUint64(data[i:i+8]))
	}
	// 判断是否都在
	for i := 0; i < len(f.MetricIDs); i++ {
		id := f.MetricIDs[i]
		if set.Add(accountID, projectID, id) {
			f.MetricIDs[i] = f.MetricIDs[len(f.MetricIDs)-1]
			f.MetricIDs = f.MetricIDs[:len(f.MetricIDs)-1]
		}
	}
	return len(f.MetricIDs) == 0
}

// type FilterForMetricIDToNameIndex struct {
// 	MetricIDs *metricidset.MetricIDSet
// 	AddCount  uint64
// 	SkipCount uint64
// }

// func NewFilterForMetricIDToNameIndex() *FilterForMetricIDToNameIndex {
// 	return &FilterForMetricIDToNameIndex{
// 		MetricIDs: metricidset.NewMetricIDSet(),
// 	}
// }

// func (f *FilterForMetricIDToNameIndex) IsSkip(data []byte) bool {
// 	accountID := encoding.UnmarshalUint32(data[1:])
// 	projectID := encoding.UnmarshalUint32(data[5:])
// 	metricID := encoding.UnmarshalUint64(data[9:])
// 	return f.MetricIDs.Add(accountID, projectID, metricID)
// }
