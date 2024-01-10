package metricidset

import "github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"

type MetricIDSet struct {
	Tenants       map[uint32]map[uint32]*uint64set.Set
	DefaultTenant uint64set.Set
}

func NewMetricIDSet() *MetricIDSet {
	return &MetricIDSet{
		Tenants: make(map[uint32]map[uint32]*uint64set.Set),
	}
}

func (c *MetricIDSet) Add(accountID, projectID uint32, metricID uint64) (exists bool) {
	if accountID == 0 && projectID == 0 {
		exists = c.DefaultTenant.Has(metricID)
		c.DefaultTenant.Add(metricID)
		return
	}
	projects, ok := c.Tenants[accountID]
	if !ok {
		projects = make(map[uint32]*uint64set.Set)
		c.Tenants[accountID] = projects
	}
	s, ok := projects[projectID]
	if !ok {
		s = &uint64set.Set{}
		projects[projectID] = s
	}
	exists = s.Has(metricID)
	s.Add(metricID)
	return
}

func (c *MetricIDSet) Has(accountID, projectID uint32, metricID uint64) (exists bool) {
	if accountID == 0 && projectID == 0 {
		exists = c.DefaultTenant.Has(metricID)
		return
	}
	projects, ok := c.Tenants[accountID]
	if !ok {
		exists = false
		return
	}
	s, ok := projects[projectID]
	if !ok {
		exists = false
		return
	}
	exists = s.Has(metricID)
	return
}
