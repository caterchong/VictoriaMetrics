package metricidset

import "github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"

type MetricIDSet struct {
	Tenants       map[uint32]map[uint32]*uint64set.Set
	DefaultTenant uint64set.Set
}

func NewMetricIDSet() *MetricIDSet {
	inst := &MetricIDSet{
		Tenants: make(map[uint32]map[uint32]*uint64set.Set),
	}
	return inst
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

func (c *MetricIDSet) Len() int {
	l := c.DefaultTenant.Len()
	for _, item := range c.Tenants {
		for _, m := range item {
			l += m.Len()
		}
	}
	return l
}

func (c *MetricIDSet) UnionMayOwn(from *MetricIDSet) {
	c.DefaultTenant.UnionMayOwn(&from.DefaultTenant)
	for accountID, projects := range from.Tenants {
		targetProjects, has := c.Tenants[accountID]
		if !has {
			c.Tenants[accountID] = projects
			from.Tenants[accountID] = nil
			continue
		}
		for projectID, set := range projects {
			targetSet, has := targetProjects[projectID]
			if !has {
				targetProjects[projectID] = set
				projects[projectID] = nil
				continue
			}
			targetSet.UnionMayOwn(set)
		}
	}
}

func CountSameMetricID(a *MetricIDSet, b *MetricIDSet) uint64 {
	var same uint64
	a.DefaultTenant.ForEach(func(part []uint64) bool {
		for _, metricID := range part {
			if b.DefaultTenant.Has(metricID) {
				same++
			}
		}
		return true
	})
	//
	for accountID, m1 := range a.Tenants {
		projectsOfB, has := b.Tenants[accountID]
		if !has {
			continue
		}
		for projectID, setOfA := range m1 {
			setOfB, hasProject := projectsOfB[projectID]
			if !hasProject {
				continue
			}
			setOfA.ForEach(func(part []uint64) bool {
				for _, metricID := range part {
					if setOfB.Has(metricID) {
						same++
					}
				}
				return true
			})
		}
	}
	return same
}

// Is there any intersection?
func HaveIntersection(a *MetricIDSet, b *MetricIDSet) bool {
	a.DefaultTenant.ForEach(func(part []uint64) bool {
		for _, metricID := range part {
			if b.DefaultTenant.Has(metricID) {
				return true
			}
		}
		return true
	})
	//
	for accountID, m1 := range a.Tenants {
		projectsOfB, has := b.Tenants[accountID]
		if !has {
			continue
		}
		for projectID, setOfA := range m1 {
			setOfB, hasProject := projectsOfB[projectID]
			if !hasProject {
				continue
			}
			setOfA.ForEach(func(part []uint64) bool {
				for _, metricID := range part {
					if setOfB.Has(metricID) {
						return true
					}
				}
				return true
			})
		}
	}
	return false
}
