package filter

import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"

type Filter struct {
	include filterset.FilterSet
	exclude filterset.FilterSet
}

func NewFilter(include, exclude FilterConfig) (*Filter, error) {
	var includeFilter filterset.FilterSet
	var excludeFilter filterset.FilterSet
	var err error
	if len(include.Metrics) > 0 {
		includeFilter, err = filterset.CreateFilterSet(include.Metrics, &include.Config)
		if err != nil {
			return nil, err
		}
	}
	if len(exclude.Metrics) > 0 {
		excludeFilter, err = filterset.CreateFilterSet(exclude.Metrics, &exclude.Config)
		if err != nil {
			return nil, err
		}
	}
	return &Filter{
		include: includeFilter,
		exclude: excludeFilter,
	}, nil
}

func (filter *Filter) Matches(name string) bool {
	if filter.exclude != nil && filter.exclude.Matches(name) {
		return false
	}
	if filter.include != nil && !filter.include.Matches(name) {
		return false
	}
	return true
}

type FilterConfig struct {
	filterset.Config `mapstructure:",squash"`
	Metrics          []string `mapstructure:"metrics"`
}
