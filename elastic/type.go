package elastic

import (
	"github.com/olivere/elastic/v7"
	"golang.org/x/net/context"
)

type elasticSearch struct {
	Ctx         context.Context
	Client      *elastic.Client
	Index       string
	KibanaIndex string
}

type EsRetrier struct {
	backoff elastic.Backoff
}

type Stats struct {
	Total         int64
	Errors        int64
	ErrorsPercent float64
	Results       []*Result
	Apps          []*AppsStats
	Region        []*Region
}

type Result struct {
	Error string
	Count int64
}

type AppsStats struct {
	App   string
	Count int64
}

type Region struct {
	Region string
	Count  int64
}