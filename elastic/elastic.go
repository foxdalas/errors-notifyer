package elastic

import (
	"bytes"
	"context"
	"errors"
	elastic "github.com/olivere/elastic/v7"
	"net/http"
	"strings"
	"syscall"
	"time"
)

const (
	layoutISO = "2006.01.02"
)

func New(elasticHost []string, index string) (*elasticSearch, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(elasticHost...),
		elastic.SetSniff(false),
		elastic.SetRetrier(NewEsRetrier()),
		elastic.SetHealthcheck(true),
		elastic.SetHealthcheckTimeout(time.Second*300),
	)
	if err != nil {
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)

	return &elasticSearch{
		Client: client,
		Ctx:    ctx,
		Index:  index,
	}, nil
}

func NewEsRetrier() *EsRetrier {
	return &EsRetrier{
		backoff: elastic.NewExponentialBackoff(10*time.Millisecond, 8*time.Second),
	}
}

func (e elasticSearch) GetKibanaIndex() (string, error) {
	res, err := e.Client.Aliases().Index("_all").Do(e.Ctx)
	if err != nil {
		return "", err
	}
	return res.IndicesByAlias(".kibana")[0], nil
}

func (e *elasticSearch) GetIndexPattern(index string) (string, error) {
	query := elastic.NewQueryStringQuery("index-pattern.title:"+e.Index).Escape(true)

	searchResult, err := e.Client.Search().
		Index(index).
		Query(query).
		Size(1).
		Pretty(true).
		Do(e.Ctx)

	return strings.Split(searchResult.Hits.Hits[0].Id, ":")[1], err
}

func (e *elasticSearch) searchResults(query *elastic.BoolQuery, aggregationString *elastic.TermsAggregation, aggregationName string, date string) (*elastic.SearchResult, error) {
	searchResult, err := e.Client.Search().
		Index(e.Index+"-"+date). // search in index
		Query(query).            // specify the query
		Size(0).
		Aggregation(aggregationName, aggregationString).
		Pretty(true).
		Do(e.Ctx)
	return searchResult, err
}

func (e *elasticSearch) searchResultsWeekAgo(query *elastic.BoolQuery, aggregationString *elastic.TermsAggregation, aggregationName string, date string) (*elastic.SearchResult, error) {
	searchResult, err := e.Client.Search().
		Index(e.Index+"-"+date). // search in index
		Query(query).            // specify the query
		Size(0).
		Aggregation(aggregationName, aggregationString).
		Pretty(true).
		Do(e.Ctx)
	return searchResult, err
}

func (e *elasticSearch) errorsAggregation(searchResult *elastic.SearchResult, stats Stats) Stats {
	error, found := searchResult.Aggregations.Terms("error")
	if found {
		for _, b := range error.Buckets {
			result := &Result{
				Error: Addslashes(b.Key.(string)),
				Count: b.DocCount,
			}
			stats.Results = append(stats.Results, result)
		}
	}
	return stats
}

func (e *elasticSearch) appsAggregationWithWeek(searchResult *elastic.SearchResult, searchResultWeekAgo *elastic.SearchResult, stats Stats) Stats {
	apps, found := searchResult.Aggregations.Terms("app")
	appsW, _ := searchResultWeekAgo.Aggregations.Terms("app")
	if found {
		for _, b := range apps.Buckets {
			count := int64(0)
			for _, bb := range appsW.Buckets {
				if b.Key.(string) == bb.Key.(string) {
					count = bb.DocCount
				}
			}

			result := &AppsStats{
				App:     Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: count,
			}
			stats.Apps = append(stats.Apps, result)
		}
	}
	return stats
}

func (e *elasticSearch) appsAggregationWithoutWeek(searchResult *elastic.SearchResult, stats Stats) Stats {
	apps, found := searchResult.Aggregations.Terms("app")
	if found {
		for _, b := range apps.Buckets {
			result := &AppsStats{
				App:     Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: int64(0),
			}
			stats.Apps = append(stats.Apps, result)
		}
	}

	return stats
}

func (e *elasticSearch) regionAggregationWithWeek(searchResult *elastic.SearchResult, searchResultWeekAgo *elastic.SearchResult, stats Stats) Stats {
	region, found := searchResult.Aggregations.Terms("region")
	regionW, _ := searchResultWeekAgo.Aggregations.Terms("region")
	if found {
		for _, b := range region.Buckets {
			count := int64(0)
			for _, bb := range regionW.Buckets {
				if b.Key.(string) == bb.Key.(string) {
					count = bb.DocCount
				}
			}
			result := &Region{
				Region:  Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: count,
			}
			stats.Region = append(stats.Region, result)
		}
	}
	return stats
}

func (e *elasticSearch) regionAggregationWithoutWeek(searchResult *elastic.SearchResult, stats Stats) Stats {
	region, found := searchResult.Aggregations.Terms("region")
	if found {
		for _, b := range region.Buckets {
			result := &Region{
				Region:  Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: int64(0),
			}
			stats.Region = append(stats.Region, result)
		}
	}
	return stats
}

func (e *elasticSearch) levelAggregationWithWeek(searchResult *elastic.SearchResult, searchResultWeekAgo *elastic.SearchResult, stats Stats) Stats {
	level, found := searchResult.Aggregations.Terms("level")
	levelW, _ := searchResultWeekAgo.Aggregations.Terms("level")
	if found {
		for _, b := range level.Buckets {
			count := int64(0)
			for _, bb := range levelW.Buckets {
				if b.Key.(string) == bb.Key.(string) {
					count = bb.DocCount
				}
			}
			result := &Level{
				Level:   Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: count,
			}
			stats.Levels = append(stats.Levels, result)
		}
	}
	return stats
}

func (e *elasticSearch) levelAggregationWithoutWeek(searchResult *elastic.SearchResult, stats Stats) Stats {
	level, found := searchResult.Aggregations.Terms("level")

	if found {
		for _, b := range level.Buckets {
			result := &Level{
				Level:   Addslashes(b.Key.(string)),
				Count:   b.DocCount,
				WeekAgo: int64(0),
			}
			stats.Levels = append(stats.Levels, result)
		}
	}
	return stats
}

func (e *elasticSearch) GetErrors(ctx context.Context, elasticClient *elastic.Client) (Stats, error) {
	var stats Stats
	yesterday := time.Now().AddDate(0, 0, -1).Format(layoutISO)
	weekAgo := time.Now().AddDate(0, 0, -7).Format(layoutISO)

	level := elastic.NewTermQuery("level", "error")
	dev := elastic.NewTermQuery("region", "dev")
	testing := elastic.NewTermQuery("region", "testing")

	aggregationName := "error"
	aggr := elastic.NewTermsAggregation().Field("message.keyword").Size(20)
	appsAggr := elastic.NewTermsAggregation().Field("app.keyword").Size(10)
	regionAggr := elastic.NewTermsAggregation().Field("region.keyword").Size(10)
	levelAggr := elastic.NewTermsAggregation().Field("level.keyword").Size(10)

	generalQ := elastic.NewBoolQuery()
	generalQ = generalQ.Must(level).MustNot(dev).MustNot(testing)

	searchResult, err := e.searchResults(generalQ, aggr, aggregationName, yesterday)
	if err != nil {
		return stats, err
	}

	stats = e.errorsAggregation(searchResult, stats)
	count, err := elasticClient.Count(e.Index + "-" + yesterday).Do(ctx)
	if err != nil {
		return stats, err
	}
	errors, err := elasticClient.Count(e.Index + "-" + yesterday).Query(generalQ).Do(ctx)
	if err != nil {
		return stats, err
	}
	stats.Total = count
	stats.Errors = errors
	stats.ErrorsPercent = (float64(errors) / float64(count)) * 100

	searchResult, err = e.searchResults(generalQ, appsAggr, "app", yesterday)
	if err != nil {
		return stats, err
	}
	searchResultWeekAgo, err := e.searchResultsWeekAgo(generalQ, appsAggr, "app", weekAgo)
	if err != nil {
		stats = e.appsAggregationWithoutWeek(searchResult, stats)
	} else {
		stats = e.appsAggregationWithWeek(searchResult, searchResultWeekAgo, stats)
	}
	searchResult, err = e.searchResults(generalQ, regionAggr, "region", yesterday)
	if err != nil {
		return stats, err
	}
	searchResultWeekAgo, err = e.searchResultsWeekAgo(generalQ, regionAggr, "region", weekAgo)
	if err != nil {
		stats = e.regionAggregationWithoutWeek(searchResult, stats)
	} else {
		stats = e.regionAggregationWithWeek(searchResult, searchResultWeekAgo, stats)

	}

	generalQ = elastic.NewBoolQuery()
	generalQ = generalQ.MustNot(dev).MustNot(testing)
	searchResult, err = e.searchResults(generalQ, levelAggr, "level", yesterday)
	if err != nil {
		return stats, err
	}
	searchResultWeekAgo, err = e.searchResults(generalQ, levelAggr, "level", weekAgo)
	if err != nil {
		stats = e.levelAggregationWithoutWeek(searchResult, stats)
	} else {
		stats = e.levelAggregationWithWeek(searchResult, searchResultWeekAgo, stats)
	}
	return stats, err
}

func (e *elasticSearch) GetWarnings(levelField string, ctx context.Context, elasticClient *elastic.Client) (Stats, error) {
	var stats Stats
	yesterday := time.Now().AddDate(0, 0, -1).Format(layoutISO)
	weekAgo := time.Now().AddDate(0, 0, -7).Format(layoutISO)

	level := elastic.NewTermQuery("level", levelField)
	dev := elastic.NewTermQuery("region", "dev")
	testing := elastic.NewTermQuery("region", "testing")

	aggregationName := "error"
	aggr := elastic.NewTermsAggregation().Field("message.keyword").Size(20)
	appsAggr := elastic.NewTermsAggregation().Field("app.keyword").Size(10)

	generalQ := elastic.NewBoolQuery()
	generalQ = generalQ.Must(level).MustNot(dev).MustNot(testing)

	searchResult, err := e.searchResults(generalQ, aggr, aggregationName, yesterday)
	if err != nil {
		return stats, err
	}

	stats = e.errorsAggregation(searchResult, stats)
	count, err := elasticClient.Count(e.Index + "-" + yesterday).Do(ctx)
	if err != nil {
		return stats, err
	}
	errors, err := elasticClient.Count(e.Index + "-" + yesterday).Query(generalQ).Do(ctx)
	if err != nil {
		return stats, err
	}
	stats.Total = count
	stats.Errors = errors
	stats.ErrorsPercent = (float64(errors) / float64(count)) * 100

	searchResult, err = e.searchResults(generalQ, appsAggr, "app", yesterday)
	if err != nil {
		return stats, err
	}
	searchResultWeekAgo, err := e.searchResultsWeekAgo(generalQ, appsAggr, "app", weekAgo)
	if err != nil {
		stats = e.appsAggregationWithoutWeek(searchResult, stats)
	} else {
		stats = e.appsAggregationWithWeek(searchResult, searchResultWeekAgo, stats)
	}

	return stats, err
}

func (r *EsRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("Elasticsearch or network down")
	}

	if retry >= 5 {
		return 0, false, nil
	}

	wait, stop := r.backoff.Next(retry)
	return wait, stop, nil
}

func Addslashes(str string) string {
	var buf bytes.Buffer
	for _, char := range str {
		switch char {
		case '\'':
			buf.WriteRune('\\')
		}
		buf.WriteRune(char)
	}
	return buf.String()
}
