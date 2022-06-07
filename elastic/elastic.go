package elastic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	ctx, _ := context.WithTimeout(context.Background(), 300*time.Second)

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
	sortQuery := elastic.NewFieldSort("updated_at").Desc()
	query := elastic.NewBoolQuery().Must(elastic.NewQueryStringQuery(fmt.Sprintf("index-pattern.title:\"%s-*\"", e.Index)))
	searchResult, err := e.Client.Search().
		Index(index).
		Query(query).
		Size(10).
		Pretty(true).
		SortBy(sortQuery).
		Do(e.Ctx)

	if searchResult.TotalHits() > 0 {
		return strings.Split(searchResult.Hits.Hits[0].Id, ":")[1], err
	}

	return "", errors.New("pattern not found")
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

func (e *elasticSearch) errorsAggregation(generalQ *elastic.BoolQuery, dates *Dates, stats Stats) Stats {
	aggr := elastic.NewTermsAggregation().Field("message.keyword").Size(20)
	searchResult, _ := e.searchResults(generalQ, aggr, "error", dates.Yesterday)

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

func (e *elasticSearch) appsAggregation(generalQ *elastic.BoolQuery, dates *Dates, stats Stats) Stats {
	appsAggr := elastic.NewTermsAggregation().Field("app.keyword").Size(10)
	searchResult, err := e.searchResults(generalQ, appsAggr, "app", dates.Yesterday)
	if err != nil {
		return stats
	}
	searchResultDayBeforeYesterday, errD := e.searchResults(generalQ, appsAggr, "app", dates.DayBeforeYesterday)
	searchResultWeekAgo, errW := e.searchResults(generalQ, appsAggr, "app", dates.WeekAgo)

	count := int64(0)
	countD := int64(0)

	apps, found := searchResult.Aggregations.Terms("app")

	if found {
		var foundD bool
		var foundW bool
		var appsD *elastic.AggregationBucketKeyItems
		var appsW *elastic.AggregationBucketKeyItems

		if errD == nil {
			appsD, foundD = searchResultDayBeforeYesterday.Aggregations.Terms("app")
		}
		if errW == nil {
			appsW, foundW = searchResultWeekAgo.Aggregations.Terms("app")
		}
		for _, b := range apps.Buckets {
			if foundD {
				for _, bb := range appsD.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						countD = bb.DocCount
					}
				}
			}
			if foundW {
				for _, bb := range appsW.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						count = bb.DocCount
					}
				}
			}
			result := &AppsStats{
				App:                Addslashes(b.Key.(string)),
				Count:              b.DocCount,
				DayBeforeYesterday: countD,
				WeekAgo:            count,
			}
			stats.Apps = append(stats.Apps, result)
		}
	}
	return stats
}

func (e *elasticSearch) regionAggregation(generalQ *elastic.BoolQuery, dates *Dates, stats Stats) Stats {
	regionAggr := elastic.NewTermsAggregation().Field("region.keyword").Size(10)
	searchResult, err := e.searchResults(generalQ, regionAggr, "region", dates.Yesterday)
	if err != nil {
		return stats
	}
	searchResultDayBeforeYesterday, errD := e.searchResults(generalQ, regionAggr, "region", dates.DayBeforeYesterday)
	searchResultWeekAgo, errW := e.searchResults(generalQ, regionAggr, "region", dates.WeekAgo)

	region, found := searchResult.Aggregations.Terms("region")

	if found {
		var foundD bool
		var foundW bool
		var regionD *elastic.AggregationBucketKeyItems
		var regionW *elastic.AggregationBucketKeyItems

		if errD == nil {
			regionD, foundD = searchResultDayBeforeYesterday.Aggregations.Terms("region")
		}
		if errW == nil {
			regionW, foundW = searchResultWeekAgo.Aggregations.Terms("region")
		}

		for _, b := range region.Buckets {
			count := int64(0)
			countD := int64(0)

			if foundD {
				for _, bb := range regionD.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						countD = bb.DocCount
					}
				}
			}
			if foundW {
				for _, bb := range regionW.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						count = bb.DocCount
					}
				}
			}

			result := &Region{
				Region:             Addslashes(b.Key.(string)),
				Count:              b.DocCount,
				DayBeforeYesterday: countD,
				WeekAgo:            count,
			}
			stats.Region = append(stats.Region, result)
		}
	}
	return stats
}

func (e *elasticSearch) levelAggregation(generalQ *elastic.BoolQuery, dates *Dates, stats Stats) Stats {
	levelAggr := elastic.NewTermsAggregation().Field("level.keyword").Size(10)

	searchResult, err := e.searchResults(generalQ, levelAggr, "level", dates.Yesterday)
	if err != nil {
		return stats
	}
	searchResultDayBeforeYesterday, errD := e.searchResults(generalQ, levelAggr, "level", dates.DayBeforeYesterday)
	searchResultWeekAgo, errW := e.searchResults(generalQ, levelAggr, "level", dates.WeekAgo)

	level, found := searchResult.Aggregations.Terms("level")

	if found {
		var foundD bool
		var foundW bool
		var levelD *elastic.AggregationBucketKeyItems
		var levelW *elastic.AggregationBucketKeyItems

		if errD == nil {
			levelD, foundD = searchResultDayBeforeYesterday.Aggregations.Terms("level")
		}

		if errW == nil {
			levelW, foundW = searchResultWeekAgo.Aggregations.Terms("level")
		}

		for _, b := range level.Buckets {
			count := int64(0)
			countD := int64(0)

			if foundD {
				for _, bb := range levelD.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						countD = bb.DocCount
					}
				}
			}
			if foundW {
				for _, bb := range levelW.Buckets {
					if b.Key.(string) == bb.Key.(string) {
						count = bb.DocCount
					}
				}
			}

			result := &Level{
				Level:              Addslashes(b.Key.(string)),
				Count:              b.DocCount,
				WeekAgo:            count,
				DayBeforeYesterday: countD,
			}
			stats.Levels = append(stats.Levels, result)
		}
	}
	return stats
}

func (e *elasticSearch) GetErrors(ctx context.Context, elasticClient *elastic.Client) (Stats, error) {
	var stats Stats
	dates := &Dates{
		Yesterday:          time.Now().AddDate(0, 0, -1).Format(layoutISO),
		DayBeforeYesterday: time.Now().AddDate(0, 0, -2).Format(layoutISO),
		WeekAgo:            time.Now().AddDate(0, 0, -7).Format(layoutISO),
	}

	level := elastic.NewTermQuery("level", "error")
	dev := elastic.NewTermQuery("region", "dev")
	testing := elastic.NewTermQuery("region", "testing")

	generalQ := elastic.NewBoolQuery()
	generalQ = generalQ.Must(level).MustNot(dev).MustNot(testing)
	stats = e.errorsAggregation(generalQ, dates, stats)

	count, err := elasticClient.Count(e.Index + "-" + dates.Yesterday).Do(ctx)
	if err != nil {
		return stats, err
	}
	errors, err := elasticClient.Count(e.Index + "-" + dates.Yesterday).Query(generalQ).Do(ctx)
	if err != nil {
		return stats, err
	}
	stats.Total = count
	stats.Errors = errors
	stats.ErrorsPercent = (float64(errors) / float64(count)) * 100

	stats = e.appsAggregation(generalQ, dates, stats)
	stats = e.regionAggregation(generalQ, dates, stats)

	generalQ = elastic.NewBoolQuery().MustNot(dev).MustNot(testing)
	stats = e.levelAggregation(generalQ, dates, stats)

	return stats, err
}

func (e *elasticSearch) GetWarnings(levelField string, ctx context.Context, elasticClient *elastic.Client) (Stats, error) {
	var stats Stats

	dates := &Dates{
		Yesterday:          time.Now().AddDate(0, 0, -1).Format(layoutISO),
		WeekAgo:            time.Now().AddDate(0, 0, -7).Format(layoutISO),
		DayBeforeYesterday: time.Now().AddDate(0, 0, -2).Format(layoutISO),
	}

	level := elastic.NewTermQuery("level", levelField)
	dev := elastic.NewTermQuery("region", "dev")
	testing := elastic.NewTermQuery("region", "testing")

	generalQ := elastic.NewBoolQuery()
	generalQ = generalQ.Must(level).MustNot(dev).MustNot(testing)

	stats = e.errorsAggregation(generalQ, dates, stats)
	count, err := elasticClient.Count(e.Index + "-" + dates.Yesterday).Do(ctx)
	if err != nil {
		return stats, err
	}
	errors, err := elasticClient.Count(e.Index + "-" + dates.Yesterday).Query(generalQ).Do(ctx)
	if err != nil {
		return stats, err
	}
	stats.Total = count
	stats.Errors = errors
	stats.ErrorsPercent = (float64(errors) / float64(count)) * 100

	stats = e.appsAggregation(generalQ, dates, stats)

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
