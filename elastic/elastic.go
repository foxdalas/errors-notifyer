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

func New(elasticHost []string, index string, kibanaIndex string) (*elasticSearch, error) {
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
		Client:      client,
		Ctx:         ctx,
		Index:       index,
		KibanaIndex: kibanaIndex,
	}, nil
}

func NewEsRetrier() *EsRetrier {
	return &EsRetrier{
		backoff: elastic.NewExponentialBackoff(10*time.Millisecond, 8*time.Second),
	}
}

func (e *elasticSearch) GetIndexPattern(index string) (string, error) {
	query := elastic.NewBoolQuery()
	query = query.Must(elastic.NewTermQuery("index-pattern.title", e.Index))

	searchResult, err := e.Client.Search().
		Index(e.KibanaIndex).
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

func (e *elasticSearch) appsAggregation(searchResult *elastic.SearchResult, stats Stats) Stats {

	apps, found := searchResult.Aggregations.Terms("app")
	if found {
		for _, b := range apps.Buckets {
			result := &AppsStats{
				App:   Addslashes(b.Key.(string)),
				Count: b.DocCount,
			}
			stats.Apps = append(stats.Apps, result)
		}
	}
	return stats
}

func (e *elasticSearch) regionAggregation(searchResult *elastic.SearchResult, stats Stats) Stats {

	apps, found := searchResult.Aggregations.Terms("region")
	if found {
		for _, b := range apps.Buckets {
			result := &Region{
				Region: Addslashes(b.Key.(string)),
				Count:  b.DocCount,
			}
			stats.Region = append(stats.Region, result)
		}
	}
	return stats
}

func (e *elasticSearch) GetErrors(ctx context.Context, elasticClient *elastic.Client) (Stats, error) {
	var stats Stats
	yesterday := time.Now().AddDate(0, 0, -1).Format(layoutISO)

	level := elastic.NewTermQuery("level", "error")
	dev := elastic.NewTermQuery("region", "dev")
	aggregationName := "error"
	aggr := elastic.NewTermsAggregation().Field("message.keyword").Size(20)
	appsAggr := elastic.NewTermsAggregation().Field("app.keyword").Size(10)
	regionAggr := elastic.NewTermsAggregation().Field("region.keyword").Size(10)

	generalQ := elastic.NewBoolQuery()
	generalQ = generalQ.Must(level).MustNot(dev)

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
	stats = e.appsAggregation(searchResult, stats)

	searchResult, err = e.searchResults(generalQ, regionAggr, "region", yesterday)
	if err != nil {
		return stats, err
	}
	stats = e.regionAggregation(searchResult, stats)

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
