package main

import (
	"fmt"
	"github.com/foxdalas/errors-notifyer/elastic"
	"github.com/slack-go/slack"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

func formatKibanaUrl(date string, index string, query string) string {
	var result string

	kibanaUrl := os.Getenv("KIBANA")

	path := fmt.Sprintf("%s/app/data-explorer/discover/#", kibanaUrl)
	timeRange := fmt.Sprintf("time:(from:'%sT00:00:00.000Z',to:'%sT23:59:59.000Z')", date, date)

	timeParameters := fmt.Sprintf("(filters:!(),refreshInterval:(pause:!t,value:0),%s)", timeRange)                                                                             //_g=
	queryParameters := fmt.Sprintf("(query:(language:kuery,query:'%s'))", url.PathEscape(query))                                                                                //_q=
	discoverParameters := fmt.Sprintf("(discover:(columns:!(app,message,error),interval:auto,sort:!(!('@timestamp',desc))),metadata:(indexPattern:'%s',view:discover))", index) // _a=

	result = fmt.Sprintf("%s?_a=%s&_q=%s&_g=%s", path, discoverParameters, queryParameters, timeParameters)

	return result
}

func main() {
	index := os.Getenv("INDEX")
	elasticHosts := strings.Split(os.Getenv("ELASTICSEARCH"), ",")

	client, err := elastic.New(elasticHosts, index)
	if err != nil {
		log.Fatal(err)
	}

	warningMode := false
	if os.Getenv("WARNING_MODE") == "true" {
		warningMode = true
	}

	data, err := client.GetErrors(client.Ctx, client.Client)
	if err != nil {
		log.Fatal(err)
	}

	kibanaIndexName, err := client.GetKibanaIndex()
	if err != nil {
		log.Fatal(err)
	}

	kibanaIndex, err := client.GetIndexPattern(kibanaIndexName)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Kibana index name: %s", kibanaIndexName)
	log.Printf("Kibana index pattern %s", kibanaIndex)

	layoutISO := "2006-01-02"
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	head := fmt.Sprintf(
		"Вчера *%s* в индексе *%s* было залогировано сообщений\n*%d* всего\n",
		time.Now().AddDate(0, 0, -1).Format(layoutISO),
		index,
		data.Total,
	)
	head += fmt.Sprintf("*%d* ошибок *(%.2f%%)*\n\n", data.Errors, data.ErrorsPercent)

	types := fmt.Sprintf("*Топ по типам событий:*\n")
	for _, level := range data.Levels {
		kibanaUrl := formatKibanaUrl(yesterday, kibanaIndex, fmt.Sprintf("level:\"%s\" and not region:\"dev\" and not region:\"testing\" and not region:\"ap-south-1\"", level.Level))
		str := fmt.Sprintf("*%s* <%s|*%d*>", level.Level, kibanaUrl, level.Count)
		if level.DayBeforeYesterday > 0 {
			diff := ((float64(level.Count) - float64(level.DayBeforeYesterday)) / float64(level.DayBeforeYesterday)) * 100
			str += fmt.Sprintf(" *(D:%.2f%%)*", diff)
		}
		if level.WeekAgo > 0 {
			diff := ((float64(level.Count) - float64(level.WeekAgo)) / float64(level.WeekAgo)) * 100
			str += fmt.Sprintf(" *(W:%.2f%%)*", diff)
		}
		str += "\n"
		types += str
	}

	datacenters := fmt.Sprintf("*Ошибок по дата-центрам:*\n")
	for _, dc := range data.Region {
		kibanaUrl := formatKibanaUrl(yesterday, kibanaIndex, fmt.Sprintf("region:\"%s\" and level:\"error\"", dc.Region))
		str := fmt.Sprintf("*%s* ошибок <%s|*%d*>", dc.Region, kibanaUrl, dc.Count)
		if dc.DayBeforeYesterday > 0 {
			diff := ((float64(dc.Count) - float64(dc.DayBeforeYesterday)) / float64(dc.DayBeforeYesterday)) * 100
			str += fmt.Sprintf(" *(D:%.2f%%)*", diff)
		}
		if dc.WeekAgo > 0 {
			diff := ((float64(dc.Count) - float64(dc.WeekAgo)) / float64(dc.WeekAgo)) * 100
			str += fmt.Sprintf(" *(W:%.2f%%)*", diff)
		}
		str += "\n"
		datacenters += str
	}

	apps := fmt.Sprintf("*Топ 10 приложений*\n")
	for id, rs := range data.Apps {
		if id >= 9 {
			continue
		}
		kibanaUrl := formatKibanaUrl(yesterday, kibanaIndex, fmt.Sprintf("app:\"%s\" and level:\"error\" and not region:\"dev\" and not region:\"testing\" and not region:\"ap-south-1\"", rs.App))
		str := fmt.Sprintf("*%s* ошибок <%s|*%d*>", rs.App, kibanaUrl, rs.Count)
		if rs.DayBeforeYesterday > 0 {
			diff := ((float64(rs.Count) - float64(rs.DayBeforeYesterday)) / float64(rs.DayBeforeYesterday)) * 100
			str += fmt.Sprintf(" *(D:%.2f%%)*", diff)
		}
		if rs.WeekAgo > 0 {
			diff := ((float64(rs.Count) - float64(rs.WeekAgo)) / float64(rs.WeekAgo)) * 100
			str += fmt.Sprintf(" *(W:%.2f%%)*", diff)
		}
		str += "\n"
		apps += str
	}

	var warn string
	if warningMode {
		warnings, err := client.GetWarnings("warning", client.Ctx, client.Client)
		if err != nil {
			log.Print(err)
		}

		head += "\n\n"
		warn = fmt.Sprintf("*Топ 10 типов предупреждений*\n")
		for id, rs := range warnings.Results {
			if id >= 9 {
				continue
			}
			kibanaUrl := formatKibanaUrl(yesterday, kibanaIndex, fmt.Sprintf("message:\"%s\" and level:\"warning\" and not region:\"dev\" and not region:\"testing\" and not region:\"ap-south-1\"", rs.Error))
			warn += fmt.Sprintf("*%s* предупреждений <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
		}
	}

	head += "\n\n"
	topTypes := fmt.Sprintf("*Top 10 типов ошибок*\n")
	for id, rs := range data.Results {
		if id >= 9 {
			continue
		}
		kibanaUrl := formatKibanaUrl(yesterday, kibanaIndex, fmt.Sprintf("message:\"%s\" AND level:\"error\" and not region:\"dev\" and not region:\"testing\" and not region:\"ap-south-1\"", rs.Error))
		topTypes += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
	}

	api := slack.New(os.Getenv("SLACK"), slack.OptionDebug(true))
	div := slack.NewDividerBlock()

	headerSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", head, false, false)}...,
	)
	typeSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", types, false, false)}...,
	)
	datacentersSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", datacenters, false, false)}...,
	)
	appsSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", apps, false, false)}...,
	)
	topTypesSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", topTypes, false, false)}...,
	)
	warnSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", warn, false, false)}...,
	)

	if warningMode {
		channelID, timestamp, err := api.PostMessage(os.Getenv("CHANNEL"),
			slack.MsgOptionUsername("Максим"),
			slack.MsgOptionBlocks(headerSection, div, typeSection, div, datacentersSection, div, appsSection, div, warnSection, div, topTypesSection),
		)
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		fmt.Printf("Message successfully sent to channel %s at %s", channelID, timestamp)
	} else {
		channelID, timestamp, err := api.PostMessage(os.Getenv("CHANNEL"),
			slack.MsgOptionUsername("Максим"),
			slack.MsgOptionText("fish", false),
			slack.MsgOptionBlocks(headerSection, div, typeSection, div, datacentersSection, div, appsSection, div, topTypesSection),
		)
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
		fmt.Printf("Message successfully sent to channel %s at %s", channelID, timestamp)
	}

	//payload := make(map[string]interface{})
	//payload["channel"] = os.Getenv("CHANNEL")
	//payload["text"] = head
	//payload["username"] = "Максим"
	//payload["mrkdwn"] = false
	//
	//d, err := json.Marshal(payload)
	//if err != nil {
	//	log.Fatalf("error on encode request, %v", err)
	//}
	//
	//_, _, errors := gorequest.New().Post(os.Getenv("SLACK")).Send(string(d)).End()
	//if len(errors) > 0 {
	//	log.Fatalf("error on send request, %#v", errors)
	//}
}
