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


func main() {
	client, err := elastic.New(strings.Split(os.Getenv("ELASTICSEARCH"), ","), os.Getenv("INDEX"))
	if err != nil {
		log.Fatal(err)
	}

	warningMode := false
	if os.Getenv("WARNING_MODE") == "true" {
		warningMode = true
	}

	data, err := client.GetErrors(client.Ctx, client.Client)
	if err != nil {
		log.Print(err)
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

	head := fmt.Sprintf("Вчера *%s* было залогировано сообщений\n*%d* всего\n", time.Now().AddDate(0, 0, -1).Format(layoutISO), data.Total)
	head += fmt.Sprintf("*%d* ошибок *(%.2f%%)*\n\n", data.Errors, data.ErrorsPercent)

	types := fmt.Sprintf("*Топ по типам событий:*\n")
	for _, level := range data.Levels {
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'level:" + url.QueryEscape(level.Level) + "%20AND%20NOT%20region:dev%20AND%20NOT%20region:testing'),sort:!(!('@timestamp',desc)))")
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
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'region:%20\"" + url.QueryEscape(dc.Region) + "\"%20AND%20level:error'),sort:!(!('@timestamp',desc)))")
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
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'app:%20\"" + url.QueryEscape(rs.App) + "\"%20AND%20level:error'),sort:!(!('@timestamp',desc)))")
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
			kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'message:%20\"" + url.QueryEscape(rs.Error) + "\"%20AND%20level:%20\"warning\"%20AND%20NOT%20region:%20\"dev\"%20AND%20NOT%20region:%20\"testing\"'),sort:!(!('@timestamp',desc)))")
			warn += fmt.Sprintf("*%s* предупреждений <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
		}
	}

	head += "\n\n"
	topTypes := fmt.Sprintf("*Top 10 типов ошибок*\n")
	for id, rs := range data.Results {
		if id >= 9 {
			continue
		}
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'message:%20\"" + url.QueryEscape(rs.Error) + "\"%20AND%20level:%20\"error\"%20AND%20NOT%20region:%20\"dev\"%20AND%20NOT%20region:%20\"testing\"'),sort:!(!('@timestamp',desc)))")
		topTypes += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
	}

	api := slack.New(os.Getenv("SLACK"), slack.OptionDebug(true))
	div := slack.NewDividerBlock()

	headerSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", head, false, false)}...
	)
	typeSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", types, false, false)}...
	)
	datacentersSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", datacenters, false, false)}...
	)
	appsSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", apps, false, false)}...
	)
	topTypesSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", topTypes, false, false)}...
	)
	warnSection := slack.NewContextBlock(
		"",
		[]slack.MixedElement{slack.NewTextBlockObject("mrkdwn", warn, false, false)}...
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
