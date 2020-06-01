package main

import (
	"encoding/json"
	"fmt"
	"github.com/foxdalas/errors-notifyer/elastic"
	"github.com/parnurzeal/gorequest"
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

	head += fmt.Sprintf("Топ по типам событий:\n")
	for _, level := range data.Levels {
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'level:" + url.QueryEscape(level.Level) + "%20AND%20NOT%20region:dev%20AND%20NOT%20region:testing'),sort:!(!('@timestamp',desc)))")
		if level.WeekAgo == 0 {
			head += fmt.Sprintf("*%s* <%s|*%d*>\n", level.Level, kibanaUrl, level.Count)
		} else {
			diff := ((float64(level.Count) - float64(level.WeekAgo)) / float64(level.WeekAgo)) * 100
			head += fmt.Sprintf("*%s* <%s|*%d*> *(%.2f%%)*\n", level.Level, kibanaUrl, level.Count, diff)
		}
	}

	head += fmt.Sprintf("\nОшибок по дата-центрам:\n")
	for _, dc := range data.Region {
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'region:%20\"" + url.QueryEscape(dc.Region) + "\"%20AND%20level:error'),sort:!(!('@timestamp',desc)))")
		if dc.WeekAgo == 0 {
			head += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", dc.Region, kibanaUrl, dc.Count)
		} else {
			diff := ((float64(dc.Count) - float64(dc.WeekAgo)) / float64(dc.WeekAgo)) * 100
			head += fmt.Sprintf("*%s* ошибок <%s|*%d*> *(%.2f%%)*\n", dc.Region, kibanaUrl, dc.Count, diff)
		}
	}

	head += fmt.Sprintf("\n\nТоп 10 приложений\n")
	for id, rs := range data.Apps {
		if id >= 9 {
			continue
		}
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'app:%20\"" + url.QueryEscape(rs.App) + "\"%20AND%20level:error'),sort:!(!('@timestamp',desc)))")
		if rs.WeekAgo == 0 {
			head += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", rs.App, kibanaUrl, rs.Count)
		} else {
			diff := ((float64(rs.Count) - float64(rs.WeekAgo)) / float64(rs.WeekAgo)) * 100
			head += fmt.Sprintf("*%s* ошибок <%s|*%d*> *(%.2f%%)*\n", rs.App, kibanaUrl, rs.Count, diff)
		}
	}

	if warningMode {
		warnings, err := client.GetWarnings("warning", client.Ctx, client.Client)
		if err != nil {
			log.Print(err)
		}

		head += "\n\n"
		head += fmt.Sprintf("Топ 10 типов предупреждений\n")
		for id, rs := range warnings.Results {
			if id >= 9 {
				continue
			}
			kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'message:%20\"" + url.QueryEscape(rs.Error) + "\"%20AND%20level:%20\"warning\"%20AND%20NOT%20region:%20\"dev\"%20AND%20NOT%20region:%20\"testing\"'),sort:!(!('@timestamp',desc)))")
			head += fmt.Sprintf("*%s* предупреждений <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
		}
	}

	head += "\n\n"
	head += fmt.Sprintf("Top 10 типов ошибок\n")
	for id, rs := range data.Results {
		if id >= 9 {
			continue
		}
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'message:%20\"" + url.QueryEscape(rs.Error) + "\"%20AND%20level:%20\"error\"%20AND%20NOT%20region:%20\"dev\"%20AND%20NOT%20region:%20\"testing\"'),sort:!(!('@timestamp',desc)))")
		head += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", rs.Error, kibanaUrl, rs.Count)
	}

	payload := make(map[string]interface{})
	payload["channel"] = os.Getenv("CHANNEL")
	payload["text"] = head
	payload["username"] = "Максим"
	payload["mrkdwn"] = true

	d, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("error on encode request, %v", err)
	}

	_, _, errors := gorequest.New().Post(os.Getenv("SLACK")).Send(string(d)).End()
	if len(errors) > 0 {
		log.Fatalf("error on send request, %#v", errors)
	}
}
