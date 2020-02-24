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
	client, err := elastic.New(strings.Split(os.Getenv("ELASTICSEARCH"), ","), os.Getenv("INDEX"), os.Getenv("KIBANA_INDEX"))
	if err != nil {
		log.Fatal(err)
	}

	data, err := client.GetErrors(client.Ctx, client.Client)
	if err != nil {
		log.Fatal(err)
	}

	kibanaIndex, err := client.GetIndexPattern(os.Getenv("INDEX"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(kibanaIndex)

	layoutISO := "2006-01-02"
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	head := fmt.Sprintf("Вчера *%s* было залогировано сообщений\n*%d* всего\n", time.Now().AddDate(0, 0, -1).Format(layoutISO), data.Total)
	head += fmt.Sprintf("*%d* ошибок *(%.2f%%)*\n\n", data.Errors, data.ErrorsPercent)

	head += fmt.Sprintf("Ошибок по дата-центрам:\n")
	for _, dc := range data.Region {
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error,region),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'region:%20\"" + url.QueryEscape(dc.Region) + "\"%20AND%20level:%20\"error\"'),sort:!(!('@timestamp',desc)))")
		head += fmt.Sprintf("*%s* ошибок <%s|*%d*>\n", dc.Region, kibanaUrl, dc.Count)
	}

	head += fmt.Sprintf("\n\nТоп 10 приложений\n")
	for id, rs := range data.Apps {
		if id >= 9 {
			continue
		}
		head += fmt.Sprintf("*%s* ошибок *%d*\n", rs.App, rs.Count)
	}

	head += "\n\n"

	head += fmt.Sprintf("Top 10 типов ошибок\n")
	for id, rs := range data.Results {
		if id >= 9 {
			continue
		}
		kibanaUrl := fmt.Sprint(os.Getenv("KIBANA") + "/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0),time:(from:'" + yesterday + "T00:00:00.000Z',to:'" + yesterday + "T23:59:59.000Z'))&_a=(columns:!(app,message,error),index:'" + kibanaIndex + "',interval:auto,query:(language:kuery,query:'message:%20\"" + url.QueryEscape(rs.Error) + "\"%20AND%20level:%20\"error\"%20AND%20NOT%20region:%20\"dev\"'),sort:!(!('@timestamp',desc)))")
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
