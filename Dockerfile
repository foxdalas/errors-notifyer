FROM golang:1.21-alpine as build

RUN apk add git
RUN apk add alpine-sdk

WORKDIR /app
COPY go.mod go.sum /app/
RUN go mod download
COPY . .
RUN go build .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=build /app/errors-notifyer /bin/
ENTRYPOINT ["/bin/errors-notifyer"]
