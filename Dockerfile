FROM golang:alpine as build

RUN apk add git

WORKDIR /app
COPY go.mod go.sum /app/
RUN go mod download
COPY . .
RUN apk add alpine-sdk
RUN go build .

FROM alpine:3.10
RUN apk --no-cache add ca-certificates
COPY --from=build /app/errors-notifyer /bin/
ENTRYPOINT ["/bin/errors-notifyer"]
