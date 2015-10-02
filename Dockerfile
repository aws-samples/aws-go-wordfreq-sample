
FROM ubuntu:12.04
FROM golang:1.5.1

ADD . /go/src/github.com/awslabs/go-wordfreq

RUN go get github.com/awslabs/go-wordfreq/cmd/worker/...
RUN go install github.com/awslabs/go-wordfreq/cmd/worker

EXPOSE 80

ENTRYPOINT /go/bin/worker

CMD ["/go/bin/worker"]