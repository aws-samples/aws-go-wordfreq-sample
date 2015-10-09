# Example Dockerfile if the service was going to be run in a docker
# container instead of a preconfigured Elastic Beanstalk Go platform.
FROM ubuntu:12.04
FROM golang:1.5.1

ADD . /go/src/github.com/awslabs/aws-go-wordfreq-sample

RUN go get github.com/awslabs/aws-go-wordfreq-sample/cmd/worker/...
RUN go install github.com/awslabs/aws-go-wordfreq-sample/cmd/worker

EXPOSE 80

ENTRYPOINT /go/bin/worker

CMD ["/go/bin/worker"]