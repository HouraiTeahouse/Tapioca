FROM znly/protoc as proto_builder
WORKDIR /proto
ADD . /proto
RUN mkdir /proto_go && protoc $(find . -type f -regex ".*\.proto") --go_out=/proto_go

FROM golang:1.11.2-alpine3.8 as builder
WORKDIR /go/src/github.com/HouraiTeahouse/Tapioca
ADD . /go/src/github.com/HouraiTeahouse/Tapioca
COPY --from=proto_builder /proto_go /go/src/github.com/HouraiTeahouse/Tapioca
WORKDIR /go/src/github.com/HouraiTeahouse/Tapioca/cmd/tapioca-server
RUN apk add --no-cache git upx && \
    go get -t ./... && \
    go build -ldflags="-s -w" -o /bin/goapp && \
    upx --ultra-brute /bin/goapp

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /bin/goapp .
CMD ["./goapp"]
