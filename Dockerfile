FROM alpine as base
ENV PORT 80
EXPOSE 80

FROM znly/protoc as proto_builder
WORKDIR /proto
ADD . /proto
RUN mkdir /proto_go && protoc $(find . -type f -regex ".*\.proto") \
  --go_out=plugins=grpc:/proto_go

FROM golang:1.11.2-alpine3.8 as builder
RUN apk add --no-cache git upx build-base
WORKDIR /go/src/github.com/HouraiTeahouse/Tapioca
ADD . /go/src/github.com/HouraiTeahouse/Tapioca
COPY --from=proto_builder /proto_go /go/src/github.com/HouraiTeahouse/Tapioca
WORKDIR /go/src/github.com/HouraiTeahouse/Tapioca/cmd/tapioca-server
RUN go get -t ./... && \
    go build -ldflags="-s -w" -o /bin/tapioca-server && \
    upx /bin/tapioca-server

FROM base
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /bin/tapioca-server /bin/tapioca-server
CMD ["/bin/tapioca-server"]
