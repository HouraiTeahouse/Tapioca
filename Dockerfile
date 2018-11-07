FROM golang:1.11.2-alpine3.8 as builder
ADD . /go/src/github.com/HouraiTeahouse/Tapioca
RUN cd /go/src/github.com/HouraiTeahouse/Tapioca/cmd/tapioca_manifest_gen && \
  go build -o /bin/goapp

FROM alpine
WORKDIR /app
COPY --from=builder /bin/goapp /app
ENTRYPOINT ./goapp
