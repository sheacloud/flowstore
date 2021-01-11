FROM golang:1.15-alpine as build

COPY . /build
RUN cd /build; CGO_ENABLED=1 GOBIN=/bin/ go install ./cmd/flowstore/;

FROM alpine as prod

COPY --from=build /bin/flowstore /bin/flowstore

CMD ["/bin/flowstore"]
