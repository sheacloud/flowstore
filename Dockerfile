FROM golang:1.16-alpine as build

COPY . /build
RUN cd /build; CGO_ENABLED=1 GOBIN=/bin/ go install ./cmd/flowstore/;

FROM alpine as prod

COPY --from=build /bin/flowstore /bin/flowstore
COPY ./geoip-database/GeoLite2-City.mmdb /flowstore/geoip-database/

WORKDIR /flowstore/

CMD ["/bin/flowstore"]
