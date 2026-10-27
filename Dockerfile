FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/objstored /out/objctl /usr/local/bin/
VOLUME /data
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/objstored"]
CMD ["--data-dir=/data", "--listen=:9000"]
