FROM golang:1.20-alpine AS build
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /storage ./server
FROM alpine:3.18
RUN mkdir /data
VOLUME ["/data"]
COPY --from=build /storage /storage
EXPOSE 9000
ENTRYPOINT ["/storage", "-data", "/data"]
