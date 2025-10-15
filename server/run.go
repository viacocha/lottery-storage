package main

import (
    "flag"
    "log"
    "net/http"
)

func main() {
    listen := flag.String("listen",":9000","listen")
    flag.Parse()
    http.HandleFunc("/healthz", healthHandler)
    http.HandleFunc("/scan", scanHandler)
    http.HandleFunc("/write", writeHandler)
    log.Println("storage v2 listening :9000")
    log.Fatal(http.ListenAndServe(*listen, nil))
}
