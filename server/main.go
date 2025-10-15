package main

import (
    "encoding/binary"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "sync"

    "github.com/klauspost/compress/zstd"
    "github.com/example/lottery-storage/record"
    "github.com/example/lottery-storage/writer"
)

func readBlkIdx(idxPath string) (map[uint32]uint64, error) {
    m := map[uint32]uint64{}
    f, err := os.Open(idxPath)
    if err != nil { return m, err }
    defer f.Close()
    buf := make([]byte, 12)
    for {
        n, err := io.ReadFull(f, buf)
        if err != nil {
            if err==io.EOF || err==io.ErrUnexpectedEOF { break }
            return m, err
        }
        seq := binary.BigEndian.Uint32(buf[0:4])
        off := binary.BigEndian.Uint64(buf[4:12])
        m[seq]=off
        _ = n
    }
    return m, nil
}

func scanHandler(w http.ResponseWriter, r *http.Request) {
    qs := r.URL.Query()
    game := qs.Get("game")
    period := qs.Get("period")
    shard := qs.Get("shard")
    if game=="" || period=="" { http.Error(w, "game+period required", 400); return }
    base := filepath.Join("/data", game, period)
    files, err := filepath.Glob(filepath.Join(base, "*.seg"))
    if err!=nil { http.Error(w, "glob err", 500); return }

    enc, _ := zstd.NewReader(nil)
    defer enc.Close()

    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte("["))
    first := true
    mu := sync.Mutex{}
    var wg sync.WaitGroup
    for _, f := range files {
        if shard!="" && !strings.Contains(f, fmt.Sprintf("_s%04s_", shard)) { continue }
        fi, err := os.Open(f)
        if err!=nil { continue }
        wg.Add(1)
        go func(fi *os.File) {
            defer fi.Close()
            defer wg.Done()
            for {
                hdr := make([]byte, 12)
                if _, err := io.ReadFull(fi, hdr); err!=nil { break }
                magic := binary.BigEndian.Uint32(hdr[0:4])
                if magic != 0x4C4F5442 { break }
                compLen := binary.BigEndian.Uint32(hdr[4:8])
                comp := make([]byte, compLen)
                if _, err := io.ReadFull(fi, comp); err!=nil { break }
                raw, err := enc.DecodeAll(comp, nil)
                if err!=nil { break }
                off := 0
                for off < len(raw) {
                    if off+40+4+64 > len(raw) { break }
                    dataLen := int(binary.BigEndian.Uint32(raw[off+40:off+44]))
                    recLen := 40 + 4 + dataLen + 64
                    if off+recLen > len(raw) { break }
                    recBytes := raw[off:off+recLen]
                    rec, err := record.Parse(recBytes)
                    if err==nil {
                        b, _ := json.Marshal(rec)
                        mu.Lock()
                        if !first { w.Write([]byte(",")) } else { first=false }
                        w.Write(b)
                        mu.Unlock()
                    }
                    off += recLen
                }
            }
        }(fi)
    }
    wg.Wait()
    w.Write([]byte("]"))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
    body, _ := io.ReadAll(r.Body)
    rec, err := record.Parse(body)
    if err!=nil { http.Error(w, "bad record", 400); return }
    baseDir := filepath.Join("/data", fmt.Sprintf("%02d", rec.GameCode), fmt.Sprintf("%08d", rec.PeriodID))
    wtr, _ := writer.NewSegmentWriter(baseDir, fmt.Sprintf("%02d", rec.GameCode), fmt.Sprintf("%08d", rec.PeriodID), 0, 256<<20)
    defer wtr.Close()
    wtr.Append(rec)
    w.Write([]byte("OK"))
}

func main() {
    flag.Parse()
    http.HandleFunc("/scan", scanHandler)
    http.HandleFunc("/write", writeHandler)
    log.Println("storage v2 listening :9000")
    log.Fatal(http.ListenAndServe(":9000", nil))
}
