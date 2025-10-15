package writer

import (
    "encoding/binary"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"

    "github.com/klauspost/compress/zstd"
    "github.com/viacocha/lottery-storage/record"
)

const BlockMagic uint32 = 0x4C4F5442
const DefaultBlockThreshold = 64 * 1024 // 64KB

type SegmentWriter struct {
    mu sync.Mutex
    f *os.File
    path string
    size int64
    maxSize int64
    prevChain []byte
    blockBuf []byte
    blockThreshold int
    enc *zstd.Encoder
}

func NewSegmentWriter(baseDir, game, period string, shard int, maxSize int64) (*SegmentWriter, error) {
    name := fmt.Sprintf("g%s_p%s_s%04d_%d.seg", game, period, shard, time.Now().UnixNano())
    path := filepath.Join(baseDir, name)
    os.MkdirAll(baseDir, 0755)
    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil { return nil, err }
    enc, err := zstd.NewWriter(nil)
    if err != nil { return nil, err }
    return &SegmentWriter{f:f, path:path, size:0, maxSize:maxSize, blockBuf:make([]byte,0,DefaultBlockThreshold*2), blockThreshold:DefaultBlockThreshold, enc:enc}, nil
}

func (s *SegmentWriter) Append(rec *record.Record) (string, error) {
    s.mu.Lock(); defer s.mu.Unlock()
    recBytes, err := rec.Serialize(s.prevChain)
    if err != nil { return "", err }
    if len(recBytes) >= 32 {
        s.prevChain = recBytes[len(recBytes)-32:]
    }
    startOffset := uint64(s.size)
    s.blockBuf = append(s.blockBuf, recBytes...)
    idxPath := s.path + ".blkidx"
    idxf, _ := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    defer idxf.Close()
    offBuf := make([]byte, 8)
    binary.BigEndian.PutUint64(offBuf, startOffset)
    seqBuf := make([]byte, 4)
    binary.BigEndian.PutUint32(seqBuf, rec.SeqNo)
    idxf.Write(seqBuf)
    idxf.Write(offBuf)
    if len(s.blockBuf) >= s.blockThreshold {
        if err := s.flushBlock(); err != nil { return "", err }
    }
    return s.path, nil
}

func (s *SegmentWriter) flushBlock() error {
    if len(s.blockBuf) == 0 { return nil }
    comp := s.enc.EncodeAll(s.blockBuf, nil)
    header := make([]byte, 12)
    binary.BigEndian.PutUint32(header[0:], BlockMagic)
    binary.BigEndian.PutUint32(header[4:], uint32(len(comp)))
    binary.BigEndian.PutUint32(header[8:], uint32(len(s.blockBuf)))
    if _, err := s.f.Write(header); err != nil { return err }
    if _, err := s.f.Write(comp); err != nil { return err }
    s.size += int64(len(header) + len(comp))
    s.blockBuf = s.blockBuf[:0]
    return nil
}

func (s *SegmentWriter) Close() error {
    s.mu.Lock(); defer s.mu.Unlock()
    s.flushBlock()
    s.enc.Close()
    return s.f.Close()
}
