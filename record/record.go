package record

import (
    "crypto/sha256"
    "encoding/binary"
    "errors"
)

const HeaderSize = 40

type Record struct {
    Version   uint8
    Flags     uint8
    MachineID uint64
    GameCode  uint32
    PeriodID  uint32
    SeqNo     uint32
    BettorID  uint64
    BetTime   uint64
    PickMode  uint8
    Data      []byte
    ThisHash  [32]byte
    ChainHash [32]byte
}

func (r *Record) Serialize(prevChain []byte) ([]byte, error) {
    buf := make([]byte, HeaderSize)
    buf[0] = 1
    buf[1] = r.Flags
    binary.BigEndian.PutUint64(buf[4:], r.MachineID)
    binary.BigEndian.PutUint32(buf[12:], r.GameCode)
    binary.BigEndian.PutUint32(buf[16:], r.PeriodID)
    binary.BigEndian.PutUint32(buf[20:], r.SeqNo)
    binary.BigEndian.PutUint64(buf[24:], r.BettorID)
    binary.BigEndian.PutUint64(buf[32:], r.BetTime)
    dataLen := uint32(len(r.Data))
    out := make([]byte, 0, HeaderSize+4+len(r.Data)+64)
    out = append(out, buf...)
    lenb := make([]byte, 4)
    binary.BigEndian.PutUint32(lenb, dataLen)
    out = append(out, lenb...)
    out = append(out, r.Data...)
    h := sha256.Sum256(out)
    r.ThisHash = h
    out = append(out, h[:]...)
    chbuf := make([]byte,0)
    if prevChain!=nil { chbuf = append(chbuf, prevChain...) }
    chbuf = append(chbuf, h[:]...)
    ch := sha256.Sum256(chbuf)
    r.ChainHash = ch
    out = append(out, ch[:]...)
    return out, nil
}

func Parse(b []byte) (*Record, error) {
    if len(b) < HeaderSize+4+64 { return nil, errors.New("buffer too small") }
    r := &Record{}
    hdr := b[:HeaderSize]
    r.Version = hdr[0]
    r.Flags = hdr[1]
    r.MachineID = binary.BigEndian.Uint64(hdr[4:12])
    r.GameCode = binary.BigEndian.Uint32(hdr[12:16])
    r.PeriodID = binary.BigEndian.Uint32(hdr[16:20])
    r.SeqNo = binary.BigEndian.Uint32(hdr[20:24])
    r.BettorID = binary.BigEndian.Uint64(hdr[24:32])
    r.BetTime = binary.BigEndian.Uint64(hdr[32:40])
    off := HeaderSize
    dataLen := binary.BigEndian.Uint32(b[off:off+4])
    off += 4
    if int(off+int(dataLen)+64) > len(b) { return nil, errors.New("truncated") }
    r.Data = make([]byte, dataLen)
    copy(r.Data, b[off:off+int(dataLen)])
    off += int(dataLen)
    copy(r.ThisHash[:], b[off:off+32])
    off += 32
    copy(r.ChainHash[:], b[off:off+32])
    return r, nil
}
