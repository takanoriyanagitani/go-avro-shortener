package blob2ext

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"compress/gzip"

	util "github.com/takanoriyanagitani/go-avro-shortener/util"
)

var (
	ErrInvalidId error = errors.New("invalid id")

	ErrBlobNotFound   error = errors.New("blob not found")
	ErrBlobIdNotFound error = errors.New("blob id not found")
	ErrInvalidType    error = errors.New("not a blob")

	ErrInvalidCompressType error = errors.New("unknown compress type")
	ErrUnsupportedCompress error = errors.New("unsupported compress type")
)

type CompressionType string

const (
	CompressUnknown CompressionType = "UNKNOWN"
	CompressNone    CompressionType = "none"
	CompressGzip    CompressionType = "gzip"
	CompressBzip2   CompressionType = "bzip2"
)

type Compress func(original []byte, buf *bytes.Buffer) error

var compressTypeMap map[string]CompressionType = map[string]CompressionType{
	"none":  CompressNone,
	"gzip":  CompressGzip,
	"bzip2": CompressBzip2,
}

func CompressTypeFromString(s string) (CompressionType, error) {
	typ, found := compressTypeMap[s]
	switch found {
	case true:
		return typ, nil
	default:
		return CompressUnknown, ErrInvalidCompressType
	}
}

func CompressNoneNew() Compress {
	return func(original []byte, buf *bytes.Buffer) error {
		buf.Reset()
		_, _ = buf.Write(original) // error is always nil or panic
		return nil
	}
}

const (
	CompressLevelGzipDefault int = gzip.BestSpeed
)

func CompressGzipNew(level int) Compress {
	var empty bytes.Buffer
	gw, e := gzip.NewWriterLevel(&empty, level)
	if nil != e {
		return func(_ []byte, _ *bytes.Buffer) error {
			return e
		}
	}
	return func(original []byte, buf *bytes.Buffer) error {
		buf.Reset()
		gw.Reset(buf)
		defer gw.Close()
		_, e := gw.Write(original)
		if nil != e {
			return e
		}
		return gw.Flush()
	}
}

func CompressGzipFast() Compress { return CompressGzipNew(gzip.BestSpeed) }

func (t CompressionType) ToCompresser(level int) (Compress, error) {
	switch t {
	case CompressNone:
		return CompressNoneNew(), nil
	case CompressGzip:
		return CompressGzipNew(level), nil
	default:
		return nil, ErrUnsupportedCompress
	}
}

type BlobId interface {
	fmt.Stringer
	io.WriterTo
	ToAny() any
}

type BlobIdInt64 int64

func (b BlobIdInt64) String() string {
	return fmt.Sprintf("%v", int64(b))
}

func (b BlobIdInt64) WriteTo(w io.Writer) (int64, error) {
	var u uint64 = uint64(b)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u)
	i, e := w.Write(buf[:])
	return int64(i), e
}

func (b BlobIdInt64) ToAny() any { return int64(b) }

func (b BlobIdInt64) AsBlobId() BlobId { return b }

type BlobIdUuid [16]byte

func (b BlobIdUuid) String() string {
	var buf [32]byte
	hex.Encode(buf[:], b[:])
	var s strings.Builder
	_, _ = s.Write(buf[:]) // error is always nil or OOM
	return s.String()
}

func (b BlobIdUuid) WriteTo(w io.Writer) (int64, error) {
	i, e := w.Write(b[:])
	return int64(i), e
}

func (b BlobIdUuid) ToAny() any { return b[:] }

func (b BlobIdUuid) AsBlobId() BlobId { return b }

type BlobIdString string

func (b BlobIdString) String() string {
	return string(b)
}

func (b BlobIdString) WriteTo(w io.Writer) (int64, error) {
	i, e := io.WriteString(w, string(b))
	return int64(i), e
}

func (b BlobIdString) ToAny() any { return string(b) }

func (b BlobIdString) AsBlobId() BlobId { return b }

type BlobInfo struct {
	BlobId
	Blob []byte
}

type SaveBlob func(BlobInfo) util.IO[BlobId]

func (s SaveBlob) WithCompress(c Compress) SaveBlob {
	var buf bytes.Buffer
	return func(bi BlobInfo) util.IO[BlobId] {
		return func(ctx context.Context) (BlobId, error) {
			var dat []byte = bi.Blob
			e := c(dat, &buf)
			if nil != e {
				return bi.BlobId, e
			}
			bi.Blob = buf.Bytes()
			return s(bi)(ctx)
		}
	}
}

func BlobIdFromAny(a any) (BlobId, error) {
	switch t := a.(type) {
	case [16]byte:
		return BlobIdUuid(t), nil
	case string:
		return BlobIdString(t), nil
	case int64:
		return BlobIdInt64(t), nil
	}
	return nil, ErrInvalidId
}

func BlobFromAny(a any) ([]byte, error) {
	switch t := a.(type) {
	case nil:
		return nil, nil
	case []byte:
		return t, nil
	default:
		return nil, ErrInvalidType
	}
}

func (s SaveBlob) ToMapper(
	blobKey string,
	blobIdKey string,
) func(map[string]any) util.IO[util.Void] {
	return func(original map[string]any) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			blob, found := original[blobKey]
			if !found {
				return util.Empty, ErrBlobNotFound
			}

			tblob, e := BlobFromAny(blob)
			if nil != e {
				return util.Empty, e
			}

			id, found := original[blobIdKey]
			if !found {
				return util.Empty, ErrBlobIdNotFound
			}

			tid, e := BlobIdFromAny(id)
			if nil != e {
				return util.Empty, e
			}

			bi := BlobInfo{
				BlobId: tid,
				Blob:   tblob,
			}

			_, e = s(bi)(ctx)
			if nil != e {
				return util.Empty, e
			}

			original[blobKey] = tid.ToAny()

			return util.Empty, nil
		}
	}
}

type Config struct {
	BlobKey   string
	BlobIdKey string
}

func (c Config) ToMapper(s SaveBlob) func(map[string]any) util.IO[util.Void] {
	return s.ToMapper(c.BlobKey, c.BlobIdKey)
}
