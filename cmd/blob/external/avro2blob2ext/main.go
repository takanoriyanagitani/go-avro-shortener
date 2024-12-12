package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	as "github.com/takanoriyanagitani/go-avro-shortener"
	util "github.com/takanoriyanagitani/go-avro-shortener/util"

	dh "github.com/takanoriyanagitani/go-avro-shortener/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-shortener/avro/enc/hamba"

	be "github.com/takanoriyanagitani/go-avro-shortener/blob/external"
	ef "github.com/takanoriyanagitani/go-avro-shortener/blob/external/fs"
	fo "github.com/takanoriyanagitani/go-avro-shortener/blob/external/fs/os"
)

var EnvVarByKey func(string) util.IO[string] = util.Lift(
	func(key string) (val string, e error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var blobSizeMaxStr util.IO[string] = EnvVarByKey("ENV_BLOB_SIZE_MAX").
	OrElse(util.Of("1048576"))
var blobSizeMax util.IO[int] = util.Bind(
	blobSizeMaxStr,
	util.Lift(strconv.Atoi),
)
var inputConfig util.IO[as.InputConfig] = util.Bind(
	blobSizeMax,
	util.Lift(func(i int) (as.InputConfig, error) {
		return as.InputConfig{}.
			WithBlobSizeMax(i), nil
	}),
)

var inputRows util.IO[iter.Seq2[map[string]any, error]] = util.Bind(
	inputConfig,
	dh.ConfigToStdinToMaps,
)

var compressType util.IO[be.CompressionType] = util.Bind(
	EnvVarByKey("ENV_COMPRESS_TYPE").OrElse(util.Of("none")),
	util.Lift(be.CompressTypeFromString),
)
var compressLevel util.IO[int] = util.Bind(
	EnvVarByKey("ENV_COMPRESS_LEVEL"),
	util.Lift(strconv.Atoi),
).OrElse(util.Of(be.CompressLevelGzipDefault))
var compress util.IO[be.Compress] = util.Bind(
	compressType,
	func(typ be.CompressionType) util.IO[be.Compress] {
		return util.Bind(
			compressLevel,
			util.Lift(func(level int) (be.Compress, error) {
				return typ.ToCompresser(level)
			}),
		)
	},
)

var dirname util.IO[string] = EnvVarByKey("ENV_BLOB_DIRNAME")
var ext util.IO[string] = EnvVarByKey("ENV_BLOB_EXT")
var pathCfg util.IO[ef.Config] = util.Bind(
	dirname,
	func(dname string) util.IO[ef.Config] {
		return util.Bind(
			ext,
			util.Lift(func(ex string) (ef.Config, error) {
				return ef.Config{
					Dirname: dname,
					Ext:     ex,
				}, nil
			}),
		)
	},
)

var id2path util.IO[ef.IdToPath] = util.Bind(
	pathCfg,
	util.Lift(func(pc ef.Config) (ef.IdToPath, error) {
		return pc.ToIdToPath(), nil
	}),
)

var saveBlob util.IO[be.SaveBlob] = util.Bind(
	id2path,
	util.Lift(fo.SaveBlobFsDefault),
)
var saveBlobWithCompress util.IO[be.SaveBlob] = util.Bind(
	saveBlob,
	func(s be.SaveBlob) util.IO[be.SaveBlob] {
		return util.Bind(
			compress,
			util.Lift(func(c be.Compress) (be.SaveBlob, error) {
				return s.WithCompress(c), nil
			}),
		)
	},
)

var blobKey util.IO[string] = EnvVarByKey("ENV_BLOB_KEY")
var blobIdKey util.IO[string] = EnvVarByKey("ENV_BLOB_ID_KEY")
var blob2extCfg util.IO[be.Config] = util.Bind(
	blobKey,
	func(bk string) util.IO[be.Config] {
		return util.Bind(
			blobIdKey,
			util.Lift(func(bik string) (be.Config, error) {
				return be.Config{
					BlobKey:   bk,
					BlobIdKey: bik,
				}, nil
			}),
		)
	},
)

var mapper util.IO[func(map[string]any) util.IO[util.Void]] = util.Bind(
	blob2extCfg,
	func(cfg be.Config) util.IO[func(map[string]any) util.IO[util.Void]] {
		return util.Bind(
			saveBlobWithCompress,
			func(
				saver be.SaveBlob,
			) util.IO[func(map[string]any) util.IO[util.Void]] {
				var mapper func(map[string]any) util.IO[util.Void] = cfg.
					ToMapper(saver)
				return util.Of(mapper)
			},
		)
	},
)

var mapdRows util.IO[iter.Seq2[map[string]any, error]] = util.Bind(
	mapper,
	func(
		m func(map[string]any) util.IO[util.Void],
	) util.IO[iter.Seq2[map[string]any, error]] {
		return util.Bind(
			inputRows,
			func(
				original iter.Seq2[map[string]any, error],
			) util.IO[iter.Seq2[map[string]any, error]] {
				return func(
					ctx context.Context,
				) (iter.Seq2[map[string]any, error], error) {
					return func(yield func(map[string]any, error) bool) {
						for row, e := range original {
							var mapd map[string]any = row
							if nil == e {
								_, e = m(mapd)(ctx)
							}
							if !yield(mapd, e) {
								return
							}
						}
					}, nil
				}
			},
		)
	},
)

var schemaFilename util.IO[string] = EnvVarByKey("ENV_SCHEMA_FILENAME")
var schemaSizeLimit util.IO[int] = util.Bind(
	EnvVarByKey("ENV_SCHEMA_SIZE_LIMIT").OrElse(util.Of("1048576")),
	util.Lift(strconv.Atoi),
)

func FilenameToStringLimited(limit int) func(string) util.IO[string] {
	return func(filename string) util.IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()
			var buf strings.Builder
			limited := &io.LimitedReader{
				R: f,
				N: int64(limit),
			}
			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		}
	}
}

var schema util.IO[string] = util.Bind(
	schemaSizeLimit,
	func(limit int) util.IO[string] {
		return util.Bind(
			schemaFilename,
			FilenameToStringLimited(limit),
		)
	},
)

var maps2stdout util.
	IO[func(iter.Seq2[map[string]any, error]) util.IO[util.Void]] = util.
	Bind(
		schema,
		func(s string) util.
			IO[func(iter.Seq2[map[string]any, error]) util.IO[util.Void]] {
			return util.Of(eh.MapsToStdoutWithSchema(s))
		},
	)

var stdin2mapd2stdout util.IO[util.Void] = util.Bind(
	maps2stdout,
	func(
		sink func(iter.Seq2[map[string]any, error]) util.IO[util.Void],
	) util.IO[util.Void] {
		return util.Bind(
			mapdRows,
			sink,
		)
	},
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, e := stdin2mapd2stdout(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
