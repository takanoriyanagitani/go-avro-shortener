package maps2avro

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	util "github.com/takanoriyanagitani/go-avro-shortener/util"
)

func MapsToWriterWithSchemaHamba(
	w io.Writer,
	s ha.Schema,
) func(iter.Seq2[map[string]any, error]) util.IO[util.Void] {
	return func(rows iter.Seq2[map[string]any, error]) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			var bw *bufio.Writer = bufio.NewWriter(w)
			defer bw.Flush()

			enc, e := ho.NewEncoderWithSchema(
				s,
				bw,
			)
			if nil != e {
				return util.Empty, e
			}
			defer enc.Close()

			for row, e := range rows {
				select {
				case <-ctx.Done():
					return util.Empty, ctx.Err()
				default:
				}
				if nil != e {
					return util.Empty, e
				}

				e = enc.Encode(row)
				if nil != e {
					return util.Empty, e
				}

				e = enc.Flush()
				if nil != e {
					return util.Empty, e
				}
			}
			return util.Empty, nil
		}
	}
}

func MapsToStdoutWithSchemaHamba(
	s ha.Schema,
) func(iter.Seq2[map[string]any, error]) util.IO[util.Void] {
	return MapsToWriterWithSchemaHamba(os.Stdout, s)
}

func MapsToStdoutWithSchema(
	schema string,
) func(iter.Seq2[map[string]any, error]) util.IO[util.Void] {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return func(_ iter.Seq2[map[string]any, error]) util.IO[util.Void] {
			return util.Err[util.Void](e)
		}
	}
	return MapsToStdoutWithSchemaHamba(parsed)
}
