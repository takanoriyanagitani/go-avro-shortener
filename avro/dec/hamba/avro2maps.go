package avro2maps

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	as "github.com/takanoriyanagitani/go-avro-shortener"
	util "github.com/takanoriyanagitani/go-avro-shortener/util"
)

func ReaderToMapsWithOptionsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		dec, e := ho.NewDecoder(rdr, opts...)
		if nil != e {
			yield(nil, e)
			return
		}

		var err error = nil
		var buf map[string]any
		for dec.HasNext() {
			clear(buf)

			err = dec.Decode(&buf)
			if !yield(buf, err) {
				return
			}
		}
	}
}

func ReaderToMapsWithApiHamba(
	rdr io.Reader,
	api ha.API,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = []ho.DecoderFunc{
		ho.WithDecoderConfig(api),
	}
	return ReaderToMapsWithOptionsHamba(rdr, opts...)
}

func ReaderToMapsWithConfigHamba(
	rdr io.Reader,
	cfg ha.Config,
) iter.Seq2[map[string]any, error] {
	var api ha.API = cfg.Freeze()
	return ReaderToMapsWithApiHamba(rdr, api)
}

func ReaderToMapsWithConfig(
	rdr io.Reader,
	cfg as.InputConfig,
) iter.Seq2[map[string]any, error] {
	var hcfg ha.Config
	hcfg.MaxByteSliceSize = cfg.BlobSizeMax()
	return ReaderToMapsWithConfigHamba(rdr, hcfg)
}

func StdinToMapsWithConfig(
	cfg as.InputConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMapsWithConfig(os.Stdin, cfg)
}

func ConfigToStdinToMaps(
	cfg as.InputConfig,
) util.IO[iter.Seq2[map[string]any, error]] {
	return func(_ context.Context) (iter.Seq2[map[string]any, error], error) {
		return StdinToMapsWithConfig(cfg), nil
	}
}
