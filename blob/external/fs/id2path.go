package blob2fs

import (
	"path/filepath"

	be "github.com/takanoriyanagitani/go-avro-shortener/blob/external"
)

type IdToPath func(be.BlobId) string

func IdToPathFromDir(dirname string) func(ext string) IdToPath {
	return func(ext string) IdToPath {
		return func(i be.BlobId) string {
			var s string = i.String()
			return filepath.Join(dirname, s+"."+ext)
		}
	}
}

type Config struct {
	Dirname string
	Ext     string
}

func (c Config) ToIdToPath() IdToPath {
	return IdToPathFromDir(c.Dirname)(c.Ext)
}
