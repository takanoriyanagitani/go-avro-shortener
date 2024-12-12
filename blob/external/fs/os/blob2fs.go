package blob2fs

import (
	"context"
	"os"

	util "github.com/takanoriyanagitani/go-avro-shortener/util"

	be "github.com/takanoriyanagitani/go-avro-shortener/blob/external"
	ef "github.com/takanoriyanagitani/go-avro-shortener/blob/external/fs"
)

const (
	FileModeDefault os.FileMode = 0644
)

func SaveBlobFs(
	mode os.FileMode,
	id2path ef.IdToPath,
) be.SaveBlob {
	return func(bi be.BlobInfo) util.IO[be.BlobId] {
		return func(_ context.Context) (be.BlobId, error) {
			var i be.BlobId = bi.BlobId
			var fullpath string = id2path(i)
			var dat []byte = bi.Blob
			e := os.WriteFile(fullpath, dat, mode)
			return i, e
		}
	}
}

func SaveBlobFsDefault(id2path ef.IdToPath) (be.SaveBlob, error) {
	return SaveBlobFs(FileModeDefault, id2path), nil
}
