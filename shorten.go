package shorten

const (
	BlobSizeMaxDefault int = 1048576
)

type InputConfig struct {
	blobSizeMax int
}

func (c InputConfig) WithBlobSizeMax(i int) InputConfig {
	c.blobSizeMax = i
	return c
}

func (c InputConfig) BlobSizeMax() int { return c.blobSizeMax }

var InputConfigDefault InputConfig = InputConfig{}.
	WithBlobSizeMax(BlobSizeMaxDefault)
