package buffer

type bufferFile struct {
	buff []byte
	loc  int
}

func (bf bufferFile) Bytes() []byte {
	return bf.buff
}
