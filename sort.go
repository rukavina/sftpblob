package sftpblob

import (
	"strings"

	"gocloud.dev/blob/driver"
)

//ByDirFilename implements sort.Interface
type ByDirFilename []*driver.ListObject

func (nf ByDirFilename) Len() int {
	return len(nf)
}

func (nf ByDirFilename) Swap(i, j int) {
	nf[i], nf[j] = nf[j], nf[i]
}

func (nf ByDirFilename) Less(i, j int) bool {
	//directories first
	if nf[i].IsDir && !nf[j].IsDir {
		return true
	}
	//files after
	if !nf[i].IsDir && nf[j].IsDir {
		return false
	}

	comp := strings.Compare(nf[i].Key, nf[j].Key)
	return comp < 0
}
