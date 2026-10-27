package erasure

import "encoding/binary"

// GF(2^8) with the usual 0x11d reduction polynomial (same field as RAID-6 / Backblaze / klauspost).
const poly = 0x11d

var (
	expT [512]byte
	logT [256]int
	mulT [256][256]byte
)

func init() {
	x := 1
	for i := 0; i < 255; i++ {
		expT[i] = byte(x)
		logT[x] = i
		x <<= 1
		if x&0x100 != 0 {
			x ^= poly
		}
	}
	for i := 255; i < 512; i++ {
		expT[i] = expT[i-255]
	}
	for a := 1; a < 256; a++ {
		for b := 1; b < 256; b++ {
			mulT[a][b] = expT[logT[a]+logT[b]]
		}
	}
}

func gfInv(a byte) byte {
	if a == 0 {
		panic("erasure: inverse of zero")
	}
	return expT[255-logT[a]]
}

// dst ^= c * src
func mulAdd(dst, src []byte, c byte) {
	switch c {
	case 0:
		return
	case 1:
		xorInto(dst, src)
		return
	}
	row := &mulT[c]
	n := len(src)
	dst = dst[:n]
	i := 0
	for ; i+8 <= n; i += 8 {
		s := src[i : i+8 : i+8]
		d := dst[i : i+8 : i+8]
		d[0] ^= row[s[0]]
		d[1] ^= row[s[1]]
		d[2] ^= row[s[2]]
		d[3] ^= row[s[3]]
		d[4] ^= row[s[4]]
		d[5] ^= row[s[5]]
		d[6] ^= row[s[6]]
		d[7] ^= row[s[7]]
	}
	for ; i < n; i++ {
		dst[i] ^= row[src[i]]
	}
}

// dst = c * src
func mulSet(dst, src []byte, c byte) {
	switch c {
	case 0:
		clear(dst[:len(src)])
		return
	case 1:
		copy(dst, src)
		return
	}
	row := &mulT[c]
	dst = dst[:len(src)]
	for i, v := range src {
		dst[i] = row[v]
	}
}

func xorInto(dst, src []byte) {
	n := len(src)
	dst = dst[:n]
	i := 0
	for ; i+8 <= n; i += 8 {
		v := binary.LittleEndian.Uint64(dst[i:]) ^ binary.LittleEndian.Uint64(src[i:])
		binary.LittleEndian.PutUint64(dst[i:], v)
	}
	for ; i < n; i++ {
		dst[i] ^= src[i]
	}
}
