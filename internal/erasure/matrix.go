package erasure

import "errors"

type matrix [][]byte

var errSingular = errors.New("erasure: singular matrix")

func newMatrix(r, c int) matrix {
	m := make(matrix, r)
	buf := make([]byte, r*c)
	for i := range m {
		m[i] = buf[i*c : (i+1)*c : (i+1)*c]
	}
	return m
}

func identity(n int) matrix {
	m := newMatrix(n, n)
	for i := 0; i < n; i++ {
		m[i][i] = 1
	}
	return m
}

func (m matrix) rows(idx []int) matrix {
	out := make(matrix, len(idx))
	for i, r := range idx {
		out[i] = m[r]
	}
	return out
}

// Gauss-Jordan on [m | I]. Fine for k+m <= 256, we only ever do this on cache miss.
func (m matrix) invert() (matrix, error) {
	n := len(m)
	w := newMatrix(n, 2*n)
	for i := 0; i < n; i++ {
		copy(w[i], m[i])
		w[i][n+i] = 1
	}
	for col := 0; col < n; col++ {
		p := col
		for p < n && w[p][col] == 0 {
			p++
		}
		if p == n {
			return nil, errSingular
		}
		w[col], w[p] = w[p], w[col]

		if d := w[col][col]; d != 1 {
			inv := gfInv(d)
			mulSet(w[col], w[col], inv)
		}
		for r := 0; r < n; r++ {
			if r == col || w[r][col] == 0 {
				continue
			}
			mulAdd(w[r], w[col], w[r][col])
		}
	}
	out := newMatrix(n, n)
	for i := 0; i < n; i++ {
		copy(out[i], w[i][n:])
	}
	return out, nil
}

// cauchy builds the parity block: entry (i,j) = 1 / (x_i ^ y_j) with x = k+i, y = j.
// Every square submatrix of a Cauchy matrix is invertible, so [I; C] is MDS: any k rows work.
func cauchy(k, m int) matrix {
	c := newMatrix(m, k)
	for i := 0; i < m; i++ {
		for j := 0; j < k; j++ {
			c[i][j] = gfInv(byte(k+i) ^ byte(j))
		}
	}
	return c
}
