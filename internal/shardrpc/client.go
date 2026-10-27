package shardrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/monaaoua9/objstore/internal/frame"
)

var (
	ErrNotFound = errors.New("shard not found on node")
	// ErrCorrupt is what the node says when it refuses a bad frame during PUT.
	ErrCorrupt = frame.ErrCorrupt
)

type Client struct {
	HTTP   *http.Client
	Scheme string
	Secret string
}

func (c *Client) url(addr, v string, idx int, suffix string) string {
	s := c.Scheme
	if s == "" {
		s = "http"
	}
	return s + "://" + addr + "/internal/shard/" + v + "/" + strconv.Itoa(idx) + suffix
}

func (c *Client) newReq(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if c.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.Secret)
	}
	return req, nil
}

func statusErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	msg := strings.TrimSpace(string(b))
	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", ErrCorrupt, msg)
	}
	return fmt.Errorf("node returned %d: %s", resp.StatusCode, msg)
}

// PutStream is an in-flight upload. Write framed bytes, then Close to commit or Abort to discard.
type PutStream struct {
	pw   *io.PipeWriter
	done chan error
}

func (c *Client) OpenPut(ctx context.Context, addr, version string, idx int) *PutStream {
	pr, pw := io.Pipe()
	ps := &PutStream{pw: pw, done: make(chan error, 1)}
	go func() {
		req, err := c.newReq(ctx, http.MethodPut, c.url(addr, version, idx, ""), pr)
		if err != nil {
			pr.CloseWithError(err)
			ps.done <- err
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := c.HTTP.Do(req)
		if err != nil {
			pr.CloseWithError(err) // unblocks a writer stuck on the pipe
			ps.done <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			err = statusErr(resp)
			pr.CloseWithError(err)
			ps.done <- err
			return
		}
		io.Copy(io.Discard, resp.Body)
		ps.done <- nil
	}()
	return ps
}

// Write returns the remote's real failure if there is one instead of a bare "closed pipe".
func (p *PutStream) Write(b []byte) (int, error) { return p.pw.Write(b) }

func (p *PutStream) Close() error {
	p.pw.Close()
	return <-p.done
}

func (p *PutStream) Abort(cause error) {
	if cause == nil {
		cause = context.Canceled
	}
	p.pw.CloseWithError(cause)
	<-p.done
}

// Get opens a shard stream at the byte offset. The caller closes the body.
func (c *Client) Get(ctx context.Context, addr, version string, idx int, offset int64) (io.ReadCloser, error) {
	u := c.url(addr, version, idx, "")
	if offset > 0 {
		u += "?offset=" + strconv.FormatInt(offset, 10)
	}
	req, err := c.newReq(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, statusErr(resp)
	}
	return resp.Body, nil
}

// Head returns the shard's size, or ErrNotFound.
func (c *Client) Head(ctx context.Context, addr, version string, idx int) (int64, error) {
	req, err := c.newReq(ctx, http.MethodHead, c.url(addr, version, idx, ""), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.ContentLength, nil
	case http.StatusNotFound:
		return 0, ErrNotFound
	}
	return 0, fmt.Errorf("node returned %d", resp.StatusCode)
}

func (c *Client) Delete(ctx context.Context, addr, version string, idx int) error {
	req, err := c.newReq(ctx, http.MethodDelete, c.url(addr, version, idx, ""), nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return statusErr(resp)
	}
	return nil
}

func (c *Client) Verify(ctx context.Context, addr, version string, idx int) (VerifyResult, error) {
	var out VerifyResult
	req, err := c.newReq(ctx, http.MethodPost, c.url(addr, version, idx, "/verify"), nil)
	if err != nil {
		return out, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, statusErr(resp)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func (c *Client) Info(ctx context.Context, addr string) (Info, error) {
	var out Info
	s := c.Scheme
	if s == "" {
		s = "http"
	}
	req, err := c.newReq(ctx, http.MethodGet, s+"://"+addr+"/internal/node", nil)
	if err != nil {
		return out, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, statusErr(resp)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}
