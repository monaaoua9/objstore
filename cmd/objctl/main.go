package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/monaaoua9/objstore/internal/httpx"
)

const usage = `usage: objctl [-e endpoint] [-t token] <command> [args]

  mb <bucket>                   create bucket
  rb <bucket>                   remove empty bucket
  lb                            list buckets
  ls <bucket> [prefix]          list objects
  put <bucket> <key> <file|->   upload (streams, never buffers)
  mput <bucket> <key> <file> [partMiB]  multipart upload, 4 parts in flight (default 64 MiB parts)
  get <bucket> <key> [file|-]   download (-r a-b for a byte range)
  stat <bucket> <key>           object headers
  rm <bucket> <key>             delete object
  status                        cluster nodes, raft, coding params
  repairs                       shards waiting for repair
  drain <node>                  stop placing data on node and move its shards away
  activate <node>               put a drained node back in service
  rmnode <node> [-f]            deregister a drained or dead node
  voters                        metadata voters (raft members)
  addvoter <id> <host:port>     add a running --join node as a voter
  rmvoter <id>                  remove a voter from the metadata group
  versioning <bucket>           turn on object versioning (cannot be undone)
  versions <bucket> [prefix]    every version and delete marker (get/stat/rm accept -v <version>)
  lifecycle <bucket> [set <file|->|clear]   show, replace or remove expiry rules (JSON: {"rules":[...]})
`

var (
	endpoint = flag.String("e", envOr("OBJCTL_ENDPOINT", "http://127.0.0.1:9000"), "node URL (env OBJCTL_ENDPOINT)")
	token    = flag.String("t", os.Getenv("OBJCTL_TOKEN"), "API bearer token (env OBJCTL_TOKEN)")
	byteRng  = flag.String("r", "", "byte range for get, e.g. 0-1023")
	force    = flag.Bool("f", false, "force")
	version  = flag.String("v", "", "object version id for get, stat and rm")
	client   = httpx.NewClient(nil)
)

func main() {
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	a := flag.Args()
	if len(a) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	need := func(n int) {
		if len(a) < n+1 {
			flag.Usage()
			os.Exit(2)
		}
	}
	var err error
	switch a[0] {
	case "mb":
		need(1)
		err = simple("PUT", "/buckets/"+a[1], nil, 201)
	case "rb":
		need(1)
		err = simple("DELETE", "/buckets/"+a[1], nil, 204)
	case "lb":
		err = lb()
	case "ls":
		need(1)
		prefix := ""
		if len(a) > 2 {
			prefix = a[2]
		}
		err = ls(a[1], prefix)
	case "put":
		need(3)
		err = put(a[1], a[2], a[3])
	case "mput":
		need(3)
		mib := 64
		if len(a) > 4 {
			fmt.Sscan(a[4], &mib)
		}
		err = mput(a[1], a[2], a[3], int64(mib)<<20)
	case "get":
		need(2)
		out := "-"
		if len(a) > 3 {
			out = a[3]
		}
		err = get(a[1], a[2], out)
	case "stat":
		need(2)
		err = stat(a[1], a[2])
	case "rm":
		need(2)
		err = simple("DELETE", "/buckets/"+a[1]+"/objects/"+a[2]+vq(), nil, 204)
	case "versioning":
		need(1)
		err = simple("PUT", "/buckets/"+a[1]+"/versioning", strings.NewReader(`{"enabled":true}`), 200)
	case "lifecycle":
		need(1)
		switch {
		case len(a) == 2:
			err = dump("/buckets/" + a[1] + "/lifecycle")
		case a[2] == "clear":
			err = simple("DELETE", "/buckets/"+a[1]+"/lifecycle", nil, 204)
		case a[2] == "set" && len(a) > 3:
			var in io.Reader = os.Stdin
			if a[3] != "-" {
				f, ferr := os.Open(a[3])
				if ferr != nil {
					err = ferr
					break
				}
				defer f.Close()
				in = f
			}
			err = simple("PUT", "/buckets/"+a[1]+"/lifecycle", in, 200)
		default:
			flag.Usage()
			os.Exit(2)
		}
	case "versions":
		need(1)
		p := ""
		if len(a) > 2 {
			p = "?prefix=" + url.QueryEscape(a[2])
		}
		err = dump("/buckets/" + a[1] + "/versions" + p)
	case "status":
		err = status()
	case "repairs":
		err = dump("/cluster/repair")
	case "drain":
		need(1)
		err = simple("POST", "/cluster/nodes/"+a[1]+"/drain", nil, 200)
	case "activate":
		need(1)
		err = simple("POST", "/cluster/nodes/"+a[1]+"/activate", nil, 200)
	case "rmnode":
		need(1)
		q := ""
		if *force {
			q = "?force=true"
		}
		err = simple("DELETE", "/cluster/nodes/"+a[1]+q, nil, 204)
	case "voters":
		err = dump("/cluster/voters")
	case "addvoter":
		need(2)
		b, _ := json.Marshal(map[string]string{"addr": a[2]})
		err = simple("PUT", "/cluster/voters/"+a[1], strings.NewReader(string(b)), 200)
	case "rmvoter":
		need(1)
		err = simple("DELETE", "/cluster/voters/"+a[1], nil, 204)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "objctl:", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func req(method, path string, body io.Reader, hdr map[string]string) (*http.Response, error) {
	r, err := http.NewRequest(method, strings.TrimRight(*endpoint, "/")+"/api/v1"+path, body)
	if err != nil {
		return nil, err
	}
	if *token != "" {
		r.Header.Set("Authorization", "Bearer "+*token)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	return client.Do(r)
}

func apiErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Error struct{ Code, Message string }
	}
	if json.Unmarshal(b, &e) == nil && e.Error.Code != "" {
		return fmt.Errorf("%s: %s (HTTP %d)", e.Error.Code, e.Error.Message, resp.StatusCode)
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
}

func simple(method, path string, body io.Reader, want int) error {
	resp, err := req(method, path, body, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		return apiErr(resp)
	}
	io.Copy(os.Stdout, resp.Body)
	return nil
}

func dump(path string) error {
	resp, err := req("GET", path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return apiErr(resp)
	}
	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func lb() error {
	resp, err := req("GET", "/buckets", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return apiErr(resp)
	}
	var out struct {
		Data []struct{ Name, Created string }
	}
	json.NewDecoder(resp.Body).Decode(&out)
	for _, b := range out.Data {
		fmt.Println(b.Created[:19], b.Name)
	}
	return nil
}

func ls(bucket, prefix string) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	cursor := ""
	for {
		q := url.Values{"prefix": {prefix}, "cursor": {cursor}, "limit": {"1000"}}
		resp, err := req("GET", "/buckets/"+bucket+"/objects?"+q.Encode(), nil, nil)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			defer resp.Body.Close()
			return apiErr(resp)
		}
		var out struct {
			Data []struct {
				Key     string
				Size    int64
				Created string
			}
			Meta struct {
				Next string `json:"next_cursor"`
			}
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err != nil {
			return err
		}
		for _, o := range out.Data {
			fmt.Fprintf(tw, "%s\t%d\t%s\n", o.Created[:19], o.Size, o.Key)
		}
		if out.Meta.Next == "" {
			break
		}
		cursor = out.Meta.Next
	}
	return tw.Flush()
}

func put(bucket, key, file string) error {
	var body io.Reader = os.Stdin
	hdr := map[string]string{}
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()
		body = f
	}
	resp, err := req("PUT", "/buckets/"+bucket+"/objects/"+key, body, hdr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return apiErr(resp)
	}
	var out struct {
		Data struct {
			Size int64
			ETag string
		}
	}
	json.NewDecoder(resp.Body).Decode(&out)
	fmt.Printf("stored %d bytes  sha256=%s\n", out.Data.Size, out.Data.ETag)
	return nil
}

func get(bucket, key, out string) error {
	hdr := map[string]string{}
	if *byteRng != "" {
		hdr["Range"] = "bytes=" + *byteRng
	}
	resp, err := req("GET", "/buckets/"+bucket+"/objects/"+key+vq(), nil, hdr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		return apiErr(resp)
	}
	w := io.Writer(os.Stdout)
	if out != "-" {
		f, err := os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	n, err := io.Copy(w, resp.Body)
	if err == nil && resp.ContentLength >= 0 && n != resp.ContentLength {
		err = fmt.Errorf("short read: %d of %d bytes", n, resp.ContentLength)
	}
	return err
}

func stat(bucket, key string) error {
	resp, err := req("HEAD", "/buckets/"+bucket+"/objects/"+key+vq(), nil, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	fmt.Printf("size     %d\netag     %s\nversion  %s\nmodified %s\ntype     %s\n",
		resp.ContentLength, strings.Trim(resp.Header.Get("ETag"), `"`), resp.Header.Get("X-Object-Version"),
		resp.Header.Get("Last-Modified"), resp.Header.Get("Content-Type"))
	return nil
}

func status() error {
	resp, err := req("GET", "/cluster", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return apiErr(resp)
	}
	var out struct {
		Data struct {
			Nodes []struct {
				ID, Addr, Zone, State string
			}
			Stats  map[string]int64
			Coding map[string]int
			Raft   map[string]any
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	d := out.Data
	fmt.Printf("coding   %d+%d, %d KiB shards, write quorum %d\n", d.Coding["k"], d.Coding["m"], d.Coding["shard_size"]>>10, d.Coding["write_quorum"])
	fmt.Printf("objects  %d (%d bytes) in %d buckets; %d shards awaiting repair; %d versions awaiting gc\n",
		d.Stats["objects"], d.Stats["bytes"], d.Stats["buckets"], d.Stats["damaged"], d.Stats["gc_queued"])
	if d.Raft != nil {
		fmt.Printf("raft     this node is %v, term %v, leader %v\n", d.Raft["state"], d.Raft["term"], d.Raft["leader"])
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tSTATE\tZONE\tADDR")
	for _, n := range d.Nodes {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.ID, n.State, n.Zone, n.Addr)
	}
	return tw.Flush()
}

func mput(bucket, key, file string, partSize int64) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}

	resp, err := req("POST", "/buckets/"+bucket+"/uploads?key="+url.QueryEscape(key), nil, nil)
	if err != nil {
		return err
	}
	var created struct {
		Data struct {
			UploadID string `json:"upload_id"`
		}
	}
	err = json.NewDecoder(resp.Body).Decode(&created)
	if resp.StatusCode != 201 {
		defer resp.Body.Close()
		return apiErr(resp)
	}
	resp.Body.Close()
	if err != nil {
		return err
	}
	id := created.Data.UploadID
	base := "/buckets/" + bucket + "/uploads/" + id

	n := int((st.Size() + partSize - 1) / partSize)
	if n == 0 {
		n = 1
	}
	etags := make([]string, n)
	errs := make(chan error, n)
	sem := make(chan struct{}, 4)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			off := int64(i) * partSize
			sz := min(partSize, st.Size()-off)
			r, err := req("PUT", fmt.Sprintf("%s/parts/%d", base, i+1), io.NewSectionReader(f, off, sz), nil)
			if err != nil {
				errs <- err
				return
			}
			defer r.Body.Close()
			if r.StatusCode != 201 {
				errs <- apiErr(r)
				return
			}
			var out struct{ Data struct{ ETag string } }
			json.NewDecoder(r.Body).Decode(&out)
			etags[i] = out.Data.ETag
			errs <- nil
		}()
	}
	var firstErr error
	for i := 0; i < n; i++ {
		if e := <-errs; e != nil && firstErr == nil {
			firstErr = e
		}
	}
	if firstErr != nil {
		if r, err := req("DELETE", base, nil, nil); err == nil {
			r.Body.Close()
		}
		return firstErr
	}

	type sel struct {
		Part int    `json:"part"`
		ETag string `json:"etag"`
	}
	var body struct {
		Parts []sel `json:"parts"`
	}
	for i, e := range etags {
		body.Parts = append(body.Parts, sel{i + 1, e})
	}
	b, _ := json.Marshal(body)
	r, err := req("POST", base+"/complete", strings.NewReader(string(b)), nil)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 201 {
		return apiErr(r)
	}
	var done struct {
		Data struct {
			Size  int64
			ETag  string
			Parts int
		}
	}
	json.NewDecoder(r.Body).Decode(&done)
	fmt.Printf("stored %d bytes in %d parts  etag=%s\n", done.Data.Size, done.Data.Parts, done.Data.ETag)
	return nil
}

func vq() string {
	if *version == "" {
		return ""
	}
	return "?version=" + url.QueryEscape(*version)
}
