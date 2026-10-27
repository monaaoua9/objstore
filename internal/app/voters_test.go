package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/monaaoua9/objstore/internal/meta"
)

func (c *tcluster) startJoiner(id string) string {
	c.t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	cfg := c.config(id, ln)
	cfg.Join = true
	n, err := Start(cfg)
	if err != nil {
		c.t.Fatalf("start joiner %s: %v", id, err)
	}
	c.nmu.Lock()
	c.nodes[id] = n
	c.nmu.Unlock()
	return ln.Addr().String()
}

// voterCall hits the voter admin API on via, retrying while another change is still committing.
func (c *tcluster) voterCall(via, method, id, addr string, wantCode int) {
	c.t.Helper()
	url := c.base(via) + "/api/v1/cluster/voters/" + id
	deadline := time.Now().Add(20 * time.Second)
	for {
		var body *bytes.Reader
		if addr != "" {
			b, _ := json.Marshal(map[string]string{"addr": addr})
			body = bytes.NewReader(b)
		} else {
			body = bytes.NewReader(nil)
		}
		resp, out := c.do(method, url, body, nil)
		if resp.StatusCode == wantCode {
			return
		}
		if resp.StatusCode != 409 && resp.StatusCode != 503 || time.Now().After(deadline) {
			c.t.Fatalf("%s %s: %d %s", method, url, resp.StatusCode, out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *tcluster) voterIDs(via string) []string {
	resp, b := c.do("GET", c.base(via)+"/api/v1/cluster/voters", nil, nil)
	if resp.StatusCode != 200 {
		c.t.Fatalf("voters: %d %s", resp.StatusCode, b)
	}
	var out struct{ Data []struct{ ID string } }
	mustJSON(c.t, b, &out)
	var ids []string
	for _, v := range out.Data {
		ids = append(ids, v.ID)
	}
	return ids
}

// Every original metadata voter is replaced by a new one, then the old ones are killed. Storage-only
// nodes started with the old voter list must keep working, which means they learned the new one.
func TestReplaceAllVoters(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	c.mkbucket("n4", "mig")
	c.put("n4", "mig", "before", randBytes(50_000))

	// storage-only nodes can't serve the voter admin API
	if resp, _ := c.do("GET", c.base("n4")+"/api/v1/cluster/voters", nil, nil); resp.StatusCode != 501 {
		t.Fatalf("storage-only node answered voters: %d", resp.StatusCode)
	}

	for _, id := range []string{"n7", "n8", "n9"} {
		addr := c.startJoiner(id)
		c.voterCall("n1", "PUT", id, addr, 200)
	}
	if got := strings.Join(c.voterIDs("n8"), ","); got != "n1,n2,n3,n7,n8,n9" {
		t.Fatalf("voters after adds: %s", got)
	}

	// n7 is in the group now, so it can drive the removals (including whoever is leader)
	for _, id := range []string{"n1", "n2", "n3"} {
		c.voterCall("n7", "DELETE", id, "", 204)
	}
	eventually(t, 10*time.Second, "voter list shrinks to the new three", func() bool {
		return strings.Join(c.voterIDs("n7"), ",") == "n7,n8,n9"
	})

	// the swap can finish inside one refresh tick; killing the old voters before the storage nodes
	// have heard of the new ones would orphan them for good
	eventually(t, 10*time.Second, "storage nodes learn a new voter", func() bool {
		for _, id := range []string{"n4", "n5", "n6"} {
			known := false
			for _, a := range c.nodes[id].Meta.B.(*meta.HTTPBackend).Voters() {
				known = known || a == c.nodes["n7"].Addr()
			}
			if !known {
				return false
			}
		}
		return true
	})
	c.kill("n1", "n2", "n3")

	eventually(t, 30*time.Second, "writes through a storage-only node after the swap", func() bool {
		resp, _ := c.tryDo("PUT", c.base("n4")+"/api/v1/buckets/after", nil)
		return resp != nil && (resp.StatusCode == 201 || resp.StatusCode == 409)
	})
	data := randBytes(150_000)
	eventually(t, 30*time.Second, "object write after the swap", func() bool {
		resp, _ := c.tryDo("PUT", c.base("n5")+"/api/v1/buckets/after/objects/k", bytes.NewReader(data))
		return resp != nil && resp.StatusCode == 201
	})
	c.mustGet("n6", "after", "k", data)

	// the removed ones are no longer voters even if they come back
	resp, b := c.do("GET", c.base("n9")+"/api/v1/cluster", nil, nil)
	if resp.StatusCode != 200 || !strings.Contains(string(b), fmt.Sprintf(`"voters":["n7","n8","n9"]`)) {
		t.Fatalf("cluster info: %d %s", resp.StatusCode, b)
	}
}

func TestVoterAPIErrors(t *testing.T) {
	c := newCluster(t, 3, 3, 3, 2)
	resp, b := c.do("DELETE", c.base("n1")+"/api/v1/cluster/voters/ghost", nil, nil)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), "INVALID_MEMBERSHIP_CHANGE") {
		t.Fatalf("remove unknown voter: %d %s", resp.StatusCode, b)
	}
	resp, b = c.do("PUT", c.base("n1")+"/api/v1/cluster/voters/n2", strings.NewReader(`{"addr":"x:1"}`), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("add existing voter: %d %s", resp.StatusCode, b)
	}
	resp, b = c.do("PUT", c.base("n1")+"/api/v1/cluster/voters/n7", strings.NewReader(`{}`), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing addr: %d %s", resp.StatusCode, b)
	}
}
