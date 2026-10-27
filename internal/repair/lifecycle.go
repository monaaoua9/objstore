package repair

import (
	"context"
	"errors"
	"time"

	"github.com/monaaoua9/objstore/internal/meta"
)

type removal struct {
	key, version string // version == "" means "delete the key" (marker in a versioned bucket)
}

// lifecycle applies every bucket's rules. It only plans from listings and then issues ordinary
// deletes, so everything downstream (GC of shard files, markers) is the normal delete path.
func (s *Supervisor) lifecycle(ctx context.Context) {
	bs, err := s.Meta.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, b := range bs {
		if len(b.Lifecycle) == 0 || ctx.Err() != nil {
			continue
		}
		s.applyLifecycle(ctx, b)
	}
}

func (s *Supervisor) applyLifecycle(ctx context.Context, b meta.Bucket) {
	now := time.Now()
	age := func(t time.Time, days int) bool { return now.Sub(t) >= time.Duration(days)*s.Cfg.DayLength }
	budget := s.Cfg.LifecycleBatch
	var plan []removal

	for _, r := range b.Lifecycle {
		if r.Disabled {
			continue
		}
		if r.ExpireDays > 0 {
			cursor := ""
			for len(plan) < budget {
				items, next, err := s.Meta.ListObjects(ctx, b.Name, r.Prefix, cursor, 1000)
				if err != nil {
					return
				}
				for _, it := range items {
					if age(it.Created, r.ExpireDays) {
						plan = append(plan, removal{key: it.Key})
					}
				}
				if next == "" {
					break
				}
				cursor = next
			}
		}
		if (r.NoncurrentDays > 0 || r.CleanMarkers) && b.Versioning {
			plan = append(plan, s.planVersions(ctx, b.Name, r, age, budget-len(plan))...)
		}
	}

	var gone int
	for _, p := range plan {
		if ctx.Err() != nil || gone >= budget {
			break
		}
		var err error
		if p.version == "" {
			_, err = s.Meta.DeleteObject(ctx, b.Name, p.key)
		} else {
			err = s.Meta.DeleteVersion(ctx, b.Name, p.key, p.version)
		}
		if err != nil && !errors.Is(err, meta.ErrObjectNotFound) {
			s.Log.Warn("lifecycle delete failed", "bucket", b.Name, "key", p.key, "err", err)
			continue
		}
		gone++
	}
	if gone > 0 {
		s.lcDeleted.Add(uint64(gone))
		s.Log.Info("lifecycle expired objects", "bucket", b.Name, "count", gone)
	}
}

// planVersions walks the version listing key by key (newest first). A version turns noncurrent the
// moment its successor was written, so the successor's timestamp is the clock for the older one.
func (s *Supervisor) planVersions(ctx context.Context, bucket string, r meta.LifecycleRule, old func(time.Time, int) bool, room int) []removal {
	var plan []removal
	var group []meta.VersionItem
	flush := func() {
		if len(group) == 0 {
			return
		}
		if r.NoncurrentDays > 0 {
			for i := 1; i < len(group); i++ {
				if old(group[i-1].Created, r.NoncurrentDays) {
					plan = append(plan, removal{group[i].Key, group[i].Version})
				}
			}
		}
		if r.CleanMarkers && len(group) == 1 && group[0].DeleteMarker {
			plan = append(plan, removal{group[0].Key, group[0].Version})
		}
		group = group[:0]
	}

	ak, av := "", ""
	for len(plan) < room {
		items, nk, nv, err := s.Meta.ListVersions(ctx, bucket, r.Prefix, ak, av, 1000)
		if err != nil {
			return plan
		}
		for _, it := range items {
			if len(group) > 0 && group[0].Key != it.Key {
				flush()
			}
			group = append(group, it)
		}
		if nk == "" {
			break
		}
		ak, av = nk, nv
	}
	flush()
	return plan
}
