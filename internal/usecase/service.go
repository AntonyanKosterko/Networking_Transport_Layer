package usecase

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stayfatal/Networking_Transport_Layer/internal/domain"
	"sync"
	"time"
)

var ErrExists = errors.New("manifest exists")
var ErrInconsistent = errors.New("manifest missing or inconsistent")

type assembly struct {
	manifest domain.Manifest
	parts    map[int][]byte
	last     time.Time
	emitted  bool
}
type Service struct {
	mu     sync.Mutex
	items  map[string]*assembly
	failed map[string]time.Time
}

func New() *Service { return &Service{items: map[string]*assembly{}, failed: map[string]time.Time{}} }
func Key(m domain.Manifest) string {
	return fmt.Sprintf("%s|%s|%s|%d", m.RequestID, m.SentAt, m.Kind, m.ChunkIndex)
}
func ManifestFor(s domain.Segment) domain.Manifest {
	return domain.Manifest{Request: domain.Request{RequestID: s.RequestID, SentAt: s.SentAt}, Kind: s.Kind, ChunkIndex: s.ChunkIndex, ChunkTotal: s.ChunkTotal, SegmentTotal: s.SegmentTotal}
}
func (s *Service) Register(m domain.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := Key(m)
	if _, ok := s.items[k]; ok {
		return ErrExists
	}
	s.items[k] = &assembly{manifest: m, parts: map[int][]byte{}, last: time.Now()}
	return nil
}
func (s *Service) ValidateSegment(seg domain.Segment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.items[Key(ManifestFor(seg))]
	if !ok || a.manifest.SegmentTotal != seg.SegmentTotal || a.manifest.ChunkTotal != seg.ChunkTotal {
		return ErrInconsistent
	}
	return nil
}
func (s *Service) Accept(seg domain.Segment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.items[Key(ManifestFor(seg))]
	if a != nil && !a.emitted && s.failed[seg.RequestID].IsZero() && a.manifest.SegmentTotal == seg.SegmentTotal {
		if _, ok := a.parts[seg.SegmentNumber]; !ok {
			a.parts[seg.SegmentNumber] = seg.Payload
			a.last = time.Now()
		}
	}
}
func (s *Service) Fail(c domain.Chunk) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	already := !s.failed[c.RequestID].IsZero()
	s.failed[c.RequestID] = time.Now()
	return !already
}
func (s *Service) Collect(now time.Time) []domain.Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	var outgoing []domain.Chunk
	for id, a := range s.items {
		if now.Sub(a.last) > 10*time.Minute {
			delete(s.items, id)
			continue
		}
		m := a.manifest
		if a.emitted || !s.failed[m.RequestID].IsZero() {
			continue
		}
		if len(a.parts) == m.SegmentTotal {
			if m.Kind == "names" {
				description := s.items[fmt.Sprintf("%s|%s|description|0", m.RequestID, m.SentAt)]
				if description == nil || !description.emitted {
					continue
				}
			}
			var data []byte
			for n := 1; n <= m.SegmentTotal; n++ {
				data = append(data, a.parts[n]...)
			}
			a.emitted = true
			if m.Kind == "names" {
				var names []string
				if json.Unmarshal(data, &names) != nil || len(names) < 2 || len(names) > 3 {
					s.failed[m.RequestID] = now
					outgoing = append(outgoing, domain.Chunk{Request: m.Request, IsError: true, Payload: []string{}, ErrorMessage: "Некорректная порция"})
				} else {
					outgoing = append(outgoing, domain.Chunk{Request: m.Request, ChunkIndex: m.ChunkIndex, ChunkTotal: m.ChunkTotal, Payload: names})
				}
			}
		} else if now.Sub(a.last) >= 6*time.Second {
			a.emitted = true
			s.failed[m.RequestID] = now
			outgoing = append(outgoing, domain.Chunk{Request: m.Request, ChunkIndex: m.ChunkIndex, ChunkTotal: m.ChunkTotal, IsError: true, Payload: []string{}, ErrorMessage: "Потеряны сегменты сообщения"})
		}
	}
	for id, at := range s.failed {
		if now.Sub(at) > 10*time.Minute {
			delete(s.failed, id)
		}
	}
	return outgoing
}
