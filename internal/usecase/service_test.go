package usecase

import (
	"github.com/stayfatal/Networking_Transport_Layer/internal/domain"
	"testing"
	"time"
)

func TestAssemblyAndDescriptionOrdering(t *testing.T) {
	s := New()
	req := domain.Request{RequestID: "1", SentAt: "time"}
	d := domain.Manifest{Request: req, Kind: "description", SegmentTotal: 1}
	n := domain.Manifest{Request: req, Kind: "names", ChunkIndex: 1, ChunkTotal: 1, SegmentTotal: 2}
	s.Register(d)
	s.Register(n)
	for _, p := range []domain.Segment{{RequestID: "1", SentAt: "time", Kind: "names", ChunkIndex: 1, ChunkTotal: 1, SegmentTotal: 2, SegmentNumber: 2, Payload: []byte(`,"b"]`)}, {RequestID: "1", SentAt: "time", Kind: "names", ChunkIndex: 1, ChunkTotal: 1, SegmentTotal: 2, SegmentNumber: 1, Payload: []byte(`["a"`)}} {
		s.Accept(p)
	}
	if got := s.Collect(time.Now()); len(got) != 0 {
		t.Fatal(got)
	}
	s.Accept(domain.Segment{RequestID: "1", SentAt: "time", Kind: "description", SegmentTotal: 1, SegmentNumber: 1, Payload: []byte("text")})
	var out []domain.Chunk
	for i := 0; i < 2; i++ {
		out = append(out, s.Collect(time.Now())...)
	}
	if len(out) != 1 || len(out[0].Payload) != 2 || out[0].Payload[0] != "a" || out[0].Payload[1] != "b" {
		t.Fatal(out)
	}
}
func TestMissingSegmentFailure(t *testing.T) {
	s := New()
	s.Register(domain.Manifest{Request: domain.Request{RequestID: "1"}, Kind: "description", SegmentTotal: 2})
	out := s.Collect(time.Now().Add(7 * time.Second))
	if len(out) != 1 || !out[0].IsError {
		t.Fatal(out)
	}
	if len(s.Collect(time.Now().Add(8*time.Second))) != 0 {
		t.Fatal("duplicate failure")
	}
}
