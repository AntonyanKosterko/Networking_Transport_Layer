package httpapi

import (
	"context"
	"github.com/stayfatal/Networking_Transport_Layer/internal/domain"
	"github.com/stayfatal/Networking_Transport_Layer/internal/usecase"
	"math/rand"
	"net/http"
)

type State interface {
	Register(domain.Manifest) error
	ValidateSegment(domain.Segment) error
	Fail(domain.Chunk) bool
}
type Publisher interface {
	Publish(context.Context, domain.Segment) error
}
type Poster interface{ Post(string, any) error }

func New(state State, publisher Publisher, poster Poster, agent, app string, loss float64) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/input", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var req domain.Request
		if Decode(r, &req) != nil || req.Validate() != nil {
			Error(w, 400, "invalid request")
			return
		}
		if err := poster.Post(agent+"/api/v1/process", req); err != nil {
			Error(w, 503, err.Error())
			return
		}
		JSON(w, 202, map[string]string{"request_id": req.RequestID, "status": "accepted"})
	}))
	mux.HandleFunc("/api/v1/manifest", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var m domain.Manifest
		if Decode(r, &m) != nil || m.Request.Validate() != nil || m.SegmentTotal < 1 || m.SegmentTotal > 100 || !(m.Kind == "description" || m.Kind == "names") || (m.Kind == "names" && (m.ChunkIndex < 1 || m.ChunkIndex > m.ChunkTotal || m.ChunkTotal > 5)) {
			Error(w, 400, "invalid manifest")
			return
		}
		if err := state.Register(m); err != nil {
			Error(w, 409, err.Error())
			return
		}
		JSON(w, 202, map[string]string{"status": "accepted"})
	}))
	mux.HandleFunc("/api/v1/send", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var s domain.Segment
		if Decode(r, &s) != nil || s.RequestID == "" || s.SentAt == "" || len(s.Payload) > 1000 || len(s.Payload) == 0 || s.SegmentNumber < 1 || s.SegmentNumber > s.SegmentTotal {
			Error(w, 400, "invalid segment")
			return
		}
		if err := state.ValidateSegment(s); err != nil {
			Error(w, 400, err.Error())
			return
		}
		status := "dropped"
		if rand.Float64() >= loss {
			if err := publisher.Publish(r.Context(), s); err != nil {
				Error(w, 503, "kafka unavailable")
				return
			}
			status = "queued"
		}
		JSON(w, 202, map[string]any{"request_id": s.RequestID, "segment_number": s.SegmentNumber, "status": status})
	}))
	mux.HandleFunc("/api/v1/failure", Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var c domain.Chunk
		if Decode(r, &c) != nil || c.Request.Validate() != nil {
			Error(w, 400, "invalid error")
			return
		}
		if state.Fail(c) {
			c.IsError = true
			c.Payload = []string{}
			if err := poster.Post(app+"/api/v1/receive", c); err != nil {
				Error(w, 503, err.Error())
				return
			}
		}
		JSON(w, 202, map[string]string{"status": "accepted"})
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return mux
}

var _ State = (*usecase.Service)(nil)
