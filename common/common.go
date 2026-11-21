package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Request struct {
	Sender             string `json:"sender"`
	SentAt             string `json:"sent_at"`
	RequestID          string `json:"request_id"`
	ProductDescription string `json:"product_description"`
}
type Chunk struct {
	Request
	ChunkIndex   int      `json:"chunk_index"`
	ChunkTotal   int      `json:"chunk_total"`
	IsError      bool     `json:"is_error"`
	Payload      []string `json:"payload"`
	ErrorMessage string   `json:"error_message,omitempty"`
}
type Segment struct {
	RequestID     string `json:"request_id"`
	SentAt        string `json:"sent_at"`
	Kind          string `json:"kind"`
	ChunkIndex    int    `json:"chunk_index"`
	ChunkTotal    int    `json:"chunk_total"`
	SegmentTotal  int    `json:"segment_total"`
	SegmentNumber int    `json:"segment_number"`
	Payload       []byte `json:"payload"`
}
type Manifest struct {
	Request
	Kind         string `json:"kind"`
	ChunkIndex   int    `json:"chunk_index"`
	ChunkTotal   int    `json:"chunk_total"`
	SegmentTotal int    `json:"segment_total"`
}

func (r Request) Validate() error {
	if strings.TrimSpace(r.Sender) == "" || strings.TrimSpace(r.ProductDescription) == "" || len(r.ProductDescription) > 50000 || len(r.Sender) > 100 || r.RequestID == "" {
		return errors.New("invalid request")
	}
	if _, e := time.Parse(time.RFC3339Nano, r.SentAt); e != nil {
		return errors.New("invalid sent_at")
	}
	return nil
}
func Env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func Decode(r *http.Request, v any) error {
	defer r.Body.Close()
	d := json.NewDecoder(io.LimitReader(r.Body, 256*1024))
	if err := d.Decode(v); err != nil {
		return err
	}
	return nil
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"code": "error", "message": msg})
}

var Client = &http.Client{Timeout: 15 * time.Second}

func Post(url string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	res, e := Client.Post(url, "application/json", bytes.NewReader(b))
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 2048))
		return fmt.Errorf("%s: HTTP %d: %s", url, res.StatusCode, b)
	}
	return nil
}
func Only(method string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			Error(w, 405, "method not allowed")
			return
		}
		h(w, r)
	}
}
