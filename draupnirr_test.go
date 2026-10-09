package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoRetriesOn429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "content=x&format=bbcode" {
			t.Errorf("corps renvoyé = %q", body)
		}
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"message":"Too Many Attempts."}`))
			return
		}
		w.Write([]byte(`{"html":"ok"}`))
	}))
	defer srv.Close()
	html, err := newClient(srv.URL, "t").Preview(context.Background(), "x", "bbcode")
	if err != nil || html != "ok" || calls.Load() != 3 {
		t.Fatalf("html=%q err=%v appels=%d", html, err, calls.Load())
	}
}

func TestDoGivesUpAfterMaxAttempts(t *testing.T) {
	old := retryBase
	retryBase = time.Millisecond
	defer func() { retryBase = old }()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"message":"Too Many Attempts."}`))
	}))
	defer srv.Close()
	_, err := newClient(srv.URL, "t").Me(context.Background())
	ae, ok := err.(*apiError)
	if !ok || ae.Status != 429 || calls.Load() != maxAttempts {
		t.Fatalf("err=%v appels=%d", err, calls.Load())
	}
}

func TestPauseIsShared(t *testing.T) {
	g := gateFor("https://shared.example", http.MethodGet)
	g.pause(time.Hour)
	if gateFor("https://shared.example", http.MethodGet) != g {
		t.Fatal("la pause doit valoir pour tous les clients du site")
	}
	if gateFor("https://shared.example", http.MethodPost) == g {
		t.Fatal("lecture et écriture ont des limites distinctes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := g.wait(ctx); err == nil {
		t.Fatal("wait doit respecter l'annulation")
	}
}

func TestRetryAfter(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "17")
	if d := retryAfter(h, 0); d != 17*time.Second {
		t.Fatalf("secondes : %v", d)
	}
	h = http.Header{}
	h.Set("Retry-After", time.Now().Add(30*time.Second).UTC().Format(http.TimeFormat))
	if d := retryAfter(h, 0); d < 28*time.Second || d > 30*time.Second {
		t.Fatalf("date HTTP : %v", d)
	}
	h = http.Header{}
	h.Set("Retry-After", "3600")
	if d := retryAfter(h, 0); d != retryCap {
		t.Fatalf("plafond : %v", d)
	}
	if d := retryAfter(http.Header{}, 2); d != retryBase<<2 {
		t.Fatalf("backoff : %v", d)
	}
}
