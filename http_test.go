package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSONResponseValidation(t *testing.T) {
	for _, body := range []string{`{"error":"upstream unavailable"}`, `null`, `{}`, `{"data":{}}`, `{"data":{"columns":null}}`} {
		t.Run(body, func(t *testing.T) {
			f := newFixture(t)
			f.kaneo.board = json.RawMessage(body)
			if _, err := f.d.reconcile(t.Context(), false); err == nil {
				t.Fatalf("reconcile accepted invalid board %s", body)
			}
			f.wantUntouched()
		})
	}
}

func TestRequestJSON(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantErr    bool
	}{
		{"empty put", "", 204, false}, {"error object", `{"error":"nope"}`, 200, true},
		{"bad status", `{}`, 500, true}, {"invalid json", `oops`, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" || r.Header.Get("Authorization") != "Bearer test" || r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("invalid request headers/method")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			err := requestJSON(context.Background(), srv.Client(), "PUT", srv.URL, "test", map[string]string{"status": "done"}, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("requestJSON() error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestRequestJSONPreservesIntegersAndRejectsTrailingValues(t *testing.T) {
	body := `{"number":9007199254740993}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer srv.Close()
	var out map[string]any
	if err := getJSON(t.Context(), srv.Client(), srv.URL, "", &out); err != nil {
		t.Fatal(err)
	}
	if out["number"] != json.Number("9007199254740993") {
		t.Fatalf("number=%v", out["number"])
	}
	body = `{} {}`
	if err := getJSON(t.Context(), srv.Client(), srv.URL, "", &out); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}
