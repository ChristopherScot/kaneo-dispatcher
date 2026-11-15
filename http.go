package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// requestJSON owns authentication, status checking and bounded response reads.
// Endpoint code remains responsible for validating its success payload.
func requestJSON(ctx context.Context, client *http.Client, method, u, bearer string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: unexpected status %d", method, req.URL.Path, res.StatusCode)
	}
	const maxResponse = 8 << 20
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxResponse+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", req.URL.Path, err)
	}
	if len(raw) > maxResponse {
		return fmt.Errorf("%s: response exceeds %d bytes", req.URL.Path, maxResponse)
	}
	if len(bytes.TrimSpace(raw)) == 0 && out == nil {
		return nil
	}
	// Error bodies can arrive with HTTP 200. Do not log their contents,
	// which could contain upstream credentials or user data.
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) == nil {
		if _, ok := envelope["error"]; ok {
			return fmt.Errorf("%s: API returned an error object", req.URL.Path)
		}
	}
	if out == nil {
		if !json.Valid(raw) {
			return fmt.Errorf("%s: invalid JSON response", req.URL.Path)
		}
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber() // preserve integer fields when round-tripping full tasks
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", req.URL.Path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: expected one JSON value", req.URL.Path)
	}
	return nil
}
