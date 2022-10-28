package integration

import (
	"fmt"
	"net/http"
	"time"
)

type MockServer struct {
	srv *http.Server

	Port   string
	Status []byte
	Nodes  []byte
}

func (m *MockServer) getStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(m.Status)
}

func (m *MockServer) getNodes(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(m.Nodes)
}

func (m *MockServer) Start() error {
	if m.Port == "" {
		m.Port = "14001"
	}

	mux := http.NewServeMux()
	m.srv = &http.Server{
		Addr:    fmt.Sprintf(":%s", m.Port),
		Handler: mux,
	}

	mux.HandleFunc("/status", m.getStatus)
	mux.HandleFunc("/nodes", m.getNodes)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	go func() {
		m.srv.ListenAndServe()
	}()

	return nil
}

func (m *MockServer) Stop() error {
	if m.srv == nil {
		return nil
	}
	return m.srv.Close()
}

func (m *MockServer) WaitForReady() error {
	start := time.Now()

	for {
		url := fmt.Sprintf("http://localhost:%s", m.Port)
		newRequest, err := http.NewRequest("GET", url, nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(newRequest)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
		}

		time.Sleep(time.Second)

		if time.Since(start) > time.Second*10 {
			return fmt.Errorf("timeout waiting for mock server to become ready on port %s", m.Port)
		}
	}
}
