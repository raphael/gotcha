package main

import (
  "log"
	"net/http"
  "time"
)

// "Hijack" http.ResponseWriter to capture response status code
type loggerWriter struct {
  http.ResponseWriter // Anonymous field so loggerWriter auto-delegates to ResponseWriter
  logger *httpLogger  // Pointer to logger used to record HTTP response code
}

// Record response code and delegate to original writer
func (w loggerWriter) WriteHeader(h int) {
  w.logger.ResponseCode = h
  w.ResponseWriter.WriteHeader(h)
}

// "Hijack" http.Handler to add logging
type httpLogger struct {
  http.Handler     // Anonymous field to store actual HTTP handler 
  ResponseCode int // HTTP response code
}

// Add logging before and after request is handled and delegate to given HTTP handler
func (l httpLogger) ServeHTTP(w http.ResponseWriter, req *http.Request) {
  uri, addr, meth := req.RequestURI, req.RemoteAddr, req.Method
  writer := loggerWriter{ResponseWriter: w, logger: &l}
  start := time.Now()
  log.Printf("== %v %v (%v)", meth, uri, addr)

  l.Handler.ServeHTTP(writer, req)

  ms := time.Now().Sub(start).Nanoseconds() / 1000000
  code := l.GetCode()
  status := http.StatusText(code)
  log.Printf("== Completed in %vms | %v %v [%v %v] (%v)", ms, code, status, meth, uri, addr)
}

// Synthetize HTTP response code
func (l httpLogger) GetCode() int {
  if l.ResponseCode == 0 {
    return 200
  }
  return l.ResponseCode
}


