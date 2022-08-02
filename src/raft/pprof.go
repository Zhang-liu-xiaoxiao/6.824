package raft

import (
	"net/http"
	"net/http/pprof"
)

const (
	pprofAddr string = ":17890"
)

func StartHTTPDebuger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}
