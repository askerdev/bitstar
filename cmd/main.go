package main

import (
	"log"
	"net/http"

	"github.com/askerdev/bitstar"
)

func main() {
	s := &http.Server{
		Handler: bitstar.NewHandler(),
		Addr:    ":8080",
	}
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
