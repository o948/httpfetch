package httpfetch

import (
	"log"
	"net/http"
)

type Fetcher struct {
	client *http.Client
	logger *log.Logger
}

type Task interface {
	GetRequest() *http.Request
	HandleResponse(*http.Response) []Task
	HandleError(error) []Task
}

var DefaultFetcher = NewFetcher(http.DefaultClient, log.Default())

// TODO: add context.Context
func Run(workers int, tasks ...Task) {
	DefaultFetcher.Run(workers, tasks...)
}

func NewFetcher(client *http.Client, logger *log.Logger) *Fetcher {
	return &Fetcher{client, logger}
}

func (f *Fetcher) Run(workers int, tasks ...Task) {
	todo := make(chan Task)
	done := make(chan result)

	for i := 0; i < workers; i++ {
		go func(id int) {
			for {
				t := <-todo
				if t == nil {
					return
				}

				req := t.GetRequest()
				if f.logger != nil {
					f.logger.Printf("worker %d fetching %s\n", id, req.URL.String())
				}
				resp, err := f.client.Do(req)
				done <- result{t, resp, err}
			}
		}(i+1)
	}

	busy := 0
	for len(tasks) > 0 || busy > 0 {
		for len(tasks) > 0 && busy < workers {
			t := tasks[0]
			tasks[0] = nil
			tasks = tasks[1:]

			todo <- t
			busy++
		}

		r := <-done
		busy--

		var ts []Task
		if r.Err != nil {
			ts = r.Task.HandleError(r.Err)
		} else {
			ts = r.Task.HandleResponse(r.Resp)
		}

		tasks = append(tasks, ts...)
	}

	close(todo)
}

type result struct {
	Task Task
	Resp *http.Response
	Err  error
}
