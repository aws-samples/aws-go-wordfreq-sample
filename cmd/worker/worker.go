package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/awslabs/aws-go-wordfreq-sample"
)

type WorkerPool struct {
	workers []*Worker
	wg      sync.WaitGroup
}

func NewWorkerPool(size int, resultCh chan<- *wordfreq.JobResult, queue *JobMessageQueue, s3svc s3iface.S3API) *WorkerPool {
	pool := &WorkerPool{
		workers: make([]*Worker, size),
	}

	for i := 0; i < len(pool.workers); i++ {
		pool.wg.Add(1)
		pool.workers[i] = NewWorker(i, resultCh, queue, s3svc)

		go func(worker *Worker) {
			worker.run()
			pool.wg.Done()
		}(pool.workers[i])
	}

	return pool
}

func (w *WorkerPool) WaitForWorkersDone() {
	w.wg.Wait()
}

type Worker struct {
	id       int
	resultCh chan<- *wordfreq.JobResult
	queue    *JobMessageQueue
	s3Svc    s3iface.S3API
}

func NewWorker(id int, resultCh chan<- *wordfreq.JobResult, queue *JobMessageQueue, s3Svc s3iface.S3API) *Worker {
	return &Worker{id: id, resultCh: resultCh, queue: queue, s3Svc: s3Svc}
}

func (w *Worker) run() {
	fmt.Printf("Worker %d starting\n", w.id)
	defer fmt.Printf("Worker %d quitting.\n", w.id)

	for {
		select {
		case job, ok := <-w.queue.GetJobs():
			if !ok {
				return
			}
			fmt.Printf("Worker %d received job %s\n", w.id, job.OrigMessage.ID)
			result := &wordfreq.JobResult{
				Job:      job,
			}

			words, err := w.processJob(job)
			if err != nil {
				result.Status = wordfreq.JobCompleteFailure
				result.StatusMessage = err.Error()
				log.Println("Failed to process job", job.OrigMessage.ID, err)
			} else {
				result.Status = wordfreq.JobCompleteSuccess
				result.Words = words
			}
			result.Duration = time.Now().Sub(job.StartedAt)
			w.resultCh <- result
		}
	}
}

func (w *Worker) processJob(job *wordfreq.Job) (wordfreq.Words, error) {
	result, err := w.s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(job.Bucket),
		Key:    aws.String(job.Key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	return w.countTopWords(result.Body, 10, job)
}

func (w *Worker) countTopWords(reader io.Reader, top int, job *wordfreq.Job) ([]wordfreq.Word, error) {
	wordMap, err := w.countWords(reader, job)
	if err != nil {
		return nil, err
	}

	words := collectTopWords(wordMap, top)

	return words, nil
}

func (w *Worker) countWords(reader io.Reader, job *wordfreq.Job) (map[string]int, error) {
	wordMap := map[string]int{}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		word := strings.ToLower(scanner.Text())
		if len(word) <= 4 {
			continue
		}
		word = strings.Trim(word, `.,"'?!`)

		curCount := 0
		if v, ok := wordMap[word]; ok {
			curCount = v
		}

		wordMap[word] = 1 + curCount

		// To make sure another worker doesn't grab long running processes
		// bump up the job message's visibility timeout in the Queue.
		if time.Now().Sub(job.StartedAt) > time.Duration(job.VisibilityTimeout/2)*time.Second {
			if timeAdded, err := w.queue.UpdateMessageVisibility(job.OrigMessage.ReceiptHandle); err != nil {
				return nil, fmt.Errorf("Failed to update job messages's visibility timeout, %v", err)
			} else {
				job.VisibilityTimeout += timeAdded
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to count words, %v", err)
	}

	return wordMap, nil
}

func collectTopWords(wordMap map[string]int, top int) wordfreq.Words {
	words := wordfreq.Words{}
	for word, count := range wordMap {
		words = append(words, wordfreq.Word{Word: word, Count: count})
	}
	sort.Sort(words)

	if top >= len(words) {
		return words
	}
	return words[:top]
}
