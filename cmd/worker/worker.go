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

// A WorkerPool provides a collection of workers, and access to their lifecycle.
type WorkerPool struct {
	workers []*Worker
	wg      sync.WaitGroup
}

// NewWorkerPool creates a new instance of the worker pool, and creates all the
// workers in the pool. The workers are spun off in their own goroutines and the
// WorkerPool's wait group is used to know when the workers all completed their
// work and existed.
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

// WaitForWorkersDone waits for the works to of all completed their work and
// exited.
func (w *WorkerPool) WaitForWorkersDone() {
	w.wg.Wait()
}

// A Worker is a individual processor of jobs from the job channel.
type Worker struct {
	id       int
	resultCh chan<- *wordfreq.JobResult
	queue    *JobMessageQueue
	s3Svc    s3iface.S3API
}

// NewWorker creates an initializes a new worker.
func NewWorker(id int, resultCh chan<- *wordfreq.JobResult, queue *JobMessageQueue, s3Svc s3iface.S3API) *Worker {
	return &Worker{id: id, resultCh: resultCh, queue: queue, s3Svc: s3Svc}
}

// run reads from the job channel until it is closed and drained.
func (w *Worker) run() {
	fmt.Printf("Worker %d starting\n", w.id)
	defer fmt.Printf("Worker %d quitting.\n", w.id)

	for {
		job, ok := <-w.queue.GetJobs():
		if !ok {
			return
		}
		fmt.Printf("Worker %d received job %s\n", w.id, job.OrigMessage.ID)
		result := &wordfreq.JobResult{
			Job: job,
		}

		// Stream the file from S3, counting the words and return the words
		// and error if one occurred. If an error occurred the words will be
		// ignored, and a failed result status is set. Otherwise the success
		// status is set along with the words.
		words, err := w.processJob(job)
		if err != nil {
			result.Status = wordfreq.JobCompleteFailure
			result.StatusMessage = err.Error()
			log.Println("Failed to process job", job.OrigMessage.ID, err)
		} else {
			result.Status = wordfreq.JobCompleteSuccess
			result.Words = words
		}
		// The duration is collected so that the results can report the
		// the amount of time a job took to process.
		result.Duration = time.Now().Sub(job.StartedAt)
		w.resultCh <- result
	}
}

// processJob gets a io.Reader to the uploaded file from S3 and starts counting
// the words. Returning the words counted or error.
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

// countTopWords counts the top words returning those words or error.
func (w *Worker) countTopWords(reader io.Reader, top int, job *wordfreq.Job) ([]wordfreq.Word, error) {
	wordMap, err := w.countWords(reader, job)
	if err != nil {
		return nil, err
	}

	words := collectTopWords(wordMap, top)

	return words, nil
}

// countWords collects the counts of all words received from an io.Reader. Using
// a word scanner unique words are counted. This is a fairly simplistic implementation
// of word counting and only splits words based on whitespace. Extra characters
// such as `.,"'?!` are trimmed from the front and end of each string
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
			timeAdded, err := w.queue.UpdateMessageVisibility(job.OrigMessage.ReceiptHandle);
			if err != nil {
				return nil, fmt.Errorf("Failed to update job messages's visibility timeout, %v", err)
			}
			job.VisibilityTimeout += timeAdded

		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to count words, %v", err)
	}

	return wordMap, nil
}

// collectTopWords converts the word map into an array, and sorts it. Collecting
// the top words.
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
