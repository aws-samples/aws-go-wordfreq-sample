package wordfreq

import "time"

type Job struct {
	StartedAt           time.Time
	VisibilityTimeout   int64      `json:"-"`
	OrigMessage         JobMessage `json:"-"`
	Region, Bucket, Key string
}

type JobMessage struct {
	ID            string
	ReceiptHandle string
	Body          string
}

type JobResult struct {
	Job           *Job
	Words         Words
	Duration      time.Duration
	Status        JobCompleteStatus
	StatusMessage string
}

type JobCompleteStatus string

const (
	JobCompleteSuccess JobCompleteStatus = "success"
	JobCompleteFailure                   = "failure"
)

type Word struct {
	Word  string
	Count int
}

type Words []Word

func (w Words) Len() int {
	return len(w)
}
func (w Words) Less(i, j int) bool {
	return w[i].Count > w[j].Count
}
func (w Words) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

type OrigMessage struct {
	ID            string
	ReceiptHandle string
	Body          string
}
