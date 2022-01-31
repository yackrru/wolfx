package wolfx_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/ttksm/wolfx"
	"github.com/ttksm/wolfx/middleware"
	"testing"
	"time"
)

func TestJobExecution(t *testing.T) {
	wx := wolfx.New()
	wx.ArtOFF = true

	fooJob := &FooJob{
		t: t,
	}
	echoWriter := &EchoWriter{
		t: t,
	}
	bazJob := &BazJob{
		reader: new(EchoReader),
		writer: echoWriter,
	}

	wx.Add(fooJob).
		Add(NewBarJob(t)).
		Add(bazJob)

	if err := wx.Run("FooJob"); err != nil {
		t.Fatal(err.Error())
	}
	if err := wx.Run("BarJob"); err != nil {
		t.Fatal(err.Error())
	}
	if err := wx.Run("BazJob"); err != nil {
		t.Fatal(err.Error())
	}
	if err := wx.Run("TestJob"); err != nil {
		assert.EqualError(t, err, "Not found job name: TestJob")
	} else {
		t.Fatal("Unexpected execution for TestJob")
	}
}

// FooJob is the simplest JobExecutor has a single Step.
type FooJob struct {
	t *testing.T
}

func (j *FooJob) Name() string {
	return "FooJob"
}

func (j *FooJob) Run() error {
	return wolfx.NewJobBuilder().
		Single(j.EchoStep).
		Build()
}

func (j *FooJob) EchoStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(new(EchoReader)).
		SetWriter(&EchoWriter{
			t: j.t,
		}).
		Build()
}

// BarJob is a JobExecutor has two sequential steps.
type BarJob struct {
	t *testing.T
}

// NewBarJob is the constructor of BarJob.
func NewBarJob(t *testing.T) *BarJob {
	return &BarJob{
		t: t,
	}
}

func (j *BarJob) Name() string {
	return "BarJob"
}

func (j *BarJob) Run() error {
	return wolfx.NewJobBuilder().
		Single(j.EchoStep).
		Single(j.WEchoStep).
		Build()
}

func (j *BarJob) EchoStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(new(EchoReader)).
		SetWriter(&EchoWriter{
			t: j.t,
		}).
		Build()
}

func (j *BarJob) WEchoStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(new(WEchoReader)).
		SetWriter(&WEchoWriter{
			t: j.t,
		}).
		Build()
}

// BazJob is a JobExecutor has a parallel step.
type BazJob struct {
	reader middleware.Reader
	writer middleware.Writer
}

func (j *BazJob) Name() string {
	return "BazJob"
}

func (j *BazJob) Run() error {
	return wolfx.NewJobBuilder().
		Concurrent(j.EchoStep, j.EchoStep).
		Build()
}

func (j *BazJob) EchoStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(j.reader).
		SetWriter(j.writer).
		Build()
}

var (
	_ middleware.Reader = new(EchoReader)
	_ middleware.Writer = new(EchoWriter)
	_ middleware.Reader = new(WEchoReader)
	_ middleware.Writer = new(WEchoWriter)
)

type EchoReader struct{}

func (r *EchoReader) Read(ctx context.Context, ch chan<- interface{}) error {
	ch <- "echo"
	return nil
}

type EchoWriter struct {
	t *testing.T
}

func (w *EchoWriter) Write(ctx context.Context, ch <-chan interface{}) error {
	echo := <-ch
	assert.Equal(w.t, "echo", echo)
	return nil
}

type WEchoReader struct{}

func (r *WEchoReader) Read(ctx context.Context, ch chan<- interface{}) error {
	ch <- "echo1"
	ch <- "echo2"
	return nil
}

type WEchoWriter struct {
	t *testing.T
}

func (w *WEchoWriter) Write(ctx context.Context, ch <-chan interface{}) error {
	echo1 := <-ch
	echo2 := <-ch
	assert.Equal(w.t, "echo1", echo1)
	assert.Equal(w.t, "echo2", echo2)
	return nil
}

func TestJobExecutionWithCancel(t *testing.T) {
	wx := wolfx.New()
	wx.ArtOFF = true
	cancelJob := &CancelJob{
		cancelReader: new(CancelReader),
		cancelWriter: new(CancelWriter),
	}
	wx.Add(cancelJob)
	if err := wx.Run("CancelJob"); err == nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, 3, cancelJob.cancelReader.testNumber)
	// Never called CancelWriterStep because the process killed by error of CancelReaderStep.
	assert.Equal(t, 0, cancelJob.cancelWriter.count)
}

type CancelJob struct {
	cancelReader *CancelReader
	cancelWriter *CancelWriter
}

func (j *CancelJob) Name() string {
	return "CancelJob"
}

func (j *CancelJob) Run() error {
	return wolfx.NewJobBuilder().
		Single(j.CancelReaderStep).
		Single(j.CancelWriterStep).
		Build()
}

func (j *CancelJob) CancelReaderStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(j.cancelReader).
		SetWriter(new(SleepWriter)).
		Build()
}

func (j *CancelJob) CancelWriterStep(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(new(SleepReader)).
		SetWriter(j.cancelWriter).
		Build()
}

func TestJobExecutionWithWriterCancel(t *testing.T) {
	wx := wolfx.New()
	wx.ArtOFF = true
	job := &CancelWriterJob{
		writer: new(CancelWriter),
	}
	wx.Add(job)
	if err := wx.Run("CancelWriterJob"); err == nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, 1, job.writer.count)
}

type CancelWriterJob struct {
	writer *CancelWriter
}

func (j *CancelWriterJob) Name() string {
	return "CancelWriterJob"
}

func (j *CancelWriterJob) Run() error {
	return wolfx.NewJobBuilder().Single(j.Step).Build()
}

func (j *CancelWriterJob) Step(ctx context.Context) error {
	return wolfx.NewStepBuilder(ctx).
		SetReader(new(SleepReader)).
		SetWriter(j.writer).
		Build()
}

type CancelReader struct {
	testNumber int
}

func (r *CancelReader) Read(ctx context.Context, ch chan<- interface{}) error {
	r.testNumber = 3
	return fmt.Errorf("CancelReader error")
}

type SleepReader struct{}

func (r *SleepReader) Read(ctx context.Context, ch chan<- interface{}) error {
	time.Sleep(10 * time.Second)
	return nil
}

type CancelWriter struct {
	count int
}

func (w *CancelWriter) Write(ctx context.Context, ch <-chan interface{}) error {
	w.count++
	return fmt.Errorf("CancelWriter Error")
}

type SleepWriter struct{}

func (w *SleepWriter) Write(ctx context.Context, ch <-chan interface{}) error {
	time.Sleep(10 * time.Second)
	return nil
}
