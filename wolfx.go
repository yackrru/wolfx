package wolfx

import (
	"context"
	"fmt"
	"github.com/yackrru/gogger"
	"github.com/yackrru/wolfx/middleware"
	"golang.org/x/sync/errgroup"
	"os"
	"reflect"
	"runtime"
	"time"
)

const (
	art = `
    __  __  __     __  _____    __
   / / / / / /    / / / _/\ \  / /
  / / / / / /__  / /_/ /_  \ \/ /
 / /_/ /_/ / _ \/ /_  __/  / /\ \
/____/\___/\___/_/ /_/    /_/  \_\
A batch framework for golang.
`
)

// WolfX is the top-level framework instance.
// It is the collection of batch jobs.
type WolfX struct {
	JobExecutors []JobExecutor

	// If ArtOFF is true, ASCII art will not be displayed at launching.
	ArtOFF bool

	// LogLevel is gogger's LogLevel.
	LogLevel gogger.LogLevel
}

// New returns a WolfX instance.
func New() *WolfX {
	return new(WolfX)
}

// Run boots WolfX application.
//
// Arg jobName must be corresponded one of the name of WolfX.JobExecutors.
func (wx *WolfX) Run(jobName string) error {
	logWriter := gogger.NewLogStreamWriter(gogger.LogStreamWriterOption{
		Output:        os.Stderr,
		SyncQueueSize: 1000,
	})
	logWriter.Open()
	defer logWriter.Close(1 * time.Minute)
	conf := &gogger.LogConfig{
		Writers:   []gogger.LogWriter{logWriter},
		Formatter: gogger.NewLogSimpleFormatter(gogger.DefaultLogSimpleFormatterTmpl),
	}
	if wx.LogLevel > gogger.LevelDefault {
		conf.LogMinLevel = wx.LogLevel
	}
	middleware.Logger = gogger.NewLog(conf)

	if !wx.ArtOFF {
		fmt.Fprintf(os.Stdout, "%s\n", art)
	}
	middleware.Logger.Info("Launched WolfX application.")

	for _, e := range wx.JobExecutors {
		if jobName == e.Name() {
			middleware.Logger.Infof("Target job: %s", jobName)
			err := e.Run()

			if err == nil {
				middleware.Logger.Info("Completed WolfX application.")
			} else {
				middleware.Logger.Error("Errors have occurred.")
			}
			middleware.Logger.Info("Terminate WolfX application...")

			return err
		}
	}

	errStr := "Not found job name: " + jobName
	middleware.Logger.Error(errStr)
	middleware.Logger.Info("Terminate WolfX application...")
	return fmt.Errorf(errStr)
}

// Add adds JobExecutor to the WolfX instance.
func (wx *WolfX) Add(e JobExecutor) *WolfX {
	wx.JobExecutors = append(wx.JobExecutors, e)
	return wx
}

// JobExecutor is the top-level batch job.
//
// A job has a unique name and a single bootstrap.
// It is designed so that the bootstrap is invoked
// by passing the unique name to the WolfX instance.
type JobExecutor interface {
	// Name returns the unique job name.
	Name() string

	// Run invokes bootstrap.
	Run() error
}

var (
	_ JobBuilderAPI  = new(JobBuilder)
	_ StepBuilderAPI = new(StepBuilder)
)

// JobBuilderAPI is the builder interface for bootstrap
// and is designed to be invoked by JobExecutor.Run.
type JobBuilderAPI interface {
	// Build invokes multiple Step.
	Build() error

	SingleJob
	ConcurrentJob
}

// SingleJob is the interface that wraps the method of Single.
//
// Single invokes a Step.
type SingleJob interface {
	Single(s Step) *JobBuilder
}

// ConcurrentJob is the interface that wraps the method of Concurrent.
//
// Concurrent invokes multiple Step concurrently.
type ConcurrentJob interface {
	Concurrent(s ...Step) *JobBuilder
}

// JobBuilder implements JobBuilderAPI.
type JobBuilder struct {
	Flows []Flow
}

// Flow holds the steps in execution units.
type Flow []Step

func NewJobBuilder() *JobBuilder {
	builder := new(JobBuilder)
	return builder
}

func (b *JobBuilder) Build() error {
	for _, flow := range b.Flows {
		if len(flow) == 1 {
			middleware.Logger.Info("Start single step")
		} else {
			middleware.Logger.Infof("Start %d steps parallelly", len(flow))
		}

		eg, ctx := errgroup.WithContext(context.Background())
		for _, step := range flow {
			eg.Go(func() error {
				fValue := reflect.ValueOf(step)
				fName := runtime.FuncForPC(fValue.Pointer()).Name()
				middleware.Logger.Infof("Execute step: %s", fName)
				return step(ctx)
			})
		}

		if err := eg.Wait(); err != nil {
			middleware.Logger.Error("Step execution canceled: ", err)
			return err
		}
	}

	return nil
}

func (b *JobBuilder) Single(s Step) *JobBuilder {
	b.Flows = append(b.Flows, Flow{s})
	return b
}

func (b *JobBuilder) Concurrent(steps ...Step) *JobBuilder {
	var flow Flow
	for _, s := range steps {
		flow = append(flow, s)
	}

	b.Flows = append(b.Flows, flow)

	return b
}

// StepBuilderAPI is the builder interface for bootstrap
// and is designed to be invoked by each step definition.
type StepBuilderAPI interface {
	// Build invokes user's settings.
	Build() error

	StepMiddlewareSetter
}

// StepMiddlewareSetter is the interface that wraps methods of SetReader and SetWriter.
type StepMiddlewareSetter interface {
	ReaderSetter
	WriterSetter
}

// ReaderSetter sets Reader to StepBuilder.
type ReaderSetter interface {
	SetReader(r middleware.Reader) *StepBuilder
}

// WriterSetter is sets Writer to StepBuilder.
type WriterSetter interface {
	SetWriter(r middleware.Writer) *StepBuilder
}

// Step is the smallest unit of execution.
type Step func(ctx context.Context) error

// StepBuilder implements StepBuilderAPI.
type StepBuilder struct {
	ctx    context.Context
	Reader middleware.Reader
	Writer middleware.Writer
}

func NewStepBuilder(ctx context.Context) *StepBuilder {
	return &StepBuilder{
		ctx: ctx,
	}
}

func (b *StepBuilder) Build() error {
	if b.Reader == nil {
		return fmt.Errorf("ERROR: Reader must be set.")
	}
	if b.Writer == nil {
		return fmt.Errorf("ERROR: Writer must be set.")
	}

	eg, ctx := errgroup.WithContext(b.ctx)
	ch := make(chan interface{})
	// Run reader
	eg.Go(func() error {
		return readerWorker(ctx, ch, b.Reader)
	})
	// Run writer
	eg.Go(func() error {
		return writerWorker(ctx, ch, b.Writer)
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (b *StepBuilder) SetReader(r middleware.Reader) *StepBuilder {
	b.Reader = r
	return b
}

func (b *StepBuilder) SetWriter(w middleware.Writer) *StepBuilder {
	b.Writer = w
	return b
}

func readerWorker(ctx context.Context, ch chan<- interface{}, reader middleware.Reader) error {
	var err error
	worker := func() <-chan interface{} {
		terminated := make(chan interface{})
		go func() {
			defer close(terminated)
			rv := reflect.ValueOf(reader)
			middleware.Logger.Infof("Use reader: %s", rv.Type())
			err = reader.Read(ctx, ch)
		}()
		return terminated
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker():
	}

	return err
}

func writerWorker(ctx context.Context, ch <-chan interface{}, writer middleware.Writer) error {
	var err error
	worker := func() <-chan interface{} {
		terminated := make(chan interface{})
		go func() {
			defer close(terminated)
			wv := reflect.ValueOf(writer)
			middleware.Logger.Infof("Use writer: %s", wv.Type())
			err = writer.Write(ctx, ch)
		}()
		return terminated
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker():
	}

	return err

}
