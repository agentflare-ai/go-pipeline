package pipeline // import "github.com/agentflare-ai/go-pipeline"

Package pipeline provides a generic, efficient middleware pattern for chaining
operations.

The pipeline pattern allows you to compose multiple processing steps (pipes)
that can:

* Transform data as it flows through the chain
* Short-circuit execution by not calling next()
* Handle errors and context cancellation gracefully

# Basic Usage

Create a pipeline with pipes that process data sequentially:

```
// Define pipes that log and transform data
logPipe := func(ctx context.Context, sink *os.File, input string, next func(context.Context, *os.File, string) error) error {
	fmt.Fprintf(sink, "Processing: %s\n", input)
	return next(ctx, sink, input) // Continue to next pipe
}

uppercasePipe := func(ctx context.Context, sink *os.File, input string, next func(context.Context, *os.File, string) error) error {
	transformed := strings.ToUpper(input)
	fmt.Fprintf(sink, "Transformed: %s\n", transformed)
	return next(ctx, sink, transformed)
}

// Create and execute pipeline
p := pipeline.New(context.Background(), logPipe, uppercasePipe)
err := p.Process(os.Stdout, "hello world")
```

# Short-Circuiting

Pipes can stop execution by returning early without calling next():

```
validationPipe := func(ctx context.Context, sink *os.File, input string, next func(context.Context, *os.File, string) error) error {
	if input == "" {
		return fmt.Errorf("input cannot be empty")
	}
	return next(ctx, sink, input) // Only continue if valid
}
```

# Context Cancellation

The pipeline respects context cancellation and terminates early:

```
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

p := pipeline.New(ctx, longRunningPipe1, longRunningPipe2)
err := p.Process(writer, input) // Returns ctx.Err() if timeout occurs
```

# Utilities

The pipeline package provides helper functions for common patterns:

## PipeAdapter

Transform input data before passing it to the next pipe:

```
uppercasePipe := pipeline.PipeAdapter(func(ctx context.Context, input string) (string, error) {
    return strings.ToUpper(input), nil
})

p := pipeline.New(ctx, uppercasePipe, processPipe)
```

PipeAdapter acts like a TransformStream, simplifying input transformation
without needing to manually call next() with the transformed value.

## Exchange

Bridge different pipeline type parameters by describing how to translate inputs
and sinks in both directions:

```
type IntSink interface {
    Push(int) error
    Flush() error
    Close() error
}

type StringSink interface {
    Push(string) error
    Flush() error
    Close() error
}

intPipe := func(ctx context.Context, sink IntSink, input int, next pipeline.NextPipe[context.Context, IntSink, int]) error {
    if err := sink.Push(input + 1); err != nil {
        return err
    }
    return next(ctx, sink, input+1)
}

type stringSink struct {
    sink StringSink
}

func (s *stringSink) Push(v int) error   { return s.sink.Push(strconv.Itoa(v)) }
func (s *stringSink) Flush() error       { return s.sink.Flush() }
func (s *stringSink) Close() error       { return s.sink.Close() }

exchanged := pipeline.Exchange(
    intPipe,
    pipeline.Exchanger[
        context.Context, StringSink, string,
        context.Context, IntSink, int,
    ]{
        Sink: func(ctx context.Context, sink StringSink, input string) (context.Context, IntSink, int, error) {
            value, err := strconv.Atoi(input)
            if err != nil {
                return nil, nil, 0, err
            }
            return ctx, &stringSink{sink: sink}, value, nil
        },
        Source: func(outerCtx context.Context, outerSink StringSink, _ string, _ context.Context, _ IntSink, innerInput int) (context.Context, StringSink, string, error) {
            return outerCtx, outerSink, strconv.Itoa(innerInput), nil
        },
    },
)

p := pipeline.New(ctx, exchanged, finalPipe)
```

Exchanger.Sink runs before the wrapped pipe executes, while Exchanger.Source
runs every time the inner pipe advances the chain so that the outer pipeline receives
values in its native types.

## Wye

Execute two pipes in parallel:

```
wyePipe := pipeline.Wye(logPipe, metricsPipe)
p := pipeline.New(ctx, wyePipe, finalPipe)
```

Both branches receive the same input and execute concurrently. The pipeline continues
only after both branches complete successfully. If either returns an error, execution stops.

## Diverter

Conditionally select one or more pipes to execute based on a selector function:

```
router := pipeline.Diverter(
    func(ctx context.Context, input Request) ([]int, error) {
        switch input.Type {
        case "A": return []int{0}, nil
        case "B": return []int{1}, nil
        default: return []int{}, nil
        }
    },
    handleTypeA,
    handleTypeB,
)
```

The selector receives the context and input, returning the indices of pipes to execute
in parallel. If no pipes are selected (empty slice), the chain continues immediately.
If any index is invalid, an error is returned.

## Joiner

Execute all provided pipes in parallel:

```
joiner := pipeline.Joiner(
    notificationPipe,
    loggingPipe,
    metricsPipe,
)
p := pipeline.New(ctx, joiner, finalPipe)
```

All pipes receive the same input and execute concurrently. The pipeline continues
only after all pipes complete successfully. If any returns an error, execution stops.
This is a generalized version of Wye for N pipes.

## Pipeline Composition

Pipelines can be used as pipes in other pipelines via the `Pipe` method:

```
func (p *Pipeline[C, O, I]) Pipe(ctx C, sink O, input I, next NextPipe[C, O, I]) error
```

This allows you to compose complex pipeline hierarchies:

```go
// Create a sub-pipeline for validation
validator := pipeline.New(ctx,
    validateInputPipe,
    sanitizeInputPipe,
)

// Create a sub-pipeline for processing
processor := pipeline.New(ctx,
    transformPipe,
    enrichPipe,
)

// Use pipelines as pipes in a main pipeline
mainPipeline := pipeline.New(ctx,
    validator.Pipe,  // Pipeline as a pipe
    processor.Pipe,  // Another pipeline as a pipe
    finalizePipe,
)
```

The `Pipe` method:

* Executes the entire pipeline with the provided sink and input
* Returns any errors from pipeline execution
* Calls the `next` function after successful pipeline completion
* Respects context cancellation

var (
ErrAdaptNilPipe      = errors.New("pipeline: adapt pipe cannot be nil")
ErrAdaptMissingSink  = errors.New("pipeline: exchanger sink hook cannot be nil")
ErrAdaptMissingSource = errors.New("pipeline: exchanger source hook cannot be nil")
)

type Pipe\[C context.Context, O, I any] func(ctx C, sink O, source I, next NextPipe\[C, O, I]) error
type NextPipe\[C context.Context, O, I any] func(C, O, I) error

type Pipeline\[C context.Context, O, I any] struct{ ... }

func New\[C context.Context, O, I any]\(ctx C, pipes ...Pipe\[C, O, I]) \*Pipeline\[C, O, I]
func End[C context.Context, O, I any]() NextPipe\[C, O, I]

func PipeAdapter\[C context.Context, O, I any]\(transform func(C, I) (I, error)) Pipe\[C, O, I]

type Exchanger\[C1 context.Context, O1, I1 any, C2 context.Context, O2, I2 any] struct {
Sink   func(ctx C1, sink O1, input I1) (C2, O2, I2, error)
Source func(
outerCtx C1,
outerSink O1,
outerInput I1,
innerCtx C2,
innerSink O2,
innerInput I2,
) (C1, O1, I1, error)
}

func Exchange\[C1 context.Context, O1, I1 any, C2 context.Context, O2, I2 any]\(pipe Pipe\[C2, O2, I2], exchanger Exchanger\[C1, O1, I1, C2, O2, I2]) Pipe\[C1, O1, I1]
func Diverter\[C context.Context, O, I any]\(selector func(C, I) (\[]int, error), pipes ...Pipe\[C, O, I]) Pipe\[C, O, I]
func Joiner\[C context.Context, O, I any]\(pipes ...Pipe\[C, O, I]) Pipe\[C, O, I]
func Wye\[C context.Context, O, I any]\(left, right Pipe\[C, O, I]) Pipe\[C, O, I]

func (p \*Pipeline\[C, O, I]) Process(ctx C, sink O, input I) error
func (p \*Pipeline\[C, O, I]) Execute(ctx C, sink O, input I) error // Deprecated: Use Process instead.
func (p \*Pipeline\[C, O, I]) Pipe(ctx C, sink O, input I, next NextPipe\[C, O, I]) error
