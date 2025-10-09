package pipeline // import "github.com/agentflare-ai/pipeline"

Package pipeline provides a generic, efficient middleware pattern for chaining
operations.

The pipeline pattern allows you to compose multiple processing steps (pipes)
that can:
  - Transform data as it flows through the chain
  - Share state using the built-in storage mechanism
  - Short-circuit execution by not calling next()
  - Handle errors and context cancellation gracefully

# Basic Usage

Create a pipeline with pipes that process data sequentially:

    // Define pipes that log and transform data
    logPipe := func(ctx context.Context, w io.Writer, input string, next func(context.Context, io.Writer, string) error) error {
    	fmt.Fprintf(w, "Processing: %s\n", input)
    	return next(ctx, w, input) // Continue to next pipe
    }

    uppercasePipe := func(ctx context.Context, w io.Writer, input string, next func(context.Context, io.Writer, string) error) error {
    	transformed := strings.ToUpper(input)
    	fmt.Fprintf(w, "Transformed: %s\n", transformed)
    	return next(ctx, w, transformed)
    }

    // Create and execute pipeline
    p := pipeline.New(context.Background(), logPipe, uppercasePipe)
    err := p.Execute(os.Stdout, "hello world")

# Using Storage

Share data between pipes using the storage mechanism:

    pipe1 := func(ctx context.Context, w io.Writer, input int, next func(context.Context, io.Writer, int) error) error {
    	// Store data for later pipes
    	pipeline.Store(ctx, "multiplier", 2)
    	return next(ctx, w, input)
    }

    pipe2 := func(ctx context.Context, w io.Writer, input int, next func(context.Context, io.Writer, int) error) error {
    	// Load data from earlier pipe
    	multiplier, ok := pipeline.Load[int](ctx, "multiplier")
    	if ok {
    		input *= multiplier
    	}
    	fmt.Fprintf(w, "Result: %d\n", input)
    	return next(ctx, w, input)
    }

    p := pipeline.New(context.Background(), pipe1, pipe2)
    p.Execute(os.Stdout, 5) // Outputs: Result: 10

# Short-Circuiting

Pipes can stop execution by returning early without calling next():

    validationPipe := func(ctx context.Context, w io.Writer, input string, next func(context.Context, io.Writer, string) error) error {
    	if input == "" {
    		return fmt.Errorf("input cannot be empty")
    	}
    	return next(ctx, w, input) // Only continue if valid
    }

# Context Cancellation

The pipeline respects context cancellation and terminates early:

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    p := pipeline.New(ctx, longRunningPipe1, longRunningPipe2)
    err := p.Execute(writer, input) // Returns ctx.Err() if timeout occurs

# Utilities

The pipeline package provides helper functions for common patterns:

## Adapter

Transform input data before passing it to the next pipe:

    uppercasePipe := pipeline.Adapter(func(ctx context.Context, input string) (string, error) {
        return strings.ToUpper(input), nil
    })

    p := pipeline.New(ctx, uppercasePipe, processPipe)

The Adapter function acts like a TransformStream, simplifying input transformation
without needing to manually call next() with the transformed value.

## Wye

Execute two pipes in parallel:

    wyePipe := pipeline.Wye(logPipe, metricsPipe)
    p := pipeline.New(ctx, wyePipe, finalPipe)

Both branches receive the same input and execute concurrently. The pipeline continues
only after both branches complete successfully. If either returns an error, execution stops.

## Diverter

Conditionally select one or more pipes to execute based on a selector function:

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

The selector receives the context and input, returning the indices of pipes to execute
in parallel. If no pipes are selected (empty slice), the chain continues immediately.
If any index is invalid, an error is returned.

## Joiner

Execute all provided pipes in parallel:

    joiner := pipeline.Joiner(
        notificationPipe,
        loggingPipe,
        metricsPipe,
    )
    p := pipeline.New(ctx, joiner, finalPipe)

All pipes receive the same input and execute concurrently. The pipeline continues
only after all pipes complete successfully. If any returns an error, execution stops.
This is a generalized version of Wye for N pipes.

var ErrStorageNotFound = errors.New("storage not found") ...
func Adapter[C context.Context, W, I any](transform func(C, I) (I, error)) Pipe[C, W, I]
func Diverter[C context.Context, W, I any](selector func(C, I) ([]int, error), pipes ...Pipe[C, W, I]) Pipe[C, W, I]
func Joiner[C context.Context, W, I any](pipes ...Pipe[C, W, I]) Pipe[C, W, I]
func Wye[C context.Context, W, I any](left, right Pipe[C, W, I]) Pipe[C, W, I]
func Load[T any](ctx context.Context, key any) (T, bool)
func Store[T any](ctx context.Context, key any, value T) error
type Pipe[C context.Context, W, I any] func(ctx C, writer W, input I, next func(C, W, I) error) error
type Pipeline[C context.Context, W, I any] struct{ ... }
    func New[C context.Context, W, I any](ctx C, pipes ...Pipe[C, W, I]) *Pipeline[C, W, I]
