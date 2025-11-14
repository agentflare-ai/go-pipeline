package pipeline

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Pipe is a middleware function that processes sink and source.
// It receives a next function that invokes the next pipe in the chain.
// The pipe can call next(ctx, sink, source) to continue the chain, or return early to short-circuit.
type Pipe[C context.Context, O, I any] func(ctx C, sink Sink[O], source I, next NextPipe[C, O, I]) error
type NextPipe[C context.Context, O, I any] func(C, Sink[O], I) error

type Sink[T any] interface {
	Flush() error
	Push(T) (int, error)
}

type sink[T any] struct {
	push  func(T) (int, error)
	flush func() error
}

func NewSink[T any](push func(T) (int, error), flush func() error) Sink[T] {
	return &sink[T]{
		push:  push,
		flush: flush,
	}
}

func (s *sink[T]) Flush() error {
	return s.flush()
}

func (s *sink[T]) Push(v T) (int, error) {
	return s.push(v)
}

var (
	// ErrAdaptNilPipe indicates that Adapt was asked to wrap a nil pipe.
	ErrAdaptNilPipe = errors.New("pipeline: adapt pipe cannot be nil")
	// ErrAdaptMissingSink indicates that the exchanger configuration is missing the sink hook.
	ErrAdaptMissingSink = errors.New("pipeline: exchanger sink hook cannot be nil")
	// ErrAdaptMissingSource indicates that the exchanger configuration is missing the source hook.
	ErrAdaptMissingSource = errors.New("pipeline: exchanger source hook cannot be nil")
)

// Pipeline chains multiple pipes together in a non-recursive middleware pattern.
type Pipeline[C context.Context, O, I any] struct {
	pipes       []Pipe[C, O, I]
	connections []NextPipe[C, O, I] // Pre-built next functions
}

func End[C context.Context, O, I any]() NextPipe[C, O, I] {
	return func(ctx C, drain Sink[O], input I) error {
		return nil
	}
}

// New creates a new pipeline with the given pipes.
// Pipes are executed in order: pipe1 -> pipe2 -> pipe3 -> ...
// Next functions are pre-built for efficiency.
func New[C context.Context, O, I any](ctx C, pipes ...Pipe[C, O, I]) *Pipeline[C, O, I] {
	p := &Pipeline[C, O, I]{
		pipes: pipes,
	}

	if len(pipes) > 0 {
		p.connect()
	}

	return p
}

// connect pre-builds the next functions for the entire chain.
// This is called once during construction for maximum efficiency.
// Each next function checks for context cancellation before proceeding.
func (p *Pipeline[C, O, I]) connect() {
	p.connections = make([]NextPipe[C, O, I], len(p.pipes))

	// Terminal next function (does nothing, chain complete)
	p.connections[len(p.pipes)-1] = func(ctx C, sink Sink[O], input I) error {
		return nil
	}

	// Build next functions from end to start
	for i := len(p.pipes) - 2; i >= 0; i-- {
		// Capture variables for closure
		nextIdx := i + 1
		p.connections[i] = func(ctx C, sink Sink[O], input I) error {
			// Check if context is cancelled before proceeding
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.pipes[nextIdx](ctx, sink, input, p.connections[nextIdx])
		}
	}
}

func (p *Pipeline[C, O, I]) Pipe(ctx C, sink Sink[O], input I, next NextPipe[C, O, I]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.Process(ctx, sink, input); err != nil {
		return err
	}
	return next(ctx, sink, input)
}

// Process runs the pipeline with the given writer and input.
// Storage is automatically provided in the context for sharing data between pipes.
// If the context is cancelled, execution terminates and returns the context error.
func (p *Pipeline[C, O, I]) Process(ctx C, sink Sink[O], input I) error {
	if len(p.pipes) == 0 {
		return nil
	}

	// Check if context is already cancelled before starting
	if err := ctx.Err(); err != nil {
		return err
	}

	// Execute the first pipe with pre-built next functions
	err := p.pipes[0](ctx, sink, input, p.connections[0])
	return err
}

// Deprecated: Use Process instead.
func (p *Pipeline[C, O, I]) Execute(ctx C, sink Sink[O], input I) error {
	return p.Process(ctx, sink, input)
}

// PipeAdapter transforms the input before passing it to the next pipe.
// It acts like a TransformStream, applying the transform function to the input
// and passing the result down the pipeline chain.
// If the transform returns an error, the pipeline stops and the error is returned.
func PipeAdapter[C context.Context, O, I any](transform func(C, I) (I, error)) Pipe[C, O, I] {
	return func(ctx C, sink Sink[O], input I, next NextPipe[C, O, I]) error {
		// Apply transformation to the input
		transformed, err := transform(ctx, input)
		if err != nil {
			return err
		}

		// Pass transformed input to the next pipe
		return next(ctx, sink, transformed)
	}
}

// Exchanger declares how Adapt should translate between outer pipeline types [C1, O1, I1]
// and the inner pipe types [C2, O2, I2]. Sink is invoked before the wrapped pipe executes
// to project the outer sink/input into the inner types. Source is invoked every time the
// wrapped pipe calls its next function so the outer chain receives mapped arguments.
type Exchanger[C1 context.Context, O1, I1 any, C2 context.Context, O2, I2 any] struct {
	Sink   func(ctx C1, sink Sink[O1], input I1) (C2, Sink[O2], I2, error)
	Source func(
		outerCtx C1,
		outerSink Sink[O1],
		outerInput I1,
		innerCtx C2,
		innerSink Sink[O2],
		innerInput I2,
	) (C1, Sink[O1], I1, error)
}

// Exchange converts a pipe defined for [C2, O2, I2] into a pipe that can participate in a pipeline
// operating with [C1, O1, I1]. It relies on an Exchanger to translate arguments in both directions.
// If the configuration is invalid, the returned pipe always fails with the corresponding error.
func Exchange[C1 context.Context, O1, I1 any, C2 context.Context, O2, I2 any](
	pipe Pipe[C2, O2, I2],
	exchanger Exchanger[C1, O1, I1, C2, O2, I2],
) Pipe[C1, O1, I1] {
	var configErr error
	switch {
	case pipe == nil:
		configErr = ErrAdaptNilPipe
	case exchanger.Sink == nil:
		configErr = ErrAdaptMissingSink
	case exchanger.Source == nil:
		configErr = ErrAdaptMissingSource
	}

	if configErr != nil {
		return func(C1, Sink[O1], I1, NextPipe[C1, O1, I1]) error {
			return configErr
		}
	}

	return func(ctx C1, sink Sink[O1], input I1, next NextPipe[C1, O1, I1]) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		innerCtx, innerSink, innerInput, err := exchanger.Sink(ctx, sink, input)
		if err != nil {
			return err
		}

		innerNext := func(c C2, s Sink[O2], i I2) error {
			outerCtx, outerSink, outerInput, mapErr := exchanger.Source(ctx, sink, input, c, s, i)
			if mapErr != nil {
				return mapErr
			}
			return next(outerCtx, outerSink, outerInput)
		}

		return pipe(innerCtx, innerSink, innerInput, innerNext)
	}
}

// Wye splits the pipeline into two parallel branches.
// Both left and right pipes receive the same writer and input,
// execute in parallel, and both must complete before continuing.
// If either branch returns an error, execution stops and the error is returned.
func Wye[C context.Context, O, I any](left, right Pipe[C, O, I]) Pipe[C, O, I] {
	return func(ctx C, sink Sink[O], input I, next NextPipe[C, O, I]) error {
		g, _ := errgroup.WithContext(ctx)

		g.Go(func() error {
			return left(ctx, sink, input, End[C, O, I]())
		})

		g.Go(func() error {
			return right(ctx, sink, input, End[C, O, I]())
		})

		// Wait for both to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// Both branches succeeded, continue the chain
		return next(ctx, sink, input)
	}
}

// Diverter selects one or more pipes from multiple options based on a selector function.
// The selector function receives the context and input, and returns the indices of the pipes to execute.
// Selected pipes are executed in parallel, and all must complete before continuing.
// If the selector returns an empty slice, the chain continues immediately.
// If any selected pipe returns an error, execution stops and the error is returned.
func Diverter[C context.Context, O, I any](selector func(C, I) ([]int, error), pipes ...Pipe[C, O, I]) Pipe[C, O, I] {
	return func(ctx C, sink Sink[O], input I, next NextPipe[C, O, I]) error {
		// Call selector to determine which pipes to use
		indices, err := selector(ctx, input)
		if err != nil {
			return err
		}

		// If no pipes selected, continue the chain
		if len(indices) == 0 {
			return next(ctx, sink, input)
		}

		// Validate all indices
		for _, idx := range indices {
			if idx < 0 || idx >= len(pipes) {
				return fmt.Errorf("invalid pipe index %d, must be between 0 and %d", idx, len(pipes)-1)
			}
		}

		// Execute selected pipes in parallel
		g, _ := errgroup.WithContext(ctx)
		for _, idx := range indices {
			idx := idx // Capture for closure
			g.Go(func() error {
				return pipes[idx](ctx, sink, input, End[C, O, I]())
			})
		}

		// Wait for all selected pipes to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// All selected pipes succeeded, continue the chain
		return next(ctx, sink, input)
	}
}

// Joiner executes multiple pipes in parallel and waits for all to complete.
// All pipes receive the same writer and input, execute in parallel,
// and all must complete before continuing the chain.
// If any pipe returns an error, execution stops and the error is returned.
// This is a generalized version of Wye for N pipes.
func Joiner[C context.Context, O, I any](pipes ...Pipe[C, O, I]) Pipe[C, O, I] {
	return func(ctx C, sink Sink[O], input I, next NextPipe[C, O, I]) error {
		// If no pipes provided, continue immediately
		if len(pipes) == 0 {
			return next(ctx, sink, input)
		}

		// Execute all pipes in parallel
		g, _ := errgroup.WithContext(ctx)
		for _, pipe := range pipes {
			pipe := pipe // Capture for closure
			g.Go(func() error {
				return pipe(ctx, sink, input, End[C, O, I]())
			})
		}

		// Wait for all pipes to complete
		if err := g.Wait(); err != nil {
			return err
		}

		// All pipes succeeded, continue the chain
		return next(ctx, sink, input)
	}
}
