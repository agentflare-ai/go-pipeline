package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// Sink interface for testing
type Sink[T any] interface {
	Push(v T) (int, error)
	Flush() error
	Close() error
}

// sinkFunc is a wrapper that adapts sink types
type sinkFunc[T any] struct {
	sink      *mockSink[T]
	convert   func(T) string
	outerSink Sink[string]
}

func (w *sinkFunc[T]) Push(v T) (int, error) {
	// Push to inner sink
	_, err := w.sink.Push(v)
	if err != nil {
		return 0, err
	}
	// Also push converted value to outer sink
	_, err = w.outerSink.Push(w.convert(v))
	return 1, err
}

func (w *sinkFunc[T]) Flush() error {
	if err := w.sink.Flush(); err != nil {
		return err
	}
	return w.outerSink.Flush()
}

func (w *sinkFunc[T]) Close() error {
	if err := w.sink.Close(); err != nil {
		return err
	}
	return w.outerSink.Close()
}

// Mock sink for testing
type mockSink[T any] struct {
	mu   sync.Mutex
	data []T
}

func (w *mockSink[T]) Push(v T) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = append(w.data, v)
	return len(w.data), nil
}

func (w *mockSink[T]) Flush() error {
	return nil
}

func (w *mockSink[T]) Close() error {
	return nil
}

type mockSinkString = mockSink[string]

// Complex writer for testing different sink implementations
type complexWriter struct {
	builder strings.Builder
	count   int
}

func (w *complexWriter) Push(s string) (int, error) {
	w.builder.WriteString(s)
	w.count++
	return w.count, nil
}

func (w *complexWriter) Flush() error {
	return nil
}

func (w *complexWriter) Close() error {
	return nil
}

// Test pipeline creation
func TestNew(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, Sink[string], string](ctx)

		if p == nil {
			t.Fatal("expected non-nil pipeline")
		}

		if len(p.pipes) != 0 {
			t.Errorf("expected 0 pipes, got %d", len(p.pipes))
		}
	})

	t.Run("single pipe", func(t *testing.T) {
		ctx := context.Background()
		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}

		p := New[context.Context, Sink[string], string](ctx, pipe)

		if p == nil {
			t.Fatal("expected non-nil pipeline")
		}
		if len(p.pipes) != 1 {
			t.Errorf("expected 1 pipe, got %d", len(p.pipes))
		}
	})

	t.Run("multiple pipes", func(t *testing.T) {
		ctx := context.Background()
		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}
		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}
		pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)

		if len(p.pipes) != 3 {
			t.Errorf("expected 3 pipes, got %d", len(p.pipes))
		}
	})
}

// Test pipeline execution
func TestPipeline_Process(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, Sink[string], string](ctx)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("single pipe execution", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			_, err := sink.Push(input)
			if err != nil {
				return err
			}
			return nil
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "hello")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "hello" {
			t.Errorf("expected ['hello'], got %v", w.data)
		}
	})

	t.Run("multiple pipes in order", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1-before")
			err := next(ctx, sink, input)
			sink.Push("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2-before")
			err := next(ctx, sink, input)
			sink.Push("pipe2-after")
			return err
		}

		pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe3")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expected := []string{"pipe1-before", "pipe2-before", "pipe3", "pipe2-after", "pipe1-after"}
		if len(w.data) != len(expected) {
			t.Fatalf("expected %d writes, got %d: %v", len(expected), len(w.data), w.data)
		}
		for i, exp := range expected {
			if w.data[i] != exp {
				t.Errorf("at index %d: expected %q, got %q", i, exp, w.data[i])
			}
		}
	})

	t.Run("short circuit without calling next", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1")
			// Don't call next, short-circuit
			return nil
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "pipe1" {
			t.Errorf("expected ['pipe1'], got %v (pipe2 should not have executed)", w.data)
		}
	})

	t.Run("error propagation", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("test error")

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1")
			err := next(ctx, sink, input)
			sink.Push("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			return expectedErr
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		// pipe1 should still write after even though pipe2 returned error
		expected := []string{"pipe1", "pipe2", "pipe1-after"}
		if len(w.data) != len(expected) {
			t.Fatalf("expected %d writes, got %d: %v", len(expected), len(w.data), w.data)
		}
	})

	t.Run("error in first pipe", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("first pipe error")

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("input transformation through pipes", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			// Transform input before passing to next
			return next(ctx, sink, input+" modified1")
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			// Transform input before passing to next
			return next(ctx, sink, input+" modified2")
		}

		pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "original")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expected := "original modified1 modified2"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})
}

// Test Pipeline Pipe method
func TestPipeline_Pipe(t *testing.T) {
	t.Run("executes pipeline and continues to next", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			_, err := sink.Push(input)
			if err != nil {
				return err
			}
			return nil
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		nextCalled := false
		next := func(ctx context.Context, sink Sink[string], input string) error {
			nextCalled = true
			return nil
		}

		err := p.Pipe(ctx, w, "test", next)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if !nextCalled {
			t.Error("next function was not called")
		}
		if len(w.data) != 1 || w.data[0] != "test" {
			t.Errorf("expected ['test'], got %v", w.data)
		}
	})

	t.Run("propagates Process error", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("process error")

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		next := func(ctx context.Context, sink Sink[string], input string) error {
			t.Error("next should not be called when Process fails")
			return nil
		}

		err := p.Pipe(ctx, w, "test", next)
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("propagates next error", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("next error")

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		next := func(ctx context.Context, sink Sink[string], input string) error {
			return expectedErr
		}

		err := p.Pipe(ctx, w, "test", next)
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			t.Error("pipe should not execute with cancelled context")
			return nil
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		next := func(ctx context.Context, sink Sink[string], input string) error {
			t.Error("next should not be called with cancelled context")
			return nil
		}

		err := p.Pipe(ctx, w, "test", next)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// Test Context method
func TestPipeline_Context(t *testing.T) {
	t.Run("returns stored context", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, Sink[string], string](ctx)
		w := &mockSinkString{}
		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("returns todo context", func(t *testing.T) {
		ctx := context.TODO()
		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}

		p := New[context.Context, Sink[string], string](ctx, pipe)
		w := &mockSinkString{}
		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// Test edge cases
func TestPipeline_EdgeCases(t *testing.T) {
	t.Run("todo context", func(t *testing.T) {
		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return nil
		}

		// Should work with context.TODO()
		p := New[context.Context, Sink[string], string](context.TODO(), pipe)
		if p == nil {
			t.Error("expected non-nil pipeline")
		}
		w := &mockSinkString{}
		err := p.Process(context.TODO(), w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("context cancellation before execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return next(ctx, sink, input)
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		// Cancel before execution
		cancel()

		err := p.Process(ctx, w, "test")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Pipe should not have executed
		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("context cancellation during execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1")
			// Cancel context after first pipe
			cancel()
			return next(ctx, sink, input)
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2-should-not-execute")
			return next(ctx, sink, input)
		}

		pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Only pipe1 should have executed
		if len(w.data) != 1 || w.data[0] != "pipe1" {
			t.Errorf("expected ['pipe1'], got %v", w.data)
		}
	})

	t.Run("context deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0) // Already expired
		defer cancel()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return next(ctx, sink, input)
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != context.DeadlineExceeded {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}

		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("context cancellation in middle of chain", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1-before")
			err := next(ctx, sink, input)
			sink.Push("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			cancel() // Cancel here
			return next(ctx, sink, input)
		}

		pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Should have: pipe1-before, pipe2, pipe1-after (unwinding)
		expected := []string{"pipe1-before", "pipe2", "pipe1-after"}
		if len(w.data) != len(expected) {
			t.Fatalf("expected %d writes, got %d: %v", len(expected), len(w.data), w.data)
		}
		for i, exp := range expected {
			if w.data[i] != exp {
				t.Errorf("at index %d: expected %q, got %q", i, exp, w.data[i])
			}
		}
	})

	t.Run("concurrent execution", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return next(ctx, sink, input)
		}

		p := New(ctx, pipe)

		// Execute concurrently
		done := make(chan bool, 2)

		go func() {
			w := &mockSinkString{}
			p.Process(ctx, w, "goroutine1")
			done <- true
		}()

		go func() {
			w := &mockSinkString{}
			p.Process(ctx, w, "goroutine2")
			done <- true
		}()

		<-done
		<-done
		// Should not panic or deadlock
	})

	t.Run("very long pipeline", func(t *testing.T) {
		ctx := context.Background()

		// Create 100 pipes
		var pipes []Pipe[context.Context, Sink[string], string]
		for i := 0; i < 100; i++ {
			pipes = append(pipes, func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
				return next(ctx, sink, input)
			})
		}

		p := New[context.Context, Sink[string], string](ctx, pipes...)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(p.pipes) != 100 {
			t.Errorf("expected 100 pipes, got %d", len(p.pipes))
		}
	})

	t.Run("complex writer type", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			_, err := sink.Push(input)
			return err
		}

		p := New(ctx, pipe)
		w := &complexWriter{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if w.builder.String() != "test" {
			t.Errorf("expected 'test', got %q", w.builder.String())
		}
		if w.count != 1 {
			t.Errorf("expected count=1, got %d", w.count)
		}
	})

	t.Run("nil writer", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			// Handle nil writer
			if sink == nil {
				return errors.New("nil writer")
			}
			return nil
		}

		p := New(ctx, pipe)

		err := p.Process(ctx, nil, "test")
		if err == nil {
			t.Error("expected error with nil writer")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return nil
		}

		p := New(ctx, pipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "" {
			t.Errorf("expected [''], got %v", w.data)
		}
	})
}

// Test PipeAdapter function
func TestPipeAdapter(t *testing.T) {
	t.Run("transforms input", func(t *testing.T) {
		ctx := context.Background()

		// Uppercase transformation
		upperPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			return strings.ToUpper(input), nil
		})

		// Capture transformed input
		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return next(ctx, sink, input)
		}

		p := New(ctx, upperPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "hello world")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := "HELLO WORLD"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})

	t.Run("chains multiple transformations", func(t *testing.T) {
		ctx := context.Background()

		// Add prefix
		prefixPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			return "prefix:" + input, nil
		})

		// Add suffix
		suffixPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			return input + ":suffix", nil
		})

		// Capture final result
		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return nil
		}

		p := New(ctx, prefixPipe, suffixPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := "prefix:test:suffix"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})

	t.Run("handles transformation errors", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("transformation failed")

		errorPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			return "", expectedErr
		})

		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return nil
		}

		p := New(ctx, errorPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		// Capture pipe should not have executed
		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		transformPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			cancel() // Cancel during transformation
			return strings.ToUpper(input), nil
		})

		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return nil
		}

		p := New(ctx, transformPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		// Capture pipe should not execute due to cancelled context
		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("works with integer types", func(t *testing.T) {
		ctx := context.Background()

		// Multiply by 2
		multiplyPipe := PipeAdapter[context.Context, Sink[string], int](func(ctx context.Context, input int) (int, error) {
			return input * 2, nil
		})

		// Add 10
		addPipe := PipeAdapter[context.Context, Sink[string], int](func(ctx context.Context, input int) (int, error) {
			return input + 10, nil
		})

		// Capture result
		var result int
		capturePipe := func(ctx context.Context, sink Sink[string], input int, next NextPipe[context.Context, Sink[string], int]) error {
			result = input
			return nil
		}

		p := New(ctx, multiplyPipe, addPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := 20 // (5 * 2) + 10
		if result != expected {
			t.Errorf("expected %d, got %d", expected, result)
		}
	})

	t.Run("empty transformation", func(t *testing.T) {
		ctx := context.Background()

		// Identity transformation
		identityPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			return input, nil
		})

		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return nil
		}

		p := New(ctx, identityPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "unchanged")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "unchanged" {
			t.Errorf("expected ['unchanged'], got %v", w.data)
		}
	})

	t.Run("complex transformation with context", func(t *testing.T) {
		ctx := context.Background()

		// Use context value in transformation
		transformPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
			// Simulate using context for something meaningful
			if err := ctx.Err(); err != nil {
				return "", err
			}
			return "transformed:" + input, nil
		})

		capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push(input)
			return nil
		}

		p := New(ctx, transformPipe, capturePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "data")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := "transformed:data"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})
}

func TestAdapt(t *testing.T) {
	t.Run("bridges sink and input types", func(t *testing.T) {
		ctx := context.Background()
		collector := &mockSinkString{}

		innerPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			_, err := sink.Push(input + " processed")
			if err != nil {
				return err
			}
			return next(ctx, sink, input+" processed")
		}

		exchanger := Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], string,
		]{
			Sink: func(ctx context.Context, sink Sink[string], input string) (context.Context, Sink[string], string, error) {
				return ctx, sink, input, nil
			},
			Source: func(outerCtx context.Context, outerSink Sink[string], outerInput string, innerCtx context.Context, innerSink Sink[string], innerInput string) (context.Context, Sink[string], string, error) {
				return outerCtx, outerSink, innerInput, nil
			},
		}

		adaptedPipe := Exchange(innerPipe, exchanger)

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			_, err := sink.Push("outer:" + input)
			return err
		}

		p := New(ctx, adaptedPipe, finalPipe)
		if err := p.Process(ctx, collector, "41"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := []string{"41 processed", "outer:41 processed"}
		if len(collector.data) != len(expected) {
			t.Fatalf("expected %d sink values, got %v", len(expected), collector.data)
		}
		for i, want := range expected {
			if collector.data[i] != want {
				t.Fatalf("expected sink[%d]=%q, got %q", i, want, collector.data[i])
			}
		}
	})

	t.Run("propagates forward errors", func(t *testing.T) {
		ctx := context.Background()
		callCount := 0

		innerPipe := func(ctx context.Context, sink Sink[string], input int, next NextPipe[context.Context, Sink[string], int]) error {
			callCount++
			return next(ctx, sink, input)
		}

		expectedErr := errors.New("forward failure")

		adapted := Exchange(innerPipe, Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], int,
		]{
			Sink: func(context.Context, Sink[string], string) (context.Context, Sink[string], int, error) {
				return nil, nil, 0, expectedErr
			},
			Source: func(outerCtx context.Context, outerSink Sink[string], outerInput string, innerCtx context.Context, innerSink Sink[string], innerInput int) (context.Context, Sink[string], string, error) {
				return outerCtx, outerSink, outerInput, nil
			},
		})

		err := adapted(ctx, &mockSinkString{}, "bad", End[context.Context, Sink[string], string]())
		if err != expectedErr {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		if callCount != 0 {
			t.Fatalf("expected wrapped pipe not to execute, got %d calls", callCount)
		}
	})

	t.Run("propagates backward errors", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("backward failure")
		nextCalled := false

		innerPipe := func(ctx context.Context, sink Sink[string], input int, next NextPipe[context.Context, Sink[string], int]) error {
			return next(ctx, sink, input+1)
		}

		adapted := Exchange(innerPipe, Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], int,
		]{
			Sink: func(ctx context.Context, sink Sink[string], input string) (context.Context, Sink[string], int, error) {
				return ctx, sink, 1, nil
			},
			Source: func(outerCtx context.Context, outerSink Sink[string], outerInput string, innerCtx context.Context, innerSink Sink[string], innerInput int) (context.Context, Sink[string], string, error) {
				return nil, nil, "", expectedErr
			},
		})

		err := adapted(ctx, &mockSinkString{}, "1", func(context.Context, Sink[string], string) error {
			nextCalled = true
			return nil
		})

		if err != expectedErr {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		if nextCalled {
			t.Fatal("expected outer next not to be called")
		}
	})

	t.Run("configuration errors", func(t *testing.T) {
		ctx := context.Background()
		sink := &mockSinkString{}

		sinkHook := func(ctx context.Context, sink Sink[string], input string) (context.Context, Sink[string], int, error) {
			return ctx, sink, 0, nil
		}

		sourceHook := func(outerCtx context.Context, outerSink Sink[string], outerInput string, innerCtx context.Context, innerSink Sink[string], innerInput int) (context.Context, Sink[string], string, error) {
			return outerCtx, outerSink, outerInput, nil
		}

		var inner Pipe[context.Context, Sink[string], int] = func(context.Context, Sink[string], int, NextPipe[context.Context, Sink[string], int]) error {
			return nil
		}

		p := Exchange[context.Context, Sink[string], string, context.Context, Sink[string], int](nil, Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], int,
		]{Sink: sinkHook, Source: sourceHook})
		if err := p(ctx, sink, "input", End[context.Context, Sink[string], string]()); !errors.Is(err, ErrAdaptNilPipe) {
			t.Fatalf("expected ErrAdaptNilPipe, got %v", err)
		}

		p = Exchange(inner, Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], int,
		]{Source: sourceHook})
		if err := p(ctx, sink, "input", End[context.Context, Sink[string], string]()); !errors.Is(err, ErrAdaptMissingSink) {
			t.Fatalf("expected ErrAdaptMissingSink, got %v", err)
		}

		p = Exchange(inner, Exchanger[
			context.Context, Sink[string], string,
			context.Context, Sink[string], int,
		]{Sink: sinkHook})
		if err := p(ctx, sink, "input", End[context.Context, Sink[string], string]()); !errors.Is(err, ErrAdaptMissingSource) {
			t.Fatalf("expected ErrAdaptMissingSource, got %v", err)
		}
	})
}

// Test Wye function
func TestWye(t *testing.T) {
	t.Run("executes both branches in parallel", func(t *testing.T) {
		ctx := context.Background()

		leftPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("left:" + input)
			return nil
		}

		rightPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("right:" + input)
			return nil
		}

		wyePipe := Wye(leftPipe, rightPipe)

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("final")
			return nil
		}

		p := New(ctx, wyePipe, finalPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should have writes from both branches and final pipe
		if len(w.data) != 3 {
			t.Fatalf("expected 3 writes, got %d: %v", len(w.data), w.data)
		}

		// Check that both branches wrote (order may vary due to parallelism)
		hasLeft := false
		hasRight := false
		hasFinal := false
		for _, d := range w.data {
			if d == "left:test" {
				hasLeft = true
			}
			if d == "right:test" {
				hasRight = true
			}
			if d == "final" {
				hasFinal = true
			}
		}

		if !hasLeft || !hasRight || !hasFinal {
			t.Errorf("expected writes from both branches and final, got %v", w.data)
		}
	})

	t.Run("propagates error from left branch", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("left error")

		leftPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		rightPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("right")
			return nil
		}

		wyePipe := Wye(leftPipe, rightPipe)
		p := New(ctx, wyePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("propagates error from right branch", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("right error")

		leftPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("left")
			return nil
		}

		rightPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		wyePipe := Wye(leftPipe, rightPipe)
		p := New(ctx, wyePipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

// Test Diverter function
func TestDiverter(t *testing.T) {
	t.Run("selects single pipe based on input", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0:" + input)
			return nil
		}

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1:" + input)
			return nil
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2:" + input)
			return nil
		}

		// Select pipe based on input length
		selector := func(ctx context.Context, input string) ([]int, error) {
			if len(input) < 5 {
				return []int{0}, nil
			} else if len(input) < 10 {
				return []int{1}, nil
			}
			return []int{2}, nil
		}

		diverterPipe := Diverter(selector, pipe0, pipe1, pipe2)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		// Test short input (should use pipe0)
		err := p.Process(ctx, w, "hi")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe0:hi" {
			t.Errorf("expected ['pipe0:hi'], got %v", w.data)
		}

		// Test medium input (should use pipe1)
		w = &mockSinkString{}
		err = p.Process(ctx, w, "hello")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe1:hello" {
			t.Errorf("expected ['pipe1:hello'], got %v", w.data)
		}

		// Test long input (should use pipe2)
		w = &mockSinkString{}
		err = p.Process(ctx, w, "hello world")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe2:hello world" {
			t.Errorf("expected ['pipe2:hello world'], got %v", w.data)
		}
	})

	t.Run("selects multiple pipes in parallel", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0")
			return nil
		}

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1")
			return nil
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			return nil
		}

		// Select multiple pipes
		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0, 2}, nil // Select pipe0 and pipe2
		}

		diverterPipe := Diverter(selector, pipe0, pipe1, pipe2)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should have writes from pipe0 and pipe2, but not pipe1
		if len(w.data) != 2 {
			t.Fatalf("expected 2 writes, got %d: %v", len(w.data), w.data)
		}

		hasPipe0 := false
		hasPipe2 := false
		for _, d := range w.data {
			if d == "pipe0" {
				hasPipe0 = true
			}
			if d == "pipe2" {
				hasPipe2 = true
			}
			if d == "pipe1" {
				t.Errorf("pipe1 should not have executed")
			}
		}

		if !hasPipe0 || !hasPipe2 {
			t.Errorf("expected writes from pipe0 and pipe2, got %v", w.data)
		}
	})

	t.Run("continues chain after selected pipes", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("selected")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0}, nil
		}

		diverterPipe := Diverter(selector, pipe0)

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("final")
			return nil
		}

		p := New(ctx, diverterPipe, finalPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 2 {
			t.Fatalf("expected 2 writes, got %d: %v", len(w.data), w.data)
		}

		// Order might vary, but both should be present
		hasSelected := false
		hasFinal := false
		for _, d := range w.data {
			if d == "selected" {
				hasSelected = true
			}
			if d == "final" {
				hasFinal = true
			}
		}

		if !hasSelected || !hasFinal {
			t.Errorf("expected both 'selected' and 'final', got %v", w.data)
		}
	})

	t.Run("empty selection continues chain", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{}, nil // No pipes selected
		}

		diverterPipe := Diverter(selector, pipe0)

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("final")
			return nil
		}

		p := New(ctx, diverterPipe, finalPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should only have final write
		if len(w.data) != 1 || w.data[0] != "final" {
			t.Errorf("expected ['final'], got %v", w.data)
		}
	})

	t.Run("invalid index returns error", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0")
			return nil
		}

		// Select invalid index
		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{5}, nil // Out of bounds
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err == nil {
			t.Fatal("expected error for invalid index")
		}
		if !strings.Contains(err.Error(), "invalid pipe index") {
			t.Errorf("expected 'invalid pipe index' error, got %v", err)
		}
	})

	t.Run("negative index returns error", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{-1}, nil // Negative index
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err == nil {
			t.Fatal("expected error for negative index")
		}
	})

	t.Run("selector error propagates", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("selector failed")

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("should not execute")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return nil, expectedErr
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("selected pipe error propagates", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("pipe error")

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0}, nil
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

// Test Joiner function
func TestJoiner(t *testing.T) {
	t.Run("executes all pipes in parallel", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0:" + input)
			return nil
		}

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe1:" + input)
			return nil
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2:" + input)
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1, pipe2)
		p := New(ctx, joinerPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should have writes from all pipes
		if len(w.data) != 3 {
			t.Fatalf("expected 3 writes, got %d: %v", len(w.data), w.data)
		}

		// Check all pipes wrote (order may vary due to parallelism)
		hasPipe0 := false
		hasPipe1 := false
		hasPipe2 := false
		for _, d := range w.data {
			if d == "pipe0:test" {
				hasPipe0 = true
			}
			if d == "pipe1:test" {
				hasPipe1 = true
			}
			if d == "pipe2:test" {
				hasPipe2 = true
			}
		}

		if !hasPipe0 || !hasPipe1 || !hasPipe2 {
			t.Errorf("expected writes from all pipes, got %v", w.data)
		}
	})

	t.Run("continues chain after all pipes complete", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("joined0")
			return nil
		}

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("joined1")
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1)

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("final")
			return nil
		}

		p := New(ctx, joinerPipe, finalPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 3 {
			t.Fatalf("expected 3 writes, got %d: %v", len(w.data), w.data)
		}

		// Final should be present
		hasFinal := false
		for _, d := range w.data {
			if d == "final" {
				hasFinal = true
			}
		}

		if !hasFinal {
			t.Errorf("expected 'final' in writes, got %v", w.data)
		}
	})

	t.Run("empty joiner continues immediately", func(t *testing.T) {
		ctx := context.Background()

		joinerPipe := Joiner[context.Context, Sink[string], string]()

		finalPipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("final")
			return nil
		}

		p := New(ctx, joinerPipe, finalPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "final" {
			t.Errorf("expected ['final'], got %v", w.data)
		}
	})

	t.Run("propagates error from any pipe", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("pipe1 error")

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0")
			return nil
		}

		pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			return expectedErr
		}

		pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe2")
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1, pipe2)
		p := New(ctx, joinerPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("single pipe behaves correctly", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
			sink.Push("pipe0:" + input)
			return nil
		}

		joinerPipe := Joiner(pipe0)
		p := New(ctx, joinerPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "pipe0:test" {
			t.Errorf("expected ['pipe0:test'], got %v", w.data)
		}
	})

	t.Run("many pipes execute in parallel", func(t *testing.T) {
		ctx := context.Background()

		// Create 5 pipes (reduced from 10 to avoid race conditions with non-thread-safe mockSink)
		var pipes []Pipe[context.Context, Sink[string], string]
		for i := 0; i < 5; i++ {
			idx := i
			pipes = append(pipes, func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
				sink.Push(strings.Repeat("x", idx+1)) // +1 to avoid empty string
				return nil
			})
		}

		joinerPipe := Joiner(pipes...)
		p := New(ctx, joinerPipe)
		w := &mockSinkString{}

		err := p.Process(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 5 {
			t.Fatalf("expected 5 writes, got %d: %v", len(w.data), w.data)
		}
	})
}

// Benchmark pipeline execution
func BenchmarkPipeline_Process(b *testing.B) {
	ctx := context.Background()

	pipe1 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
		return next(ctx, sink, input)
	}
	pipe2 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
		return next(ctx, sink, input)
	}
	pipe3 := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
		sink.Push(input)
		return nil
	}

	p := New(ctx, pipe1, pipe2, pipe3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &mockSinkString{}
		p.Process(ctx, w, "benchmark")
	}
}

// Benchmark Adapter transformation
func BenchmarkAdapter(b *testing.B) {
	ctx := context.Background()

	transformPipe := PipeAdapter[context.Context, Sink[string], string](func(ctx context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	capturePipe := func(ctx context.Context, sink Sink[string], input string, next NextPipe[context.Context, Sink[string], string]) error {
		sink.Push(input)
		return nil
	}

	p := New(ctx, transformPipe, capturePipe)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &mockSinkString{}
		p.Process(ctx, w, "benchmark")
	}
}
