package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Mock writer for testing
type mockWriter struct {
	data []string
}

func (w *mockWriter) Write(s string) {
	w.data = append(w.data, s)
}

// Test pipeline creation
func TestNew(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, *mockWriter, string](ctx)

		if p == nil {
			t.Fatal("expected non-nil pipeline")
		}
		if p.Context() != ctx {
			t.Error("context not set correctly")
		}
		if len(p.pipes) != 0 {
			t.Errorf("expected 0 pipes, got %d", len(p.pipes))
		}
	})

	t.Run("single pipe", func(t *testing.T) {
		ctx := context.Background()
		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}

		p := New[context.Context, *mockWriter, string](ctx, pipe)

		if p == nil {
			t.Fatal("expected non-nil pipeline")
		}
		if len(p.pipes) != 1 {
			t.Errorf("expected 1 pipe, got %d", len(p.pipes))
		}
	})

	t.Run("multiple pipes", func(t *testing.T) {
		ctx := context.Background()
		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}
		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}
		pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)

		if len(p.pipes) != 3 {
			t.Errorf("expected 3 pipes, got %d", len(p.pipes))
		}
	})
}

// Test pipeline execution
func TestPipeline_Execute(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, *mockWriter, string](ctx)
		w := &mockWriter{}

		err := p.Execute(w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("single pipe execution", func(t *testing.T) {
		ctx := context.Background()
		executed := false

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			executed = true
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(w, "hello")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if !executed {
			t.Error("pipe was not executed")
		}
		if len(w.data) != 1 || w.data[0] != "hello" {
			t.Errorf("expected ['hello'], got %v", w.data)
		}
	})

	t.Run("multiple pipes in order", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe1-before")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2-before")
			err := next(ctx, w, input)
			w.Write("pipe2-after")
			return err
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe3")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe1")
			// Don't call next, short-circuit
			return nil
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe1")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2")
			return expectedErr
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return expectedErr
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("input transformation through pipes", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			// Transform input before passing to next
			return next(ctx, w, input+" modified1")
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			// Transform input before passing to next
			return next(ctx, w, input+" modified2")
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(w, "original")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expected := "original modified1 modified2"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})
}

// Test Context method
func TestPipeline_Context(t *testing.T) {
	t.Run("returns stored context", func(t *testing.T) {
		ctx := context.Background()
		p := New[context.Context, *mockWriter, string](ctx)

		if p.Context() != ctx {
			t.Error("Context() should return the stored context")
		}
	})

	t.Run("returns todo context", func(t *testing.T) {
		ctx := context.TODO()
		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}

		p := New[context.Context, *mockWriter, string](ctx, pipe)
		if p.Context() != ctx {
			t.Error("Context() should return TODO context")
		}
	})
}

// Test edge cases
func TestPipeline_EdgeCases(t *testing.T) {
	t.Run("todo context", func(t *testing.T) {
		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			return nil
		}

		// Should work with context.TODO()
		p := New[context.Context, *mockWriter, string](context.TODO(), pipe)
		if p == nil {
			t.Error("expected non-nil pipeline")
		}
		w := &mockWriter{}
		err := p.Execute(w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("context cancellation before execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("should not execute")
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		// Cancel before execution
		cancel()

		err := p.Execute(w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe1")
			// Cancel context after first pipe
			cancel()
			return next(ctx, w, input)
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2-should-not-execute")
			return next(ctx, w, input)
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("should not execute")
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe1-before")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe2")
			cancel() // Cancel here
			return next(ctx, w, input)
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write(input)
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)

		// Execute concurrently
		done := make(chan bool, 2)

		go func() {
			w := &mockWriter{}
			p.Execute(w, "goroutine1")
			done <- true
		}()

		go func() {
			w := &mockWriter{}
			p.Execute(w, "goroutine2")
			done <- true
		}()

		<-done
		<-done
		// Should not panic or deadlock
	})

	t.Run("very long pipeline", func(t *testing.T) {
		ctx := context.Background()

		// Create 100 pipes
		var pipes []Pipe[context.Context, *mockWriter, string]
		for i := 0; i < 100; i++ {
			pipes = append(pipes, func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
				return next(ctx, w, input)
			})
		}

		p := New[context.Context, *mockWriter, string](ctx, pipes...)
		w := &mockWriter{}

		err := p.Execute(w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(p.pipes) != 100 {
			t.Errorf("expected 100 pipes, got %d", len(p.pipes))
		}
	})

	t.Run("complex writer type", func(t *testing.T) {
		type complexWriter struct {
			builder strings.Builder
			count   int
		}

		ctx := context.Background()

		pipe := func(ctx context.Context, w *complexWriter, input string, next func(context.Context, *complexWriter, string) error) error {
			w.builder.WriteString(input)
			w.count++
			return nil
		}

		p := New(ctx, pipe)
		w := &complexWriter{}

		err := p.Execute(w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			// Handle nil writer
			if w == nil {
				return errors.New("nil writer")
			}
			return nil
		}

		p := New(ctx, pipe)

		err := p.Execute(nil, "test")
		if err == nil {
			t.Error("expected error with nil writer")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(w, "")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "" {
			t.Errorf("expected [''], got %v", w.data)
		}
	})
}

// Benchmark pipeline execution
func BenchmarkPipeline_Execute(b *testing.B) {
	ctx := context.Background()

	pipe1 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
		return next(ctx, w, input)
	}
	pipe2 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
		return next(ctx, w, input)
	}
	pipe3 := func(ctx context.Context, w *mockWriter, input string, next func(context.Context, *mockWriter, string) error) error {
		w.Write(input)
		return nil
	}

	p := New(ctx, pipe1, pipe2, pipe3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &mockWriter{}
		p.Execute(w, "benchmark")
	}
}
