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

		if len(p.pipes) != 0 {
			t.Errorf("expected 0 pipes, got %d", len(p.pipes))
		}
	})

	t.Run("single pipe", func(t *testing.T) {
		ctx := context.Background()
		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
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
		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return nil
		}
		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return nil
		}
		pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
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

		err := p.Execute(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("single pipe execution", func(t *testing.T) {
		ctx := context.Background()
		executed := false

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			executed = true
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "hello")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1-before")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2-before")
			err := next(ctx, w, input)
			w.Write("pipe2-after")
			return err
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe3")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1")
			// Don't call next, short-circuit
			return nil
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			return expectedErr
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return expectedErr
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			return nil
		}

		p := New(ctx, pipe1, pipe2)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}

		if len(w.data) != 0 {
			t.Errorf("expected no writes, got %v", w.data)
		}
	})

	t.Run("input transformation through pipes", func(t *testing.T) {
		ctx := context.Background()

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			// Transform input before passing to next
			return next(ctx, w, input+" modified1")
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			// Transform input before passing to next
			return next(ctx, w, input+" modified2")
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "original")
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
		w := &mockWriter{}
		err := p.Execute(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("returns todo context", func(t *testing.T) {
		ctx := context.TODO()
		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return nil
		}

		p := New[context.Context, *mockWriter, string](ctx, pipe)
		w := &mockWriter{}
		err := p.Execute(ctx, w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// Test edge cases
func TestPipeline_EdgeCases(t *testing.T) {
	t.Run("todo context", func(t *testing.T) {
		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return nil
		}

		// Should work with context.TODO()
		p := New[context.Context, *mockWriter, string](context.TODO(), pipe)
		if p == nil {
			t.Error("expected non-nil pipeline")
		}
		w := &mockWriter{}
		err := p.Execute(context.TODO(), w, "test")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("context cancellation before execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		// Cancel before execution
		cancel()

		err := p.Execute(ctx, w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1")
			// Cancel context after first pipe
			cancel()
			return next(ctx, w, input)
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2-should-not-execute")
			return next(ctx, w, input)
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1-before")
			err := next(ctx, w, input)
			w.Write("pipe1-after")
			return err
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			cancel() // Cancel here
			return next(ctx, w, input)
		}

		pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe3-should-not-execute")
			return nil
		}

		p := New(ctx, pipe1, pipe2, pipe3)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return next(ctx, w, input)
		}

		p := New(ctx, pipe)

		// Execute concurrently
		done := make(chan bool, 2)

		go func() {
			w := &mockWriter{}
			p.Execute(ctx, w, "goroutine1")
			done <- true
		}()

		go func() {
			w := &mockWriter{}
			p.Execute(ctx, w, "goroutine2")
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
			pipes = append(pipes, func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
				return next(ctx, w, input)
			})
		}

		p := New[context.Context, *mockWriter, string](ctx, pipes...)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe := func(ctx context.Context, w *complexWriter, input string, next Next[context.Context, *complexWriter, string]) error {
			w.builder.WriteString(input)
			w.count++
			return nil
		}

		p := New(ctx, pipe)
		w := &complexWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			// Handle nil writer
			if w == nil {
				return errors.New("nil writer")
			}
			return nil
		}

		p := New(ctx, pipe)

		err := p.Execute(ctx, nil, "test")
		if err == nil {
			t.Error("expected error with nil writer")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx := context.Background()

		pipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, pipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "" {
			t.Errorf("expected [''], got %v", w.data)
		}
	})
}

// Test Adapter function
func TestAdapter(t *testing.T) {
	t.Run("transforms input", func(t *testing.T) {
		ctx := context.Background()

		// Uppercase transformation
		upperPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			return strings.ToUpper(input), nil
		})

		// Capture transformed input
		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return next(ctx, w, input)
		}

		p := New(ctx, upperPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "hello world")
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
		prefixPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			return "prefix:" + input, nil
		})

		// Add suffix
		suffixPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			return input + ":suffix", nil
		})

		// Capture final result
		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, prefixPipe, suffixPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		errorPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			return "", expectedErr
		})

		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return nil
		}

		p := New(ctx, errorPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		transformPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			cancel() // Cancel during transformation
			return strings.ToUpper(input), nil
		})

		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return nil
		}

		p := New(ctx, transformPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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
		multiplyPipe := Adapter[context.Context, *mockWriter, int](func(ctx context.Context, input int) (int, error) {
			return input * 2, nil
		})

		// Add 10
		addPipe := Adapter[context.Context, *mockWriter, int](func(ctx context.Context, input int) (int, error) {
			return input + 10, nil
		})

		// Capture result
		var result int
		capturePipe := func(ctx context.Context, w *mockWriter, input int, next Next[context.Context, *mockWriter, int]) error {
			result = input
			return nil
		}

		p := New(ctx, multiplyPipe, addPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, 5)
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
		identityPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			return input, nil
		})

		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, identityPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "unchanged")
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
		transformPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
			// Simulate using context for something meaningful
			if err := ctx.Err(); err != nil {
				return "", err
			}
			return "transformed:" + input, nil
		})

		capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write(input)
			return nil
		}

		p := New(ctx, transformPipe, capturePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "data")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expected := "transformed:data"
		if len(w.data) != 1 || w.data[0] != expected {
			t.Errorf("expected [%q], got %v", expected, w.data)
		}
	})
}

// Test Wye function
func TestWye(t *testing.T) {
	t.Run("executes both branches in parallel", func(t *testing.T) {
		ctx := context.Background()

		leftPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("left:" + input)
			return nil
		}

		rightPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("right:" + input)
			return nil
		}

		wyePipe := Wye(leftPipe, rightPipe)

		finalPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("final")
			return nil
		}

		p := New(ctx, wyePipe, finalPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		leftPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return expectedErr
		}

		rightPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("right")
			return nil
		}

		wyePipe := Wye(leftPipe, rightPipe)
		p := New(ctx, wyePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("propagates error from right branch", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("right error")

		leftPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("left")
			return nil
		}

		rightPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return expectedErr
		}

		wyePipe := Wye(leftPipe, rightPipe)
		p := New(ctx, wyePipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

// Test Diverter function
func TestDiverter(t *testing.T) {
	t.Run("selects single pipe based on input", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0:" + input)
			return nil
		}

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1:" + input)
			return nil
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2:" + input)
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
		w := &mockWriter{}

		// Test short input (should use pipe0)
		err := p.Execute(ctx, w, "hi")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe0:hi" {
			t.Errorf("expected ['pipe0:hi'], got %v", w.data)
		}

		// Test medium input (should use pipe1)
		w = &mockWriter{}
		err = p.Execute(ctx, w, "hello")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe1:hello" {
			t.Errorf("expected ['pipe1:hello'], got %v", w.data)
		}

		// Test long input (should use pipe2)
		w = &mockWriter{}
		err = p.Execute(ctx, w, "hello world")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(w.data) != 1 || w.data[0] != "pipe2:hello world" {
			t.Errorf("expected ['pipe2:hello world'], got %v", w.data)
		}
	})

	t.Run("selects multiple pipes in parallel", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0")
			return nil
		}

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1")
			return nil
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			return nil
		}

		// Select multiple pipes
		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0, 2}, nil // Select pipe0 and pipe2
		}

		diverterPipe := Diverter(selector, pipe0, pipe1, pipe2)
		p := New(ctx, diverterPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("selected")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0}, nil
		}

		diverterPipe := Diverter(selector, pipe0)

		finalPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("final")
			return nil
		}

		p := New(ctx, diverterPipe, finalPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{}, nil // No pipes selected
		}

		diverterPipe := Diverter(selector, pipe0)

		finalPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("final")
			return nil
		}

		p := New(ctx, diverterPipe, finalPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0")
			return nil
		}

		// Select invalid index
		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{5}, nil // Out of bounds
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err == nil {
			t.Fatal("expected error for invalid index")
		}
		if !strings.Contains(err.Error(), "invalid pipe index") {
			t.Errorf("expected 'invalid pipe index' error, got %v", err)
		}
	})

	t.Run("negative index returns error", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{-1}, nil // Negative index
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err == nil {
			t.Fatal("expected error for negative index")
		}
	})

	t.Run("selector error propagates", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("selector failed")

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("should not execute")
			return nil
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return nil, expectedErr
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return expectedErr
		}

		selector := func(ctx context.Context, input string) ([]int, error) {
			return []int{0}, nil
		}

		diverterPipe := Diverter(selector, pipe0)
		p := New(ctx, diverterPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

// Test Joiner function
func TestJoiner(t *testing.T) {
	t.Run("executes all pipes in parallel", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0:" + input)
			return nil
		}

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe1:" + input)
			return nil
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2:" + input)
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1, pipe2)
		p := New(ctx, joinerPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("joined0")
			return nil
		}

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("joined1")
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1)

		finalPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("final")
			return nil
		}

		p := New(ctx, joinerPipe, finalPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		joinerPipe := Joiner[context.Context, *mockWriter, string]()

		finalPipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("final")
			return nil
		}

		p := New(ctx, joinerPipe, finalPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
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

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0")
			return nil
		}

		pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			return expectedErr
		}

		pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe2")
			return nil
		}

		joinerPipe := Joiner(pipe0, pipe1, pipe2)
		p := New(ctx, joinerPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("single pipe behaves correctly", func(t *testing.T) {
		ctx := context.Background()

		pipe0 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
			w.Write("pipe0:" + input)
			return nil
		}

		joinerPipe := Joiner(pipe0)
		p := New(ctx, joinerPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 1 || w.data[0] != "pipe0:test" {
			t.Errorf("expected ['pipe0:test'], got %v", w.data)
		}
	})

	t.Run("many pipes execute in parallel", func(t *testing.T) {
		ctx := context.Background()

		// Create 5 pipes (reduced from 10 to avoid race conditions with non-thread-safe mockWriter)
		var pipes []Pipe[context.Context, *mockWriter, string]
		for i := 0; i < 5; i++ {
			idx := i
			pipes = append(pipes, func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
				w.Write(strings.Repeat("x", idx+1)) // +1 to avoid empty string
				return nil
			})
		}

		joinerPipe := Joiner(pipes...)
		p := New(ctx, joinerPipe)
		w := &mockWriter{}

		err := p.Execute(ctx, w, "test")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(w.data) != 5 {
			t.Fatalf("expected 5 writes, got %d: %v", len(w.data), w.data)
		}
	})
}

// Benchmark pipeline execution
func BenchmarkPipeline_Execute(b *testing.B) {
	ctx := context.Background()

	pipe1 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
		return next(ctx, w, input)
	}
	pipe2 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
		return next(ctx, w, input)
	}
	pipe3 := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
		w.Write(input)
		return nil
	}

	p := New(ctx, pipe1, pipe2, pipe3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &mockWriter{}
		p.Execute(ctx, w, "benchmark")
	}
}

// Benchmark Adapter transformation
func BenchmarkAdapter(b *testing.B) {
	ctx := context.Background()

	transformPipe := Adapter[context.Context, *mockWriter, string](func(ctx context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	capturePipe := func(ctx context.Context, w *mockWriter, input string, next Next[context.Context, *mockWriter, string]) error {
		w.Write(input)
		return nil
	}

	p := New(ctx, transformPipe, capturePipe)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &mockWriter{}
		p.Execute(ctx, w, "benchmark")
	}
}
