package queue

import (
	"errors"
	"log/slog"
	"time"
)

const (
	LockKeyPrefix = "machinery_lock_"
)

var (
	ErrInvalidArgument = errors.New("invalid arguments")
)

// maxRetryBackoffSeconds caps the broker reconnect backoff: the Fibonacci
// sequence overflows int after ~92 steps, and an unbounded sleep would stall
// the retry loop for years
const maxRetryBackoffSeconds = 60

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := Fibonacci()
	return func(stopChan chan int) {
		if retryIn > 0 {
			backoff := min(retryIn, maxRetryBackoffSeconds)
			duration := time.Duration(backoff) * time.Second

			slog.Warn("retrying broker connection", "retryInSeconds", backoff)

			// a receive on stopChan only shortens the current sleep; whether
			// to keep retrying is decided by the caller via retrying()
			select {
			case <-stopChan:
			case <-time.After(duration):
			}
		}
		retryIn = min(fibonacci(), maxRetryBackoffSeconds)
	}
}

// Fibonacci returns successive Fibonacci numbers starting from 1
func Fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

// FibonacciNext returns next number in Fibonacci sequence greater than start
func FibonacciNext(start int) int {
	fib := Fibonacci()
	num := fib()
	for num <= start {
		num = fib()
	}
	return num
}
