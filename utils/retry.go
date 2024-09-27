package utils

import (
	"math/rand"
	"time"
)

func CalculateJitter(baseDelay time.Duration, percent float64) time.Duration {
	jitter := time.Duration(rand.Float64()*percent*float64(baseDelay)) - time.Duration(percent*float64(baseDelay)/2)
	return baseDelay + jitter
}
