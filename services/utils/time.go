package utils

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

func IsEmptyJSON(data []byte) bool {
	return len(data) == 0
}

func ParseDuration(input string) (time.Duration, error) {
	if len(input) < 2 {
		return 0, errors.New("input string is too short")
	}
	numberPart := input[:len(input)-1]
	unitPart := input[len(input)-1]
	number, err := strconv.Atoi(numberPart)
	if err != nil {
		return 0, fmt.Errorf("invalid number part: %v", err)
	}
	var duration time.Duration
	switch unitPart {
	case 's':
		duration = time.Duration(number) * time.Second
	case 'm':
		duration = time.Duration(number) * time.Minute
	case 'h':
		duration = time.Duration(number) * time.Hour
	default:
		return 0, errors.New("invalid unit part; use 's', 'm', or 'h'")
	}
	return duration, nil
}
