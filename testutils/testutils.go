// Copyright 2021 Irfan Sharif.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"fmt"
	"log"
	"runtime/debug"
	"testing"
	"time"
)

// defaultSucceedsSoonDuration is the maximum amount of time unittests
// will wait for a condition to become true. See SucceedsSoon().
const defaultSucceedsSoonDuration = 5 * time.Second

// SucceedsSoon fails the test (with t.Fatal) unless the supplied
// function runs without error within a preset maximum duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at around 1s.
func SucceedsSoon(t testing.TB, fn func() error) {
	t.Helper()
	if err := succeedsSoonError(fn); err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s\n%s",
			defaultSucceedsSoonDuration, err, string(debug.Stack()))
	}
}

func ShouldPanic(t *testing.T, expected string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic with message: %s", expected)
		} else if fmt.Sprint(r) != expected {
			t.Errorf("expected: %s, actual: %v", expected, r)
		}
	}()
	fn()
}

// succeedsSoonError returns an error unless the supplied function runs without
// error within a preset maximum duration. The function is invoked immediately
// at first and then successively with an exponential backoff starting at 1ns
// and ending at around 1s.
func succeedsSoonError(fn func() error) error {
	tBegin := time.Now()
	wrappedFn := func() error {
		err := fn()
		if time.Since(tBegin) > 3*time.Second && err != nil {
			log.Printf("SucceedsSoon: %v", err)
		}
		return err
	}
	return forDuration(defaultSucceedsSoonDuration, wrappedFn)
}

// forDuration will retry the given function until it either returns
// without error, or the given duration has elapsed. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at the specified duration.
func forDuration(duration time.Duration, fn func() error) error {
	deadline := time.Now().Add(duration)
	var lastErr error
	for wait := time.Duration(1); time.Now().Before(deadline); wait *= 2 {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if wait > time.Second {
			wait = time.Second
		}
		time.Sleep(wait)
	}
	return lastErr
}
