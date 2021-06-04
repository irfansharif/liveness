package liveness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/irfansharif/liveness/testutils"
	"github.com/stretchr/testify/assert"
)

// TestDatadriven simulates the scripts under testdata/. The syntax is as
// follows:
//
// 		liveness impl=[algorithm: string] nodes=[int] bootstrap=[int]
//      receiver.fn(args)            # expected results
//      ----
//      receiver.fn(args)            ok
//
// Both receiver and args can be variadic and ranged. Consider the following:
//
//      1.members()           # members:[1 2 3 4 5 6 7]
//      3,5-7.live(1-7)       # live:true, found:true
//
// The first line asserts that N1's view of the cluster membership includes all
// nodes with IDs 1 through 7. The second line asserts the N3, N5, N6 and N7
// each consider N1-N7 as live and part of the cluster.
func TestDatadriven(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			t.Logf("config: %s", d.CmdArgs)

			var (
				impl             string
				nodes, bootstrap int
			)
			d.ScanArgs(t, "impl", &impl)
			d.ScanArgs(t, "nodes", &nodes)
			d.ScanArgs(t, "bootstrap", &bootstrap)

			network := newNetwork()
			for i := 1; i <= nodes; i++ {
				opts := []Option{WithID(ID(i)), WithImpl(impl)}
				if i == bootstrap {
					opts = append(opts, WithBootstrap())
				}

				l := Start(opts...) // closed by the network teardown below
				network.ls = append(network.ls, l)
			}

			teardown := network.tick(t)
			defer teardown()

			scanner := newScanner(strings.NewReader(d.Input), path)
			for scanner.Scan() {
				line := scanner.Text()
				receiver, fn, args, result := parse(t, line, d.Pos)
				switch fn {
				case "add": // recv.add(args)
					recv, err := strconv.Atoi(receiver)
					assert.NoError(t, err)
					l := network.get(ID(recv))

					for _, id := range expand(t, args) {
						assert.NoError(t, l.Add(context.Background(), ID(id)))
					}
				case "remove": // recv.remove(args)
					recv, err := strconv.Atoi(receiver)
					assert.NoError(t, err)
					l := network.get(ID(recv))

					for _, id := range expand(t, args) {
						assert.NoError(t, l.Remove(context.Background(), ID(id)))
					}
				case "live": // recv.live(args)			# live:[bool], found:[bool]
					testutils.SucceedsSoon(t, func() error {
						for _, recv := range expand(t, receiver) {
							l := network.get(ID(recv))

							for _, id := range expand(t, args) {
								live, found := l.Live(ID(id))
								if got := fmt.Sprintf("live:%t, found:%t", live, found); got != result {
									return fmt.Errorf("%d: expected results %q got %q", recv, result, got)
								}
							}
						}
						return nil
					})
				case "members": // recv.members()			# members:[nodes]
					testutils.SucceedsSoon(t, func() error {
						for _, recv := range expand(t, receiver) {
							l := network.get(ID(recv))

							members := l.Members()
							if got := fmt.Sprintf("members:%s", members); got != result {
								return fmt.Errorf("%d: expected results %q got %q", recv, result, got)
							}
						}

						return nil
					})
				default:
					t.Fatalf("unrecognized fn: %s", fn)
				}
			}

			return "ok"
		})
	})
}

// parses separates out a datadriven line into following components:
//
// 		receiver.fn(args)  # results
//
func parse(t *testing.T, line, pos string) (receiver, fn, args, results string) {
	var remaining string
	tokens := strings.SplitN(line, "#", 2)
	if len(tokens) > 2 {
		t.Fatalf("%s: malformed input: expecting a single '#' separator for the results", pos)
	}
	operation := strings.TrimSpace(tokens[0])
	if len(tokens) == 2 {
		results = strings.TrimSpace(tokens[1])
	}

	tokens = strings.SplitN(operation, ".", 2)
	if len(tokens) != 2 {
		t.Fatalf("%s: malformed input: expecting a single '.' separator for the receiver", pos)
	}
	receiver, remaining = tokens[0], tokens[1]

	tokens = strings.Split(remaining, "(")
	if len(tokens) != 2 {
		t.Fatalf("%s: malformed input: expecting a single '(' for the fn", pos)
	}
	fn, remaining = tokens[0], tokens[1]
	if !strings.HasSuffix(remaining, ")") {
		t.Fatalf("%s: malformed input: expecting a closing ')' for the args", pos)
	}

	tokens = strings.Split(remaining, ")")
	if len(tokens) != 2 {
		t.Fatalf("%s: malformed input: expecting a single ')' for the fn", pos)
	}

	args, remaining = tokens[0], tokens[1]
	if remaining != "" {
		t.Fatalf("%s: malformed input: expecting nothing after closing ')'", pos)
	}

	return receiver, fn, args, results
}

// expand returns the expanded set of ids implied by variadic and ranged args
// string.
//
//   expand(t, "1"): [1]
//   expand(t, "1,2"): [1 2]
//   expand(t, "1,3-5"): [1 3 4 5]
//
func expand(t *testing.T, args string) []int {
	var ids []int
	for _, arg := range strings.Split(args, ",") { // args are variadic, split on ','
		if parts := strings.Split(arg, "-"); len(parts) > 2 { // each arg can be a range
			t.Fatalf("malformed directive: %s, expected single '-'", args)
		} else if len(parts) == 2 {
			start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			assert.NoError(t, err)

			end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			assert.NoError(t, err)

			if end < start {
				start, end = end, start
			}
			for id := start; id <= end; id++ {
				ids = append(ids, id)
			}
			continue
		}

		id, err := strconv.Atoi(strings.TrimSpace(arg)) // each arg can be a single node
		assert.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

// scanner is a convenience wrapper around a bufio.Scanner that keeps track of
// the last read line number. It's also configured with an associated name for
// the reader (typically a file name) to generate positional error messages.
type scanner struct {
	*bufio.Scanner
	line int
	name string
}

func newScanner(r io.Reader, name string) *scanner {
	bufioScanner := bufio.NewScanner(r)
	// We use a large max-token-size to account for lines in the output that far
	// exceed the default bufio scanner token size.
	bufioScanner.Buffer(make([]byte, 100), 10*bufio.MaxScanTokenSize)
	return &scanner{
		Scanner: bufioScanner,
		name:    name,
	}
}

func (s *scanner) Scan() bool {
	ok := s.Scanner.Scan()
	if ok {
		s.line++
	}
	return ok
}

// pos is a file:line prefix for the input file, suitable for inclusion in logs
// and error messages.
func (s *scanner) pos() string {
	return fmt.Sprintf("%s:%d", s.name, s.line)
}
