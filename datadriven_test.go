package liveness

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/irfansharif/liveness/testutils"
	"github.com/stretchr/testify/assert"
)

var showLogs = flag.Bool("show-logs", false, "show-logs: print additional output")

// TestDatadriven simulates the scripts under testdata/. The syntax is as
// follows:
//
// 		liveness impl=[string] nodes=[int] bootstrap=[int]
//      receiver.fn(args)       # results we're waiting for before proceeding
//      ...
//      ----
//      ok
//
// The simulator waits for the expected results declared in each step before
// proceeding. For some methods, both the receiver and args can be variadic and
// ranged. Consider the following:
//
//      1-2.members()         # members:[1 2 3 4 5 6 7]
//      3,5-7.live(1-7)       # live:true, found:true
//
// The first line asserts that N1 and N2's view of the cluster membership will
// soon include N1-N7. The second line asserts the {N3,N5,N6,N7} will soon each
// consider N1-N7 as live and part of the cluster. Some methods don't accept
// variadic+ranged receivers, or expected results (we're simply asserting that
// the operations will be successful).
//
//      3.add(5-9)
//      8.remove(5)
func TestDatadriven(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			t.Logf("config: %s", d.CmdArgs)

			var (
				impl             string
				nodes, bootstrap int
			)
			d.ScanArgs(t, "impl", &impl)
			d.ScanArgs(t, "bootstrap", &bootstrap)
			d.ScanArgs(t, "nodes", &nodes)

			// Instantiate a cluster using the specific implementation,
			// bootstrapping node, and cluster size.
			network := newNetwork()
			for i := 1; i <= nodes; i++ {
				opts := []Option{WithID(ID(i)), WithImpl(impl)}
				if i == bootstrap {
					opts = append(opts, WithBootstrap())
				}
				if *showLogs {
					opts = append(opts, WithLoggingTo(os.Stderr))
				}

				l := Start(opts...) // closed by the network teardown below
				network.ls = append(network.ls, l)
			}

			teardown := network.tick(t)
			defer teardown()

			s := newScanner(t, strings.NewReader(d.Input), path)
			for s.Scan() {
				line := s.Text()
				s.Logf("executing: %s", line)
				receiver, fn, arg, result := parse(s, line)
				switch fn {
				case "add": // node.add(others)
					r := number(s, receiver)
					l := network.get(ID(r))
					for _, id := range expand(s, arg) {
						assert.NoError(t, l.Add(context.Background(), ID(id)))
					}
				case "remove": // node.remove(others)
					r := number(s, receiver)
					l := network.get(ID(r))
					for _, id := range expand(s, arg) {
						assert.NoError(t, l.Remove(context.Background(), ID(id)))
					}
				case "live": // nodes.live(others)  # live:[bool], found:[bool]
					testutils.SucceedsSoon(s, func() error {
						for _, recv := range expand(s, receiver) {
							l := network.get(ID(recv))

							for _, id := range expand(s, arg) {
								live, found := l.Live(ID(id))
								if got := fmt.Sprintf("live:%t, found:%t", live, found); got != result {
									return fmt.Errorf("%d: expected results %q got %q", recv, result, got)
								}
							}
						}
						return nil
					})
				case "members": // nodes.members()	# members:[nodes]
					testutils.SucceedsSoon(s, func() error {
						for _, recv := range expand(s, receiver) {
							l := network.get(ID(recv))

							members := l.Members()
							got := strings.Join(strings.Split(fmt.Sprintf("%s", members), " "), ", ")
							if fmt.Sprintf("members:%s", got) != result {
								return fmt.Errorf("%d: expected results %q got %q", recv, result, got)
							}
						}

						return nil
					})
				case "partition":
					assert.Equal(s, "nw", receiver)

					var partitions [][]ID
					parts := args(s, arg)
					if len(parts) == 0 {
						s.Fatalf("unexpected empty arg for nw.partition")
					}

					for _, partition := range parts {
						var ids []ID
						for _, el := range elems(s, partition) {
							for _, id := range expand(s, el) { // each element
								ids = append(ids, ID(id))
							}
						}
						partitions = append(partitions, ids)
					}

					if err := network.partition(t, partitions); err != nil {
						s.Fatal(err)
					}
				case "sever":
					assert.Equal(s, "nw", receiver)
					if len(args(s, arg)) == 0 {
						s.Fatalf("unexpected empty arg for nw.sever")
					}

					for _, arg := range args(s, arg) {
						from, to, bi := plink(s, arg)
						network.sever(ID(from), ID(to))
						if bi {
							network.sever(ID(to), ID(from))
						}
					}
				case "heal":
					assert.Equal(s, "nw", receiver)
					if len(args(s, arg)) == 0 { // nw.heal() is a shorthand to heal all links
						for i := 1; i <= nodes; i++ {
							for j := 1; j <= nodes; j++ {
								network.heal(ID(i), ID(j))
								network.heal(ID(j), ID(i))
							}
						}
						continue
					}
					for _, arg := range args(s, arg) {
						from, to, bi := plink(s, arg)
						network.heal(ID(from), ID(to))
						if bi {
							network.heal(ID(to), ID(from))
						}
					}
				case "start":
					for _, recv := range expand(s, receiver) {
						network.restart(ID(recv))
					}
				case "stop":
					for _, recv := range expand(s, receiver) {
						network.stop(ID(recv))
					}
				default:
					t.Fatalf("unrecognized fn: %s", fn)
				}
			}

			return "ok"
		})
	})
}

// parse separates out a test line of the following form into its individual
// components.
//
// 		receiver.fn(arg)  # results
//
func parse(s *scanner, line string) (receiver, fn, arg, results string) {
	var remaining string
	tokens := strings.SplitN(line, "#", 2)
	if len(tokens) > 2 {
		s.Fatalf("malformed input: expecting a single '#' separator for the results")
	}
	operation := strings.TrimSpace(tokens[0])
	if len(tokens) == 2 {
		results = strings.TrimSpace(tokens[1])
	}

	tokens = strings.SplitN(operation, ".", 2)
	if len(tokens) != 2 {
		s.Fatalf("malformed input: expecting a single '.' separator for the receiver")
	}
	receiver, remaining = tokens[0], tokens[1]

	tokens = strings.Split(remaining, "(")
	if len(tokens) != 2 {
		s.Fatalf("malformed input: expecting a single '(' for the fn")
	}
	fn, remaining = tokens[0], tokens[1]
	if !strings.HasSuffix(remaining, ")") {
		s.Fatalf("malformed input: expecting a closing ')' for the arg")
	}

	tokens = strings.Split(remaining, ")")
	if len(tokens) != 2 {
		s.Fatalf("malformed input: expecting a single ')' for the fn")
	}

	arg, remaining = tokens[0], tokens[1]
	if remaining != "" {
		s.Fatalf("malformed input: expecting nothing after closing ')'")
	}

	return receiver, fn, arg, results
}

// expand returns the expanded set of ints implied by variadic and ranged arg
// string.
//
//   expand("1")  			[1]
//   expand("1,2")  		[1 2]
//   expand("1,3-5")  		[1 3 4 5]
//
func expand(s *scanner, token string) []int {
	var ints []int
	for _, arg := range args(s, token) { // token is variadic, split on ','
		if parts := strings.Split(arg, "-"); len(parts) > 2 { // each token can be a range
			s.Fatalf("malformed input: expected single '-', got %q", arg)
		} else if len(parts) == 2 {
			start, end := number(s, parts[0]), number(s, parts[1])
			if end < start {
				start, end = end, start
			}
			for i := start; i <= end; i++ {
				ints = append(ints, i)
			}
			continue
		}

		ints = append(ints, number(s, arg)) // each token can be a single integer
	}
	return ints
}

// args splits off the individual comma separated arguments within a function's
// arg. Arguments are allowed to be nested (in array form), but not arbitrarily
// so (no array of arrays).
//
//   args("a")			["a"]
//   args("a,b")		["a", "b"]
//   args("a,b,2-5")	["a", "b", "2-5"]
//   args("a,b,[c,d]")	["a", "b", "[c,d]"]
//
func args(s *scanner, token string) []string {
	token = strings.TrimSpace(token)

	var parts []string
	for {
		if !strings.ContainsAny(token, "[]") {
			for _, part := range strings.Split(token, ",") {
				if part == "" || part == " " {
					continue
				}
				parts = append(parts, part)
			}
			break
		}

		start := strings.Index(token, "[")
		end := strings.Index(token, "]")
		if end < start {
			s.Fatalf("malformed input: ']' appearing before '[' in %q", token)
		}

		if len(token[:start]) > 0 { // capture elems before array
			token = strings.TrimSuffix(token, ",")
			for _, part := range strings.Split(token[:start], ",") {
				if part == "" || part == " " {
					continue
				}
				parts = append(parts, part)
			}
		}
		parts = append(parts, token[start:end+1]) // capture array
		token = token[end+1:]                     // trim processed prefix
	}

	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// number parses an integer out of the given string.
//
//   number("3")		3
//   number("42")		42
//
func number(s *scanner, token string) int {
	n, err := strconv.Atoi(strings.TrimSpace(token))
	if err != nil {
		s.Fatalf("malformed input: expected number, got %q", token)
	}
	assert.NoError(s, err)
	return n
}

// elems parses out array elements in the given string.
//
//   elems("[1]")		["1"]
//   elems("[1,abc]")	["1", "abc]
//
func elems(s *scanner, token string) []string {
	token = unparen(s, token)
	return strings.Split(token, ",")
}

// unparen returns the string contained between opening and closing parens.
//
//   unparen("[]")		""
//   unparen("[a]")		"a"
//   unparen("[1,ab]")	"1,ab"
//
func unparen(s *scanner, token string) string {
	if !strings.HasPrefix(token, "[") || !strings.HasSuffix(token, "]") {
		s.Fatalf("malformed input: expected %q to be surrounded by []", token)
	}
	token = strings.TrimPrefix(token, "[")
	token = strings.TrimSuffix(token, "]")
	return token
}

// plink parses out the string representation of a network link between two
// nodes.
//
//   plink("1->2")		1, 2, false
//   plink("2<->3")		2, 3, true
func plink(s *scanner, token string) (from int, to int, bi bool) {
	if !strings.Contains(token, "->") && !strings.Contains(token, "<->") {
		s.Fatalf("malformed input: expected %q to specify directional link (<- or <->)", token)
	}

	var parts []string
	if strings.Contains(token, "<->") {
		bi = true
		parts = strings.Split(token, "<->")
	} else {
		parts = strings.Split(token, "->")
	}

	if len(parts) != 2 {
		s.Fatalf("malformed input: expected %q to contain single bidirectional link <->, found %d", token, len(parts))
	}

	parts[0], parts[1] = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	from, to = number(s, parts[0]), number(s, parts[1])
	return from, to, bi
}

// scanner is a convenience wrapper around a bufio.Scanner that keeps track of
// the last read line number. It also:
// - captures an associated name for the reader (typically a file name) to
//   generate positional error messages.
// - embeds a *testing.T to automatically record errors with the position it
//   corresponds to.
type scanner struct {
	*testing.T
	*bufio.Scanner
	line int
	name string
}

func newScanner(t *testing.T, r io.Reader, name string) *scanner {
	bufioScanner := bufio.NewScanner(r)
	// We use a large max-token-size to account for lines in the output that far
	// exceed the default bufio scanner token size.
	bufioScanner.Buffer(make([]byte, 100), 10*bufio.MaxScanTokenSize)
	return &scanner{
		T:       t,
		Scanner: bufioScanner,
		name:    name,
	}
}

// Scan is a thin wrapper around the bufio scanner's interface.
func (s *scanner) Scan() bool {
	ok := s.Scanner.Scan()
	if ok {
		s.line++
	}
	return ok
}

// Fatal is thin wrapper around testing.T's interface.
func (s *scanner) Fatal(args ...interface{}) {
	s.T.Fatalf("%s: %s", s.pos(), fmt.Sprint(args...))
}

// Fatalf is thin wrapper around testing.T's interface.
func (s *scanner) Fatalf(format string, args ...interface{}) {
	s.T.Fatalf("%s: %s", s.pos(), fmt.Sprintf(format, args...))
}

// Error is thin wrapper around testing.T's interface.
func (s *scanner) Error(args ...interface{}) {
	s.T.Errorf("%s: %s", s.pos(), fmt.Sprint(args...))
}

// Errorf is thin wrapper around testing.T's interface.
func (s *scanner) Errorf(format string, args ...interface{}) {
	s.T.Errorf("%s: %s", s.pos(), fmt.Sprintf(format, args...))
}

// pos is a file:line prefix for the input file, suitable for inclusion in logs
// and error messages.
func (s *scanner) pos() string {
	return fmt.Sprintf("%s:%d", s.name, s.line)
}
