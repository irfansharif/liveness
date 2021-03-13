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

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

type simulator struct {
	log *log.Logger
	cmd *cobra.Command
}

func newSimulator() *simulator {
	s := &simulator{}
	s.log = log.New(os.Stderr, "INFO: ", 0) // used for debug logging (see --quiet)
	s.cmd = &cobra.Command{
		Use:     "lsimulator [command] (flags)",
		Short:   "lsimulator sets up an environment to evaluate different failure detectors.",
		Version: "v0.0",
		Long: `
lsimulator sets up an environment to evaluate different failure detectors. It
provides control knobs to set up various kind of failures and can be configured
with different failure detector implementations. The results of the simulation
are printed out at the end for comparative analysis.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodes := mustGetIntFlag(cmd, "nodes")
			fdetector := mustGetStringFlag(cmd, "fdetector")
			duration := mustGetDurationFlag(cmd, "duration")
			return s.run(nodes, fdetector, duration)
		},
		// Disable automatic printing of usage information whenever an error
		// occurs. We expect the errors to be informative enough.
		SilenceUsage: true,
		// Disable automatic printing of the error. We want to also print
		// details and hints, which cobra does not do for us. Instead we do the
		// printing in the command implementation.
		SilenceErrors: true,
	}

	// Set up the necessary flags.
	var quietVar bool
	s.cmd.Flags().BoolVar(&quietVar, "quiet", false, "disable logging")
	s.cmd.Flags().Int("nodes", 3, "the number of nodes to create")
	s.cmd.Flags().String("fdetector", "crdb", "the failure detector to use (one of crdb, gossip)")
	s.cmd.Flags().Duration("duration", time.Minute, "timeout for test")
	s.cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if quietVar {
			s.log.SetOutput(ioutil.Discard)
		}

		fdetector := mustGetStringFlag(cmd, "fdetector")
		if fdetector != "crdb" && fdetector != "gossip" {
			return fmt.Errorf("unrecognized failure detector %q, expecting either %q or %q", fdetector, "crdb", "gossip")
		}

		nodes := mustGetIntFlag(cmd, "nodes")
		if nodes <= 0 {
			return fmt.Errorf("unexpected node count: %d, expected greater than zero", nodes)
		}
		return nil
	}

	// Hide the `help` sub-command.
	s.cmd.SetHelpCommand(&cobra.Command{
		Use:    "noop-help",
		Hidden: true,
	})

	return s
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("")

	cli := newSimulator()
	if err := cli.cmd.Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
