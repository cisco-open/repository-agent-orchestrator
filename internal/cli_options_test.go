// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orchestrator

import "testing"

func TestParseCLIOptions(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantPath     string
		wantHelp     bool
		wantVersion  bool
		wantClean    bool
		wantOverride agentProfileOverride
		wantErr      bool
	}{
		{
			name:     "config only",
			args:     []string{"repository-agent-orchestrator", "--config", "config/example.yaml"},
			wantPath: "config/example.yaml",
			wantHelp: false,
		},
		{
			name:        "version does not require config",
			args:        []string{"repository-agent-orchestrator", "--version"},
			wantVersion: true,
		},
		{
			name:      "config with clean",
			args:      []string{"repository-agent-orchestrator", "--clean", "--config", "config/example.yaml"},
			wantPath:  "config/example.yaml",
			wantClean: true,
		},
		{
			name:     "profile overrides with separate values",
			args:     []string{"repository-agent-orchestrator", "--config", "config/example.yaml", "--model", "gpt-5.6-sol", "--reasoning-effort", "xhigh"},
			wantPath: "config/example.yaml",
			wantOverride: agentProfileOverride{
				Model: "gpt-5.6-sol", ReasoningEffort: "xhigh", HasModel: true, HasReasoningEffort: true,
			},
		},
		{
			name:     "profile overrides with inline values",
			args:     []string{"repository-agent-orchestrator", "--model=gpt-5.6-terra", "--reasoning-effort=low", "--config=config/example.yaml"},
			wantPath: "config/example.yaml",
			wantOverride: agentProfileOverride{
				Model: "gpt-5.6-terra", ReasoningEffort: "low", HasModel: true, HasReasoningEffort: true,
			},
		},
		{
			name:    "doctor argument fails",
			args:    []string{"repository-agent-orchestrator", "doctor", "--config=config/example.yaml"},
			wantErr: true,
		},
		{
			name:     "help does not require config",
			args:     []string{"repository-agent-orchestrator", "--help"},
			wantHelp: true,
			wantErr:  false,
		},
		{
			name:    "missing config fails",
			args:    []string{"repository-agent-orchestrator"},
			wantErr: true,
		},
		{
			name:    "unknown argument fails",
			args:    []string{"repository-agent-orchestrator", "--unknown", "--config", "config/example.yaml"},
			wantErr: true,
		},
		{
			name:    "duplicate model override fails",
			args:    []string{"repository-agent-orchestrator", "--model", "first", "--model=second", "--config", "config/example.yaml"},
			wantErr: true,
		},
		{
			name:    "duplicate reasoning override fails",
			args:    []string{"repository-agent-orchestrator", "--reasoning-effort=low", "--reasoning-effort", "high", "--config", "config/example.yaml"},
			wantErr: true,
		},
		{
			name:    "empty model override fails",
			args:    []string{"repository-agent-orchestrator", "--model=", "--config", "config/example.yaml"},
			wantErr: true,
		},
		{
			name:    "missing reasoning override value fails",
			args:    []string{"repository-agent-orchestrator", "--config", "config/example.yaml", "--reasoning-effort"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCLIOptions(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("parseCLIOptions() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCLIOptions() error = %v", err)
			}
			if got.configPath != tc.wantPath {
				t.Fatalf("configPath = %q, want %q", got.configPath, tc.wantPath)
			}
			if got.showHelp != tc.wantHelp {
				t.Fatalf("showHelp = %v, want %v", got.showHelp, tc.wantHelp)
			}
			if got.showVersion != tc.wantVersion {
				t.Fatalf("showVersion = %v, want %v", got.showVersion, tc.wantVersion)
			}
			if got.cleanStart != tc.wantClean {
				t.Fatalf("cleanStart = %v, want %v", got.cleanStart, tc.wantClean)
			}
			if got.runtimeProfileOverride != tc.wantOverride {
				t.Fatalf("runtimeProfileOverride = %+v, want %+v", got.runtimeProfileOverride, tc.wantOverride)
			}
		})
	}
}
