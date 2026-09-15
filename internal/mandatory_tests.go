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

import (
	"fmt"
	"os/exec"
	"strings"
)

func normalizeMandatoryTests(raw []string) ([]string, error) {
	tests := make([]string, 0, len(raw))
	for i, entry := range raw {
		command := strings.TrimSpace(entry)
		if command == "" {
			return nil, fmt.Errorf("MANDATORY_TESTS[%d] must not be empty", i)
		}
		tests = append(tests, command)
	}
	if len(tests) == 0 {
		return nil, fmt.Errorf("MANDATORY_TESTS must contain at least one command")
	}
	return tests, nil
}

func parseMandatoryTestCommand(raw string) (string, []string, error) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return "", nil, fmt.Errorf("command is empty")
	}
	return parts[0], parts[1:], nil
}

func validateMandatoryTestExecutables(commands []string) error {
	seen := make(map[string]struct{}, len(commands))
	for _, raw := range commands {
		name, _, err := parseMandatoryTestCommand(raw)
		if err != nil {
			return fmt.Errorf("invalid mandatory test command %q: %w", raw, err)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if _, err := exec.LookPath(name); err != nil {
			return fmt.Errorf("mandatory test executable not found in PATH: %s", name)
		}
	}
	return nil
}

func normalizeHardGateMode(raw string) (HardGateMode, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", string(HardGateModeParallel):
		return HardGateModeParallel, nil
	case string(HardGateModeSerial):
		return HardGateModeSerial, nil
	default:
		return "", fmt.Errorf("HARD_GATE_MODE must be SERIAL or PARALLEL")
	}
}

func formatMandatoryTestsMarkdownList(commands []string, indent string) string {
	return formatMandatoryTestsList(commands, indent, true)
}

func formatMandatoryTestsPlainList(commands []string, indent string) string {
	return formatMandatoryTestsList(commands, indent, false)
}

func formatMandatoryTestsList(commands []string, indent string, markdown bool) string {
	var builder strings.Builder
	for _, command := range commands {
		builder.WriteString(indent)
		builder.WriteString("- ")
		if markdown {
			builder.WriteString("`")
		}
		builder.WriteString(strings.TrimSpace(command))
		if markdown {
			builder.WriteString("`")
		}
		builder.WriteString("\n")
	}
	return strings.TrimSuffix(builder.String(), "\n")
}
