# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2026 Cisco Systems, Inc. and its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BINARY_NAME := repository-agent-orchestrator
BINARY_DIR := bin
CMD_PATH := ./cmd
COVERAGE_DIR := .coverage
COVERAGE_PROFILE := $(COVERAGE_DIR)/coverage.out
COVERAGE_JSON := $(COVERAGE_DIR)/test.json
COVERAGE_HTML := $(COVERAGE_DIR)/coverage.html

.PHONY: build test test-review-replay test-coverage clean

build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/$(BINARY_NAME) $(CMD_PATH)

test:
	go test ./...

test-review-replay:
	go test ./internal -run '^TestAntiDripReplay' -count=1 -v

test-coverage:
	@mkdir -p $(COVERAGE_DIR)
	@set +e; \
	go test -json -covermode=atomic -coverprofile=$(COVERAGE_PROFILE) ./... > $(COVERAGE_JSON); \
	test_status=$$?; \
	if [ $$test_status -ne 0 ]; then \
		cat $(COVERAGE_JSON); \
	fi; \
	if [ -f "$(COVERAGE_PROFILE)" ]; then \
		go tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML); \
		coverage=$$(go tool cover -func=$(COVERAGE_PROFILE) | awk '/^total:/ {print $$3}'); \
	else \
		coverage="0.0%"; \
	fi; \
	passed=$$(grep -c '"Action":"pass".*"Test":"' $(COVERAGE_JSON) || true); \
	failed=$$(grep -c '"Action":"fail".*"Test":"' $(COVERAGE_JSON) || true); \
	skipped=$$(grep -c '"Action":"skip".*"Test":"' $(COVERAGE_JSON) || true); \
	printf '\n---- Coverage summary ----\n'; \
	printf 'unit:        coverage=%s passed=%s failed=%s skipped=%s\n' "$$coverage" "$$passed" "$$failed" "$$skipped"; \
	printf 'html_report: %s\n' "$(COVERAGE_HTML)"; \
	exit $$test_status

clean:
	rm -rf $(BINARY_DIR)
