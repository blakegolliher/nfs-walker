# nfs-walker Makefile
#
# Targets:
#   make / make build  - Build release binary with date stamp
#   make debug         - Build debug binary
#   make test          - Run all tests
#   make bench         - Run benchmarks
#   make clean         - Remove all build artifacts
#   make install-deps  - Install system dependencies
#   make check         - Run clippy and format check
#   make fmt           - Format code
#   make help          - Show this help

SHELL := /bin/bash
.PHONY: all build debug test bench clean install-deps check fmt help

# Project info
PROJECT_NAME := nfs-walker
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
DATE_STAMP := $(shell date +%Y%m%d-%H%M%S)
GIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "nogit")

# Directories
BUILD_DIR := ./build
TARGET_DIR := ./target
RELEASE_BIN := $(TARGET_DIR)/release/$(PROJECT_NAME)
DEBUG_BIN := $(TARGET_DIR)/debug/$(PROJECT_NAME)

# Output binary names
RELEASE_BINARY := $(PROJECT_NAME)-$(VERSION)-$(DATE_STAMP)
LATEST_LINK := $(BUILD_DIR)/$(PROJECT_NAME)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

#------------------------------------------------------------------------------
# Default target
#------------------------------------------------------------------------------
all: build

#------------------------------------------------------------------------------
# Build release binary with date stamp
#------------------------------------------------------------------------------
build:
	@echo -e "$(BLUE)Building $(PROJECT_NAME) v$(VERSION)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@cargo build --release 2>&1 | tee $(BUILD_DIR)/build.log; \
	BUILD_STATUS=$${PIPESTATUS[0]}; \
	if [ $$BUILD_STATUS -eq 0 ]; then \
		cp $(RELEASE_BIN) $(BUILD_DIR)/$(RELEASE_BINARY); \
		chmod +x $(BUILD_DIR)/$(RELEASE_BINARY); \
		rm -f $(LATEST_LINK); \
		ln -s $(RELEASE_BINARY) $(LATEST_LINK); \
		echo -e "$(GREEN)✓ Build successful$(NC)"; \
		echo -e "  Binary: $(BUILD_DIR)/$(RELEASE_BINARY)"; \
		echo -e "  Symlink: $(LATEST_LINK) -> $(RELEASE_BINARY)"; \
		ls -lh $(BUILD_DIR)/$(RELEASE_BINARY) | awk '{print "  Size: " $$5}'; \
	else \
		echo -e "$(RED)✗ Build failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/build.log for details"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Build debug binary
#------------------------------------------------------------------------------
debug:
	@echo -e "$(BLUE)Building $(PROJECT_NAME) (debug)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@cargo build 2>&1 | tee $(BUILD_DIR)/build-debug.log; \
	BUILD_STATUS=$${PIPESTATUS[0]}; \
	if [ $$BUILD_STATUS -eq 0 ]; then \
		cp $(DEBUG_BIN) $(BUILD_DIR)/$(PROJECT_NAME)-debug; \
		echo -e "$(GREEN)✓ Debug build successful$(NC)"; \
		echo -e "  Binary: $(BUILD_DIR)/$(PROJECT_NAME)-debug"; \
	else \
		echo -e "$(RED)✗ Debug build failed$(NC)"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Run tests
#------------------------------------------------------------------------------
test:
	@echo -e "$(BLUE)Running tests...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@cargo test --no-fail-fast 2>&1 | tee $(BUILD_DIR)/test.log; \
	TEST_STATUS=$${PIPESTATUS[0]}; \
	echo ""; \
	echo -e "$(BLUE)Test Summary:$(NC)"; \
	echo "─────────────────────────────────────────────────"; \
	PASSED=$$(grep -c "test .* ok$$" $(BUILD_DIR)/test.log 2>/dev/null || echo 0); \
	FAILED=$$(grep -c "test .* FAILED$$" $(BUILD_DIR)/test.log 2>/dev/null || echo 0); \
	IGNORED=$$(grep -c "test .* ignored$$" $(BUILD_DIR)/test.log 2>/dev/null || echo 0); \
	echo -e "  $(GREEN)Passed:$(NC)  $$PASSED"; \
	echo -e "  $(RED)Failed:$(NC)  $$FAILED"; \
	echo -e "  $(YELLOW)Ignored:$(NC) $$IGNORED"; \
	echo "─────────────────────────────────────────────────"; \
	if [ $$TEST_STATUS -ne 0 ]; then \
		echo ""; \
		echo -e "$(RED)Failed tests:$(NC)"; \
		grep "test .* FAILED$$" $(BUILD_DIR)/test.log | sed 's/^/  /'; \
		echo ""; \
		echo -e "$(RED)✗ Tests failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/test.log for details"; \
		exit 1; \
	else \
		echo -e "$(GREEN)✓ All tests passed$(NC)"; \
	fi

#------------------------------------------------------------------------------
# Run benchmarks
#------------------------------------------------------------------------------
bench:
	@echo -e "$(BLUE)Running benchmarks...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@cargo bench 2>&1 | tee $(BUILD_DIR)/bench.log; \
	if [ $${PIPESTATUS[0]} -eq 0 ]; then \
		echo -e "$(GREEN)✓ Benchmarks complete$(NC)"; \
		echo -e "  Results: $(BUILD_DIR)/bench.log"; \
	else \
		echo -e "$(RED)✗ Benchmarks failed$(NC)"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Clean all build artifacts
#------------------------------------------------------------------------------
clean:
	@echo -e "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(TARGET_DIR)
	@rm -rf $(BUILD_DIR)
	@rm -f Cargo.lock
	@echo -e "  Removed $(TARGET_DIR)/"
	@echo -e "  Removed $(BUILD_DIR)/"
	@echo -e "  Removed Cargo.lock"
	@echo -e "$(GREEN)✓ Clean complete$(NC)"

#------------------------------------------------------------------------------
# Clean only cached objects (keeps Cargo.lock)
#------------------------------------------------------------------------------
clean-cache:
	@echo -e "$(BLUE)Cleaning cached objects...$(NC)"
	@cargo clean
	@rm -rf $(BUILD_DIR)/*.log
	@echo -e "$(GREEN)✓ Cache cleaned$(NC)"

#------------------------------------------------------------------------------
# Install system dependencies
#------------------------------------------------------------------------------
install-deps:
	@echo -e "$(BLUE)Installing system dependencies...$(NC)"
	@if command -v apt &> /dev/null; then \
		echo "  Using apt package manager"; \
		sudo apt update && sudo apt install -y \
			build-essential \
			pkg-config \
			libsqlite3-dev \
			libnfs-dev \
			libclang-dev \
			llvm-dev; \
		echo -e "$(GREEN)✓ Dependencies installed$(NC)"; \
	elif command -v dnf &> /dev/null; then \
		echo "  Using dnf package manager"; \
		sudo dnf install -y \
			gcc \
			pkg-config \
			sqlite-devel \
			libnfs-devel \
			clang-devel \
			llvm-devel; \
		echo -e "$(GREEN)✓ Dependencies installed$(NC)"; \
	else \
		echo -e "$(RED)✗ Unsupported package manager$(NC)"; \
		echo "  Please install manually: build-essential pkg-config libsqlite3-dev libnfs-dev libclang-dev"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Run clippy linter and format check
#------------------------------------------------------------------------------
check:
	@echo -e "$(BLUE)Running code checks...$(NC)"
	@echo ""
	@echo -e "$(BLUE)Checking formatting...$(NC)"
	@cargo fmt --check 2>&1 || { \
		echo -e "$(YELLOW)⚠ Code is not formatted. Run 'make fmt' to fix.$(NC)"; \
	}
	@echo ""
	@echo -e "$(BLUE)Running clippy...$(NC)"
	@cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tee $(BUILD_DIR)/clippy.log; \
	if [ $${PIPESTATUS[0]} -eq 0 ]; then \
		echo -e "$(GREEN)✓ All checks passed$(NC)"; \
	else \
		echo -e "$(RED)✗ Clippy found issues$(NC)"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Format code
#------------------------------------------------------------------------------
fmt:
	@echo -e "$(BLUE)Formatting code...$(NC)"
	@cargo fmt
	@echo -e "$(GREEN)✓ Code formatted$(NC)"

#------------------------------------------------------------------------------
# Show binary info
#------------------------------------------------------------------------------
info:
	@echo -e "$(BLUE)Project Info:$(NC)"
	@echo "  Name:    $(PROJECT_NAME)"
	@echo "  Version: $(VERSION)"
	@echo "  Git:     $(GIT_HASH)"
	@echo ""
	@echo -e "$(BLUE)Build Directory:$(NC)"
	@if [ -d $(BUILD_DIR) ]; then \
		ls -lah $(BUILD_DIR)/ 2>/dev/null | grep -v "^total" | grep -v "^d" | awk '{print "  " $$9 " (" $$5 ")"}'; \
	else \
		echo "  (not built yet)"; \
	fi

#------------------------------------------------------------------------------
# List available binaries
#------------------------------------------------------------------------------
list:
	@echo -e "$(BLUE)Available binaries in $(BUILD_DIR):$(NC)"
	@if [ -d $(BUILD_DIR) ]; then \
		ls -1t $(BUILD_DIR)/$(PROJECT_NAME)-* 2>/dev/null | head -10 | while read f; do \
			SIZE=$$(ls -lh "$$f" | awk '{print $$5}'); \
			if [ -L "$(LATEST_LINK)" ] && [ "$$(readlink -f $(LATEST_LINK))" = "$$(readlink -f $$f)" ]; then \
				echo -e "  $(GREEN)$$f ($$SIZE) <- latest$(NC)"; \
			else \
				echo "  $$f ($$SIZE)"; \
			fi; \
		done; \
	else \
		echo "  (no builds yet)"; \
	fi

#------------------------------------------------------------------------------
# Help
#------------------------------------------------------------------------------
help:
	@echo ""
	@echo -e "$(BLUE)nfs-walker Makefile$(NC)"
	@echo "─────────────────────────────────────────────────"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build targets:"
	@echo "  build        Build release binary with date stamp (default)"
	@echo "  debug        Build debug binary"
	@echo "  clean        Remove all build artifacts and cache"
	@echo "  clean-cache  Remove only cached objects"
	@echo ""
	@echo "Test targets:"
	@echo "  test         Run all tests with summary"
	@echo "  bench        Run benchmarks"
	@echo "  check        Run clippy and format check"
	@echo ""
	@echo "Utility targets:"
	@echo "  fmt          Format code with rustfmt"
	@echo "  install-deps Install system dependencies"
	@echo "  info         Show project info"
	@echo "  list         List available binaries"
	@echo "  help         Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make                 # Build release binary"
	@echo "  make test            # Run tests"
	@echo "  make clean build     # Clean rebuild"
	@echo ""
