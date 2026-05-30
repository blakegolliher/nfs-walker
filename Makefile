# nfs-walker Makefile
#
# Targets:
#   make / make build  - Native release build (glibc-linked)
#   make release       - Static musl binary for distribution
#   make debug         - Debug build
#   make docker-rocky  - Rocky-9 build via Docker (analytics server + dashboard)
#   make docker-musl   - Static musl build via Docker
#   make test          - Run all tests
#   make check         - Run clippy + format check
#   make fmt           - Format code
#   make clean         - Remove all build artifacts
#   make install-deps  - Install system dependencies
#   make install-musl  - Install musl toolchain
#   make help          - Show this help

SHELL := /bin/bash
.PHONY: all build release debug test clean clean-cache install-deps install-musl check fmt help docker-rocky docker-musl info list

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
MUSL_TARGET := x86_64-unknown-linux-musl
MUSL_BIN := $(TARGET_DIR)/$(MUSL_TARGET)/release/$(PROJECT_NAME)

# Output binary names
RELEASE_BINARY := $(PROJECT_NAME)-$(VERSION)-$(DATE_STAMP)
LATEST_LINK := $(BUILD_DIR)/$(PROJECT_NAME)

# Colors
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m

#------------------------------------------------------------------------------
# Default target
#------------------------------------------------------------------------------
all: build

#------------------------------------------------------------------------------
# Native release build (links against system glibc + libnfs)
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
		echo -e "  Binary:  $(BUILD_DIR)/$(RELEASE_BINARY)"; \
		echo -e "  Symlink: $(LATEST_LINK) -> $(RELEASE_BINARY)"; \
		ls -lh $(BUILD_DIR)/$(RELEASE_BINARY) | awk '{print "  Size:    " $$5}'; \
	else \
		echo -e "$(RED)✗ Build failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/build.log for details"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Static musl binary — works on any Linux
#------------------------------------------------------------------------------
release:
	@echo -e "$(BLUE)Building $(PROJECT_NAME) v$(VERSION) (static musl)...$(NC)"
	@if ! rustup target list --installed | grep -q $(MUSL_TARGET); then \
		echo -e "$(YELLOW)Installing musl target...$(NC)"; \
		rustup target add $(MUSL_TARGET); \
	fi
	@if ! command -v musl-gcc &> /dev/null; then \
		echo -e "$(RED)✗ musl-gcc not found. Run: make install-musl$(NC)"; \
		exit 1; \
	fi
	@mkdir -p $(BUILD_DIR)
	@RUSTFLAGS='-C target-feature=+crt-static,+aes,+sse2' cargo build --release --target $(MUSL_TARGET) 2>&1 | tee $(BUILD_DIR)/build-release.log; \
	BUILD_STATUS=$${PIPESTATUS[0]}; \
	if [ $$BUILD_STATUS -eq 0 ]; then \
		RELEASE_NAME="$(PROJECT_NAME)-$(VERSION)-$(DATE_STAMP)-static"; \
		cp $(MUSL_BIN) $(BUILD_DIR)/$$RELEASE_NAME; \
		chmod +x $(BUILD_DIR)/$$RELEASE_NAME; \
		rm -f $(BUILD_DIR)/$(PROJECT_NAME)-static; \
		ln -s $$RELEASE_NAME $(BUILD_DIR)/$(PROJECT_NAME)-static; \
		echo -e "$(GREEN)✓ Static release build successful$(NC)"; \
		echo -e "  Binary:  $(BUILD_DIR)/$$RELEASE_NAME"; \
		echo -e "  Symlink: $(BUILD_DIR)/$(PROJECT_NAME)-static -> $$RELEASE_NAME"; \
		ls -lh $(BUILD_DIR)/$$RELEASE_NAME | awk '{print "  Size:    " $$5}'; \
		file $(BUILD_DIR)/$$RELEASE_NAME | sed 's/^/  /'; \
	else \
		echo -e "$(RED)✗ Static release build failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/build-release.log for details"; \
		echo -e "  You may need to run: make install-musl"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Debug build
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
# Rocky-9 build via Docker: analytics server + embedded React dashboard.
# Targets glibc 2.34 so it runs on Rocky/RHEL 9+, Ubuntu 22.04+, Debian 12+.
#------------------------------------------------------------------------------
docker-rocky:
	@echo -e "$(BLUE)Building $(PROJECT_NAME) v$(VERSION) (Rocky 9, server + dashboard)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@if command -v podman &> /dev/null; then \
		CONTAINER_CMD=podman; \
	elif command -v docker &> /dev/null; then \
		CONTAINER_CMD=docker; \
	else \
		echo -e "$(RED)✗ Neither podman nor docker found$(NC)"; \
		exit 1; \
	fi; \
	echo "Using $$CONTAINER_CMD..."; \
	$$CONTAINER_CMD build -f Dockerfile.rocky -t nfs-walker-rocky . 2>&1 | tee $(BUILD_DIR)/build-rocky.log; \
	BUILD_STATUS=$${PIPESTATUS[0]}; \
	if [ $$BUILD_STATUS -eq 0 ]; then \
		RELEASE_NAME="$(PROJECT_NAME)-$(VERSION)-$(DATE_STAMP)-el9"; \
		$$CONTAINER_CMD run --rm nfs-walker-rocky cat /build/nfs-walker > $(BUILD_DIR)/$$RELEASE_NAME; \
		chmod +x $(BUILD_DIR)/$$RELEASE_NAME; \
		rm -f $(BUILD_DIR)/$(PROJECT_NAME)-el9; \
		ln -s $$RELEASE_NAME $(BUILD_DIR)/$(PROJECT_NAME)-el9; \
		echo -e "$(GREEN)✓ Rocky 9 build successful$(NC)"; \
		echo -e "  Binary:  $(BUILD_DIR)/$$RELEASE_NAME"; \
		echo -e "  Symlink: $(BUILD_DIR)/$(PROJECT_NAME)-el9 -> $$RELEASE_NAME"; \
		ls -lh $(BUILD_DIR)/$$RELEASE_NAME | awk '{print "  Size:    " $$5}'; \
		echo -e "  Compatible with: Rocky/RHEL/Alma 9+, Ubuntu 22.04+, Debian 12+"; \
		file $(BUILD_DIR)/$$RELEASE_NAME | sed 's/^/  /'; \
	else \
		echo -e "$(RED)✗ Rocky 9 build failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/build-rocky.log for details"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Static musl build via Docker — works on ANY Linux.
#------------------------------------------------------------------------------
docker-musl:
	@echo -e "$(BLUE)Building $(PROJECT_NAME) v$(VERSION) (static musl via Docker)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@if command -v podman &> /dev/null; then \
		CONTAINER_CMD=podman; \
	elif command -v docker &> /dev/null; then \
		CONTAINER_CMD=docker; \
	else \
		echo -e "$(RED)✗ Neither podman nor docker found$(NC)"; \
		exit 1; \
	fi; \
	echo "Using $$CONTAINER_CMD..."; \
	$$CONTAINER_CMD build -f Dockerfile.musl -t nfs-walker-musl . 2>&1 | tee $(BUILD_DIR)/build-musl.log; \
	BUILD_STATUS=$${PIPESTATUS[0]}; \
	if [ $$BUILD_STATUS -eq 0 ]; then \
		RELEASE_NAME="$(PROJECT_NAME)-$(VERSION)-$(DATE_STAMP)-musl-static"; \
		$$CONTAINER_CMD run --rm nfs-walker-musl cat /app/target/x86_64-unknown-linux-musl/release-debug/nfs-walker > $(BUILD_DIR)/$$RELEASE_NAME; \
		chmod +x $(BUILD_DIR)/$$RELEASE_NAME; \
		rm -f $(BUILD_DIR)/$(PROJECT_NAME)-static; \
		ln -s $$RELEASE_NAME $(BUILD_DIR)/$(PROJECT_NAME)-static; \
		echo -e "$(GREEN)✓ Static musl build successful$(NC)"; \
		echo -e "  Binary:  $(BUILD_DIR)/$$RELEASE_NAME"; \
		echo -e "  Symlink: $(BUILD_DIR)/$(PROJECT_NAME)-static -> $$RELEASE_NAME"; \
		ls -lh $(BUILD_DIR)/$$RELEASE_NAME | awk '{print "  Size:    " $$5}'; \
		echo -e "  Type: Fully static (musl) — works on ANY Linux"; \
		file $(BUILD_DIR)/$$RELEASE_NAME | sed 's/^/  /'; \
	else \
		echo -e "$(RED)✗ Static musl build failed$(NC)"; \
		echo -e "  See $(BUILD_DIR)/build-musl.log for details"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Install musl toolchain for static builds
#------------------------------------------------------------------------------
install-musl:
	@echo -e "$(BLUE)Installing musl toolchain...$(NC)"
	@if command -v apt &> /dev/null; then \
		echo "  Installing musl-tools via apt"; \
		sudo apt update && sudo apt install -y musl-tools musl-dev; \
	elif command -v dnf &> /dev/null; then \
		echo "  Installing musl via dnf"; \
		sudo dnf install -y musl musl-devel musl-gcc; \
	else \
		echo -e "$(RED)✗ Unsupported package manager$(NC)"; \
		echo "  Please install musl-tools manually"; \
		exit 1; \
	fi
	@rustup target add $(MUSL_TARGET)
	@echo -e "$(GREEN)✓ Musl toolchain installed$(NC)"

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
# Install system dependencies (build-time)
#------------------------------------------------------------------------------
install-deps:
	@echo -e "$(BLUE)Installing system dependencies...$(NC)"
	@if command -v apt &> /dev/null; then \
		echo "  Using apt package manager"; \
		sudo apt update && sudo apt install -y \
			build-essential \
			pkg-config \
			libnfs-dev; \
		echo -e "$(GREEN)✓ Dependencies installed$(NC)"; \
	elif command -v dnf &> /dev/null; then \
		echo "  Using dnf package manager"; \
		sudo dnf install -y \
			gcc \
			make \
			pkg-config \
			libnfs-devel; \
		echo -e "$(GREEN)✓ Dependencies installed$(NC)"; \
	else \
		echo -e "$(RED)✗ Unsupported package manager$(NC)"; \
		echo "  Please install manually: build-essential pkg-config libnfs-dev"; \
		exit 1; \
	fi

#------------------------------------------------------------------------------
# Run clippy + format check
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
	@mkdir -p $(BUILD_DIR)
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
	@echo "  build           Native release build (links system libnfs/glibc)"
	@echo "  release         Static musl binary (any Linux)"
	@echo "  debug           Debug build"
	@echo "  docker-rocky    Rocky-9 build via Docker (server + dashboard, glibc 2.34+)"
	@echo "  docker-musl     Static musl build via Docker"
	@echo "  clean           Remove all build artifacts and cache"
	@echo "  clean-cache     Remove only cached objects"
	@echo ""
	@echo "Test targets:"
	@echo "  test            Run all tests with summary"
	@echo "  check           Run clippy and format check"
	@echo ""
	@echo "Utility targets:"
	@echo "  fmt             Format code with rustfmt"
	@echo "  install-deps    Install build-time system dependencies (libnfs)"
	@echo "  install-musl    Install musl toolchain for static builds"
	@echo "  info            Show project info"
	@echo "  list            List available binaries"
	@echo "  help            Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make build                              # Native release"
	@echo "  make test                               # Run tests"
	@echo "  make clean build                        # Clean rebuild"
	@echo ""
	@echo "After building:"
	@echo "  build/nfs-walker nfs://server/export -o scan.parquet -w 32"
	@echo "  build/nfs-walker stats scan.parquet"
	@echo "  duckdb -c \"SELECT count(*) FROM 'scan.parquet/scans/*/part-*.parquet'\""
	@echo ""
