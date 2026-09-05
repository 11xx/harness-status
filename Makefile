CARGO ?= cargo
INSTALL ?= install
BINDIR ?= $(or $(XDG_BIN_HOME),$(HOME)/.local/bin)
AI_BINDIR ?= $(BINDIR)/ai

.PHONY: all build install test

all: build

build:
	$(CARGO) build --release

install: build
	$(INSTALL) -Dm755 target/release/harness-status $(BINDIR)/harness-status
	$(INSTALL) -Dm755 helpers/ai-window-ping $(AI_BINDIR)/ai-window-ping

test:
	$(CARGO) test
