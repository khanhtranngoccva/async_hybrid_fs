TARGET=$(shell rustc -vV | sed -n 's|host: ||p')
TSAN_FLAGS=-Zsanitizer=thread
ASAN_FLAGS=-Zsanitizer=address,leak
RSAN_FLAGS=-Zsanitizer=realtime

test:
	cargo test --release

tsan:
	RUSTFLAGS="$(TSAN_FLAGS)" RUSTDOCFLAGS="$(TSAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET)

asan:
	RUSTFLAGS="$(ASAN_FLAGS)" RUSTDOCFLAGS="$(ASAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET)

rsan:
	RUSTFLAGS="$(RSAN_FLAGS)" RUSTDOCFLAGS="$(RSAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET)

# Bootstraps macOS cross-compilation toolchain in ~/SDK/osxcross
bootstrap-macos:
	# Base dependencies
	sudo apt install clang gcc g++ zlib1g-dev libmpc-dev libmpfr-dev libgmp-dev
	# Rust targets
	rustup target add x86_64-apple-darwin
	rustup target add aarch64-apple-darwin
	# Download osxcross
	mkdir -p ~/SDK
ifeq ("$(wildcard ~/SDK/osxcross)", "")
	git clone https://github.com/tpoechtrager/osxcross ~/SDK/osxcross
endif
	# Download macOS SDK and copy to tarball directory
	wget -nc https://github.com/joseluisq/macosx-sdks/releases/download/26.1/MacOSX26.1.sdk.tar.xz
	mv MacOSX26.1.sdk.tar.xz ~/SDK/osxcross
	UNATTENDED=yes SDK_VERSION=26.1 OSX_VERSION_MIN=10.7 ~/SDK/osxcross/build.sh

# Initializes the ARM64 macOS cross compilation mode. Must run bootstrap-macos first.
load-cross-aarch64-apple-darwin:
	cp -v .cargo/cross-compilation.toml.aarch64-apple-darwin .cargo/cross-compilation.toml

# Initializes the x86_64 macOS cross compilation mode. Must run bootstrap-macos first.
load-cross-x86_64-apple-darwin:
	cp -v .cargo/cross-compilation.toml.x86_64-apple-darwin .cargo/cross-compilation.toml

unload-cross:
	rm -f .cargo/cross-compilation.toml