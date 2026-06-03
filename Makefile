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