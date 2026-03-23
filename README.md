# async_hybrid_fs

Linux-first asynchronous I/O library for end-user systems using optional `io_uring`.

This crate aims to enhance performance for asynchronous I/O operations by leveraging true asynchronous APIs whenever possible, while allowing ease of use by working as a drop-in replacement for `tokio::fs` and `std::fs` and falling back to the async runtime when asynchronous APIs are not available or blocked.

Unlike other `io_uring` crates which primarily target servers, this library is primarily targeted at applications that can benefit from fewer syscalls and run on end-user/consumer devices like a custom filesystem, on which support for `io_uring` is not guaranteed or `io_uring` is restricted by policy.

## Comparison with original library
- This crate is directly based on the `uring_file`(https://docs.rs/uring-file/latest/uring_file) crate by [wilsonzlin](https://github.com/wilsonzlin), but with some breaking modifications:
    - Features a more complete set of filesystem APIs, which is a superset of [`std::fs`]. This includes abstractions for race-free APIs like the "*at" syscall family, as well as the vectored I/O APIs.
    - Dynamically detects whether `io_uring` is available on the system supported for the operation, and falls back to the async runtime's methods when it is not.
    - Registered files types use safe implementations and are dropped when out of scope.

## Limitations
- This crate prioritizes ease of use, API completeness and compatibility with end-user devices over raw throughput.
- The interface provided by this crate may not be identical to `std::fs`. 

## Credits
- `uring-file` by wilsonzlin: https://github.com/wilsonzlin/monorepo-rs

## License
MIT OR Apache-2.0