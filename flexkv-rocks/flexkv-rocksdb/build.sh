#!/bin/bash
export OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
export OPENSSL_INCLUDE_DIR=/usr/include/openssl
RUST_BACKTRACE=1 cargo build
