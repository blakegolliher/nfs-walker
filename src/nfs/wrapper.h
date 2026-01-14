/*
 * Wrapper header for libnfs bindings
 *
 * This file includes the necessary libnfs headers for bindgen
 * to generate Rust FFI bindings.
 */

/* System headers required by libnfs */
#include <sys/types.h>
#include <sys/time.h>
#include <stdint.h>
#include <fcntl.h>

/* libnfs headers */
#include <nfsc/libnfs.h>
#include <nfsc/libnfs-raw.h>
