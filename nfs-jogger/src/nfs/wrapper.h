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
#include <nfsc/libnfs-raw-nfs.h>
#include <nfsc/libnfs-raw-mount.h>

/*
 * Manually declare nfs_get_rootfh from libnfs-private.h
 * to avoid including the entire private header with its dependencies.
 * This struct and function are stable in libnfs.
 */
struct nfs_fh {
    int len;
    char *val;
};

const struct nfs_fh *nfs_get_rootfh(struct nfs_context *nfs);
