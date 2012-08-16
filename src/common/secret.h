#ifndef CEPH_SECRET_H
#define CEPH_SECRET_H

#ifdef __cplusplus
extern "C" {
#endif

int read_secret_from_file(const char *filename, char *secret, size_t max_len);

/*
 * Attempts to add the secret to the kernel, but falls back to
 * the old secret= option if the kernel is too old.
 */
int get_secret_option(const char *secret, const char *key_name,
		      char *secret_option, size_t secret_option_len);

int set_kernel_secret(const char *secret, const char *key_name);

int is_kernel_secret(const char *key_name);

#ifdef __cplusplus
}
#endif

#endif
