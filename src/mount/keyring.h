#ifndef _SRC_MOUNT_KEYRING_H
#define _SRC_MOUNT_KEYRING_H

#ifdef __cplusplus
extern "C" {
#endif

int get_keyring_secret(const char *name, char *secret, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* _SRC_MOUNT_KEYRING_H */
