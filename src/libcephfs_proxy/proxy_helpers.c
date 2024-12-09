
#include "proxy_helpers.h"

#include <openssl/evp.h>

static const char hex_digits[] = "0123456789abcdef";

int32_t proxy_hash(uint8_t *hash, size_t size,
		   int32_t (*feed)(void **, void *, int32_t), void *data)
{
	EVP_MD_CTX *ctx;
	void *ptr;
	uint32_t bytes;
	int32_t i, err, len;

	if (size < 32) {
		return proxy_log(LOG_ERR, ENOBUFS,
				 "Digest buffer is too small");
	}

	ctx = EVP_MD_CTX_new();
	if (ctx == NULL) {
		return proxy_log(LOG_ERR, ENOMEM, "EVP_MD_CTX_new() failed");
	}

	if (!EVP_DigestInit_ex2(ctx, EVP_sha256(), NULL)) {
		err = proxy_log(LOG_ERR, ENOMEM, "EVP_DigestInit_ex2() failed");
		goto done;
	}

	i = 0;
	while ((len = feed(&ptr, data, i)) > 0) {
		if (!EVP_DigestUpdate(ctx, ptr, len)) {
			err = proxy_log(LOG_ERR, ENOMEM,
					"EVP_DigestUpdate() failed");
			goto done;
		}
		i++;
	}
	if (len < 0) {
		err = len;
		goto done;
	}

	if (!EVP_DigestFinal_ex(ctx, hash, &bytes)) {
		err = proxy_log(LOG_ERR, ENOMEM, "EVP_DigestFinal_ex() failed");
		goto done;
	}

	err = 0;

done:
	EVP_MD_CTX_free(ctx);

	return err;
}

int32_t proxy_hash_hex(char *digest, size_t size,
		       int32_t (*feed)(void **, void *, int32_t), void *data)
{
	uint8_t hash[32];
	int32_t i, err;

	if (size < 65) {
		return proxy_log(LOG_ERR, ENOBUFS,
				 "Digest buffer is too small");
	}

	err = proxy_hash(hash, sizeof(hash), feed, data);
	if (err < 0) {
		return err;
	}

	for (i = 0; i < 32; i++) {
		*digest++ = hex_digits[hash[i] >> 4];
		*digest++ = hex_digits[hash[i] & 15];
	}
	*digest = 0;

	return 0;
}
