
#include <linux/err.h>
#include <linux/scatterlist.h>
#include <crypto/hash.h>
#include "crypt.h"

static struct crypto_blkcipher *tfm = NULL;

int ceph_crypt_init(void)
{
	tfm = crypto_alloc_blkcipher("aes", 0, CRYPTO_ALG_ASYNC);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);
	return 0;
}

void ceph_crypt_exit(void)
{
	if (tfm && (!IS_ERR(tfm)))
		crypto_free_blkcipher(tfm);
}

const u8 *aes_iv = "cephsageyudagreg";

int ceph_aes_encrypt(const char *key, int key_len, char *dst,
		     const char *src, size_t len)
{
	struct scatterlist sg_in[1], sg_out[1];
	struct blkcipher_desc desc = { .tfm = tfm };

	crypto_blkcipher_setkey((void *)tfm, key, key_len);
	sg_init_table(sg_in, 1);
	sg_init_table(sg_out, 1);
	sg_set_buf(sg_in, src, len);
	sg_set_buf(sg_out, dst, len);
	crypto_blkcipher_set_iv(tfm, aes_iv, sizeof(aes_iv) - 1);
	if (crypto_blkcipher_encrypt(&desc, sg_out, sg_in,
				     len) != 0) {
		printk(KERN_WARNING "cipher_encrypt failed\n");
	}
	return 0;
}

int ceph_aes_decrypt(const char *key, int key_len, char *dst,
		     const char *src, size_t len)
{
	struct scatterlist sg_in[1], sg_out[1];
	struct blkcipher_desc desc = { .tfm = tfm };

	crypto_blkcipher_setkey((void *)tfm, key, key_len);
	sg_init_table(sg_in, 1);
	sg_init_table(sg_out, 1);
	sg_set_buf(sg_in, src, len);
	sg_set_buf(sg_out, dst, len);
	crypto_blkcipher_set_iv(tfm, aes_iv, sizeof(aes_iv) - 1);
	if (crypto_blkcipher_decrypt(&desc, sg_out, sg_in,
				     len) != 0) {
		printk(KERN_WARNING "cipher_encrypt failed\n");
	}
	//crypto_blkcipher_decrypt_iv((void *)tfm, dst, src, len, aes_iv);
	return 0;
}

struct ceph_buffer *ceph_secret_unarmor(const char *key, int len)
{
	struct ceph_buffer *b;
	int blen = len * 3 / 4;
	
	b = ceph_buffer_new_alloc(blen, GFP_NOFS);
	if (!b)
		return ERR_PTR(-ENOMEM);
	blen = ceph_unarmor(b->vec.iov_base, key, key+len);
	if (blen < 0) {
		ceph_buffer_put(b);
		return ERR_PTR(blen);
	}
	b->vec.iov_len = blen;
	return b;
}

int ceph_decrypt(struct ceph_secret *secret, char *dst, const char *src,
		 size_t len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		memcpy(dst, src, len);
		return 0;

	case CEPH_SECRET_AES:
		return ceph_aes_decrypt(secret->key, slen, dst, src, len);

	default:
		return -EINVAL;
	}
}


int ceph_encrypt(struct ceph_secret *secret, char *dst, const char *src,
		 size_t len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		memcpy(dst, src, len);
		return 0;

	case CEPH_SECRET_AES:
		return ceph_aes_encrypt(secret->key, slen, dst, src, len);

	default:
		return -EINVAL;
	}
}
