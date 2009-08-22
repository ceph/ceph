
#include <linux/err.h>
#include <linux/scatterlist.h>
#include <crypto/hash.h>
#include "crypt.h"

static struct crypto_blkcipher *tfm = NULL;

static void test_crypt(void)
{
#define AES_KEY_SIZE 16
	struct ceph_secret *secret =
		(struct ceph_secret *)kmalloc(sizeof(struct ceph_secret) + AES_KEY_SIZE, GFP_KERNEL);
	int res;

	const char *msg = "hello! this is a message\nhello! this is a message\n";
	char enc_buf[128];
	char dec_buf[128];

	size_t src_len, dst_len;

	memset(secret->key, 0x77, AES_KEY_SIZE);

	secret->type = CEPH_SECRET_AES;
	secret->len = AES_KEY_SIZE;

	src_len = strlen(msg) + 1;
	dst_len = sizeof(enc_buf);

	printk(KERN_ERR "dst_len=%d\n",(int)dst_len);

	res = ceph_encrypt(secret, enc_buf, &dst_len, msg, src_len - 1);
	printk(KERN_ERR "encrypted, res=%d\n", res);

	print_hex_dump(KERN_ERR, "", DUMP_PREFIX_NONE, 16, 1,
			enc_buf, dst_len, 1);

	src_len = dst_len;
	dst_len = sizeof(dec_buf);
	res = ceph_decrypt(secret, dec_buf, &dst_len, enc_buf, src_len);
	printk(KERN_ERR "decrypted, res=%d dec_buf=%s\n", res, dec_buf);
	printk(KERN_ERR "dst_len=%d\n", (int)dst_len);
}


int ceph_crypt_init(void)
{
	tfm = crypto_alloc_blkcipher("cbc(aes)", 0, CRYPTO_ALG_ASYNC);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);

	test_crypt();

	return 0;
}

void ceph_crypt_exit(void)
{
	if (tfm && (!IS_ERR(tfm)))
		crypto_free_blkcipher(tfm);
}

const u8 *aes_iv = "cephsageyudagreg";

int ceph_aes_encrypt(const char *key, int key_len, char *dst, size_t *dst_len,
		     const char *src, size_t src_len)
{
	struct scatterlist sg_in[2], sg_out[1];
	struct blkcipher_desc desc = { .tfm = tfm, .flags = 0 };
	int ret;
	void *iv;
	int ivsize;
	size_t zero_padding = (0x10 - (src_len & 0x0f));
	char pad[16];

	memset(pad, zero_padding, zero_padding);
	// pad[zero_padding - 1] = zero_padding;

	*dst_len = src_len + zero_padding;

	crypto_blkcipher_setkey((void *)tfm, key, key_len);
	sg_init_table(sg_in, 2);
	sg_init_table(sg_out, 1);
	sg_set_buf(&sg_in[0], src, src_len);
	sg_set_buf(&sg_in[1], pad, zero_padding);
	sg_set_buf(sg_out, dst, *dst_len);
	iv = crypto_blkcipher_crt(tfm)->iv;
	ivsize = crypto_blkcipher_ivsize(tfm);
	memcpy(iv, aes_iv, ivsize);
	print_hex_dump(KERN_ERR, "src", DUMP_PREFIX_NONE, 16, 1,
			src, src_len, 1);
	print_hex_dump(KERN_ERR, "pad", DUMP_PREFIX_NONE, 16, 1,
			pad, zero_padding, 1);
	ret = crypto_blkcipher_encrypt(&desc, sg_out, sg_in,
				     src_len + zero_padding);
	if (ret < 0) {
		printk(KERN_WARNING "cipher_encrypt failed\n");
	}
	return 0;
}

int ceph_aes_decrypt(const char *key, int key_len, char *dst, size_t *dst_len,
		     const char *src, size_t src_len)
{
	struct scatterlist sg_in[1], sg_out[1];
	struct blkcipher_desc desc = { .tfm = tfm };
	void *iv;
	int ivsize;
	int ret;
	int last_byte;

	crypto_blkcipher_setkey((void *)tfm, key, key_len);
	sg_init_table(sg_in, 1);
	sg_init_table(sg_out, 1);
	sg_set_buf(sg_in, src, src_len);
	sg_set_buf(sg_out, dst, *dst_len);

	iv = crypto_blkcipher_crt(tfm)->iv;
	ivsize = crypto_blkcipher_ivsize(tfm);
	memcpy(iv, aes_iv, ivsize);

	ret = crypto_blkcipher_decrypt(&desc, sg_out, sg_in,
				     src_len);
	if (ret < 0) {
		printk(KERN_WARNING "cipher_decrypt failed\n");
	}

	last_byte = dst[src_len - 1];
	if (last_byte <= 16) {
		if (src_len >= last_byte)
			*dst_len = src_len - last_byte;
		else
			*dst_len = 0;
	} else {
		/* not padded correctly! */
	}
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

int ceph_decrypt(struct ceph_secret *secret, char *dst, size_t *dst_len,
		 const char *src, size_t src_len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		if (*dst_len < src_len)
			return -ERANGE;
		memcpy(dst, src, src_len);
		*dst_len = src_len;
		return 0;

	case CEPH_SECRET_AES:
		return ceph_aes_decrypt(secret->key, slen, dst,
					dst_len, src, src_len);

	default:
		return -EINVAL;
	}
}


int ceph_encrypt(struct ceph_secret *secret, char *dst, size_t *dst_len,
		 const char *src, size_t src_len)
{
	int type = le16_to_cpu(secret->type);
	int slen = le16_to_cpu(secret->len);

	switch (type) {
	case CEPH_SECRET_NONE:
		if (*dst_len < src_len)
			return -ERANGE;
		memcpy(dst, src, src_len);
		*dst_len = src_len;
		return 0;

	case CEPH_SECRET_AES:
		return ceph_aes_encrypt(secret->key, slen, dst,
					dst_len, src, src_len);

	default:
		return -EINVAL;
	}
}
