#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#include <string.h>

int encode_base64(const char *in, int in_len, char *out, int out_len)
{
  BIO *bmem, *b64;
  BUF_MEM *bptr; 

  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, in, in_len);
  if (BIO_flush(b64) < 0) {
    return -1;
  }
  BIO_get_mem_ptr(b64, &bptr); 

  int len = BIO_pending(bmem);
  if (out_len <= len) {
    return -1;
  }
  memcpy(out, bptr->data, len);
  out[len - 1] = '\0';

  BIO_free_all(b64); 

  return 0;
}

int decode_base64(const char *in, int in_len, char *out, int out_len)
{
  BIO *b64, *bmem;
  int ret;
  char in_eol[in_len + 2];
  memcpy(in_eol, in, in_len);
  in_eol[in_len] = '\n';
  in_eol[in_len + 1] = '\0';

  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new_mem_buf((unsigned char *)in_eol, in_len + 1);
  bmem = BIO_push(b64, bmem);

  ret = BIO_read(bmem, out, out_len);

  BIO_free_all(bmem);

  return ret;
}
