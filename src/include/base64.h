#ifndef __BASE64_H
#define __BASE64_H

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

static int encode_base64(const char *in, int in_len, char *out, int out_len)
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

#endif
