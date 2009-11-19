#ifndef __BASE64_H
#define __BASE64_H

extern "C" {
  int encode_base64(const char *in, int in_len, char *out, int out_len);
  int decode_base64(const char *in, int in_len, char *out, int out_len);
}

#endif
