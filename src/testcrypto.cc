#include "auth/Crypto.h"

#include "config.h"


#define AES_KEY_LEN	16

int main(int argc, char *argv[])
{
  char aes_key[AES_KEY_LEN];
  memset(aes_key, 0x77, sizeof(aes_key));
  bufferptr keybuf(aes_key, sizeof(aes_key));
  CryptoKey key(CEPH_SECRET_AES, g_clock.now(), keybuf);

  const char *msg="hello! this is a message\n";
  bufferptr ptr(msg, strlen(msg));
  bufferlist enc_in;
  enc_in.append(ptr);
  enc_in.append(ptr);
  bufferlist enc_out;

  if (key.encrypt(enc_in, enc_out) < 0) {
    derr(0) << "couldn't encode!" << dendl;
    exit(1);
  }

  bufferlist dec_in, dec_out;

  dec_in = enc_out;

  if (key.decrypt(dec_in, dec_out) < 0) {
    derr(0) << "couldn't decode!" << dendl;
  }

  dout(0) << "decoded msg: " << dec_out.c_str() << dendl;

  return 0;
}

