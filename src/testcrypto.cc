#include "auth/CryptoTools.h"

#include "config.h"


#define AES_KEY_LEN	16

int main(int argc, char *argv[])
{
  CryptoHandler *handler = ceph_crypto_mgr.get_crypto(CEPH_CRYPTO_AES);

  if (!handler) {
    derr(0) << "handler == NULL" << dendl;
    exit(1);
  }

  char aes_key[AES_KEY_LEN];
  memset(aes_key, 0x77, sizeof(aes_key));
  bufferlist keybl;
  keybl.append(aes_key, sizeof(aes_key));
  EntitySecret key(keybl);

  const char *msg="hello! this is a message\n";
  bufferlist enc_in;
  enc_in.append(msg, strlen(msg) + 1);
  bufferlist enc_out;

  if (!handler->encrypt(key, enc_in, enc_out)) {
    derr(0) << "couldn't encode!" << dendl;
    exit(1);
  }

  bufferlist dec_in, dec_out;

  dec_in = enc_out;

  if (!handler->decrypt(key, dec_in, dec_out)) {
    derr(0) << "couldn't decode!" << dendl;
  }

  dout(0) << "decoded msg: " << dec_out.c_str() << dendl;

  return 0;
}

