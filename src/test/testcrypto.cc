#include "auth/Crypto.h"
#include "common/Clock.h"

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth

#define AES_KEY_LEN	16

int main(int argc, char *argv[])
{
  char aes_key[AES_KEY_LEN];
  memset(aes_key, 0x77, sizeof(aes_key));
  bufferptr keybuf(aes_key, sizeof(aes_key));
  CryptoKey key(CEPH_CRYPTO_AES, ceph_clock_now(g_ceph_context), keybuf);

  const char *msg="hello! this is a message\n";
  char pad[16];
  memset(pad, 0, 16);
  bufferptr ptr(msg, strlen(msg));
  bufferlist enc_in;
  enc_in.append(ptr);
  enc_in.append(msg, strlen(msg));

  bufferlist enc_out;
  std::string error;
  if (key.encrypt(g_ceph_context, enc_in, enc_out, &error) < 0) {
    assert(!error.empty());
    dout(0) << "couldn't encode! error " << error << dendl;
    exit(1);
  }

  const char *enc_buf = enc_out.c_str();
  for (unsigned i=0; i<enc_out.length(); i++) {
    std::cout << hex << (int)(unsigned char)enc_buf[i] << dec << " ";
    if (i && !(i%16))
      std::cout << std::endl;
  }

  bufferlist dec_in, dec_out;

  dec_in = enc_out;

  if (key.decrypt(g_ceph_context, dec_in, dec_out, &error) < 0) {
    assert(!error.empty());
    dout(0) << "couldn't decode! error " << error << dendl;
    exit(1);
  }

  dout(0) << "decoded len: " << dec_out.length() << dendl;
  dout(0) << "decoded msg: " << dec_out.c_str() << dendl;

  return 0;
}

