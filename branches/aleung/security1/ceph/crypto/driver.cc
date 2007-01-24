/******************************
 * Cryptographic library for Ceph.
 *
 * This class implements all of the cryptgraphic functions
 * necessary to protect and secure Ceph. This includes
 * on-wire protection, enforced access control, prevention
 * of replay,M-in-M attcks, DDOS, etc...
 *
 * This library exports a flat cryptographic suite
 * which exposes templated functions. Each function
 * supports multiple
 *
 * Author: Andrew Leung Nov., 2006
 ******************************/
#include"CryptoLib.h"
#include<iostream>

using namespace std;

int main(int argc, char* argv[]) {
  // message to hash
  const byte* msg = (const byte*)"hash me";

  // sha-1
  byte digest[CryptoLib::SHA1DIGESTSIZE];
  byte digestHex[2*CryptoLib::SHA1DIGESTSIZE];
  CryptoLib myCryptoLib;
  myCryptoLib.sha1(msg,digest,strlen((const char*)msg));
  myCryptoLib.toHex(digest, digestHex, CryptoLib::SHA1DIGESTSIZE, 2*CryptoLib::SHA1DIGESTSIZE);
  cout << "SHA-1 of " << msg << " is " << string((const char*)digestHex,2*CryptoLib::SHA1DIGESTSIZE) << endl;

  // sha-256
  byte digest256[CryptoLib::SHA256DIGESTSIZE];
  byte hex256[2*CryptoLib::SHA256DIGESTSIZE];
  myCryptoLib.sha256(msg, digest256, strlen((const char*)msg));
  myCryptoLib.toHex(digest256, hex256, CryptoLib::SHA256DIGESTSIZE, 2*CryptoLib::SHA256DIGESTSIZE);
  cout << "SHA-256 of " << msg << " is " << string((const char*)hex256,2*CryptoLib::SHA256DIGESTSIZE) << endl;

  // sha-384
  byte digest384[CryptoLib::SHA384DIGESTSIZE];
  byte hex384[2*CryptoLib::SHA384DIGESTSIZE];
  myCryptoLib.sha384(msg, digest384, strlen((const char*)msg));
  myCryptoLib.toHex(digest384, hex384, CryptoLib::SHA384DIGESTSIZE, 2*CryptoLib::SHA384DIGESTSIZE);
  cout << "SHA-384 of " << msg << " is " << string((const char*)hex384,2*CryptoLib::SHA384DIGESTSIZE) << endl;

  // sha-512
  byte digest512[CryptoLib::SHA512DIGESTSIZE];
  byte hex512[2*CryptoLib::SHA512DIGESTSIZE];
  myCryptoLib.sha512(msg, digest512, strlen((const char*)msg));
  myCryptoLib.toHex(digest512, hex512, CryptoLib::SHA512DIGESTSIZE, 2*CryptoLib::SHA512DIGESTSIZE);
  cout << "SHA-512 of " << msg << " is " << string((const char*)hex512,2*CryptoLib::SHA512DIGESTSIZE) << endl;

  // md5
  byte digestmd5[CryptoLib::MD5DIGESTSIZE];
  byte hexmd5[2*CryptoLib::MD5DIGESTSIZE];
  myCryptoLib.md5(msg, digestmd5, strlen((const char*)msg));
  myCryptoLib.toHex(digestmd5, hexmd5, CryptoLib::MD5DIGESTSIZE, 2*CryptoLib::MD5DIGESTSIZE);
  cout << "MD5 of " << msg << " is " << string((const char*)hexmd5,2*CryptoLib::MD5DIGESTSIZE) << endl;

  // esign signature
  byte* signMsg = (byte *)"Message to sign";
  char* keyInput = "esig1536.dat";
  CryptoLib::esignPriv privKey = myCryptoLib.esignPrivKey(keyInput);
  CryptoLib::esignPub pubKey = myCryptoLib.esignPubKey(privKey);
  CryptoLib::SigBuf mySignature = myCryptoLib.esignSig(signMsg, strlen((const char*)signMsg), privKey);
  if (myCryptoLib.esignVer(signMsg, strlen((const char*)signMsg), mySignature, pubKey))
    cout << "ESIGN signature verification SUCCEDED" << endl;
  else
    cout << "ESIGN signature verification FAILED" << endl;

  // RSA signature
  byte* rsaMsg = (byte *)"Message to sign";
  char* rsaInput = "rsa1024.dat";
  CryptoLib::rsaPriv privRSAKey = myCryptoLib.rsaPrivKey(rsaInput);
  CryptoLib::rsaPub pubRSAKey = myCryptoLib.rsaPubKey(privRSAKey);
  CryptoLib::SigBuf myRSASignature = myCryptoLib.rsaSig(rsaMsg, strlen((const char*)rsaMsg), privRSAKey);
  if (myCryptoLib.rsaVer(rsaMsg, strlen((const char*)rsaMsg), myRSASignature, pubRSAKey))
    cout << "RSA signature verification SUCCEDED" << endl;
  else
    cout << "RSA signature verification FAILED" << endl;
  
  // Rijndael encryption/decryption
  byte* plainMsg = (byte *)"My message to encrypt is even longer now";
  // the +1 is because strlen doesn't capture null char
  unsigned int plainLen = strlen((const char*)plainMsg)+1;
  cout << "About to encrypt " << plainMsg << " of size " << plainLen << endl;

  // intializes my key and iv
  byte encKey[CryptoLib::RJ128KEYSIZE];
  byte iv[CryptoLib::RJBLOCKSIZE];
  memset(encKey, 0x01, CryptoLib::RJ128KEYSIZE);
  memset(iv, 0x01, CryptoLib::RJBLOCKSIZE);

  // initialize my cipher buffer
  byte cipherMsg[plainLen];
  memset(cipherMsg, 0x00, plainLen);
  
  // setup my encryptors and decryptors
  CryptoLib::cfbModeEnc myEnc = myCryptoLib.getCFBModeEnc(encKey, CryptoLib::RJ128KEYSIZE, iv);
  CryptoLib::cfbModeDec myDec = myCryptoLib.getCFBModeDec(encKey, CryptoLib::RJ128KEYSIZE, iv);

  // perform encryption
  myCryptoLib.encryptCFB(plainMsg, plainLen, cipherMsg, myEnc);

  // turn my cipher into hex
  // the +1 is capture the end of the cipher buffer
  byte cipherHex[(2*plainLen)+1];
  memset(cipherHex, 0x00, (2*plainLen)+1);
  myCryptoLib.toHex(cipherMsg, cipherHex, plainLen, (2*plainLen)+1);
  cout << "My ciphertext has size " << strlen((const char*)cipherMsg) << endl;
  cout << "Rijndael cipher of " << plainMsg << " is " << cipherHex << endl;

  // recover my data
  byte origMsg[plainLen];
  memset(origMsg, 0x00, plainLen);
  myCryptoLib.decryptCFB(cipherMsg, plainLen, origMsg, myDec);
  cout << "My recovered message is " << origMsg << endl;


  // RC5 encryption/decryption
  byte plainRC5[] = "My message to encrypt is even longer now";
  unsigned int plainRC5len = strlen((const char*)plainRC5)+1;
  
  // initialize my cipher/hex/recover buffers
  byte cipherRC5[plainRC5len];
  byte hexRC5[(2*plainRC5len)+1];
  byte recoverRC5[plainRC5len];
  memset(cipherRC5, 0x00, plainRC5len);
  memset(hexRC5, 0x00, (2*plainRC5len)+1);
  memset(recoverRC5, 0x00, plainRC5len);

  // init key and IV
  byte keyRC5[CryptoLib::RC5KEYSIZE];
  byte ivRC5[CryptoLib::RC5BLOCKSIZE];
  memset(keyRC5, 0x01, CryptoLib::RC5KEYSIZE);
  memset(ivRC5, 0x01, CryptoLib::RC5BLOCKSIZE);
  
  // init encrpytors and decryptors
  CryptoLib::cfbRC5Enc encRC5 = myCryptoLib.getRC5Enc(keyRC5, CryptoLib::RC5KEYSIZE, ivRC5);
  CryptoLib::cfbRC5Dec decRC5 = myCryptoLib.getRC5Dec(keyRC5, CryptoLib::RC5KEYSIZE, ivRC5);

  // encrypt
  myCryptoLib.encryptRC5(plainRC5, plainRC5len, cipherRC5, encRC5);

  // convert to hex
  myCryptoLib.toHex(cipherRC5, hexRC5, plainRC5len, (2*plainRC5len)+1);
  cout << "RC5 cipher of " << plainRC5 << " is " << hexRC5 << endl;

  //decrypt
  myCryptoLib.decryptRC5(cipherRC5, plainRC5len, recoverRC5, decRC5);
  cout << "My recovered message is " << recoverRC5 << endl;
  return 0;
}
