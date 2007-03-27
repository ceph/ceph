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
//#include "MerkleTree.h"
#include<iostream>

using namespace std;
using namespace CryptoLib;

int main(int argc, char* argv[]) {
  // message to hash
  const byte* msg = (const byte*)"hash me";

  /*
  // test merkle trees
  MerkleTree mtree;
  uid_t user1 = 1000;
  mtree.add_user(user1);
  cout << "My root hash: " << mtree << endl;
  MerkleTree mtree2 = mtree;
  uid_t user2 = 2000;
  mtree2.add_user(user2);
  cout << "My update root hash " << mtree2 << endl;
  if (mtree2.get_root_hash() > mtree.get_root_hash())
    cout << "mtree2 is bigger" << endl;
  else
    cout << "mtree is bigger" << endl;
  */

  // sha-1
  byte digest[SHA1DIGESTSIZE];
  byte digestHex[2*SHA1DIGESTSIZE];
  sha1(msg,digest,strlen((const char*)msg));
  toHex(digest, digestHex, SHA1DIGESTSIZE, 2*SHA1DIGESTSIZE);
  cout << "SHA-1 of " << msg << " is " << string((const char*)digestHex,2*SHA1DIGESTSIZE) << endl;
 
  // sha-256
  byte digest256[SHA256DIGESTSIZE];
  byte hex256[2*SHA256DIGESTSIZE];
  sha256(msg, digest256, strlen((const char*)msg));
  toHex(digest256, hex256, SHA256DIGESTSIZE, 2*SHA256DIGESTSIZE);
  cout << "SHA-256 of " << msg << " is " << string((const char*)hex256,2*SHA256DIGESTSIZE) << endl;

  // sha-384
  byte digest384[SHA384DIGESTSIZE];
  byte hex384[2*SHA384DIGESTSIZE];
  sha384(msg, digest384, strlen((const char*)msg));
  toHex(digest384, hex384, SHA384DIGESTSIZE, 2*SHA384DIGESTSIZE);
  cout << "SHA-384 of " << msg << " is " << string((const char*)hex384,2*SHA384DIGESTSIZE) << endl;

  // sha-512
  byte digest512[SHA512DIGESTSIZE];
  byte hex512[2*SHA512DIGESTSIZE];
  sha512(msg, digest512, strlen((const char*)msg));
  toHex(digest512, hex512, SHA512DIGESTSIZE, 2*SHA512DIGESTSIZE);
  cout << "SHA-512 of " << msg << " is " << string((const char*)hex512,2*SHA512DIGESTSIZE) << endl;

  // md5
  byte digestmd5[MD5DIGESTSIZE];
  byte hexmd5[2*MD5DIGESTSIZE];
  md5(msg, digestmd5, strlen((const char*)msg));
  toHex(digestmd5, hexmd5, MD5DIGESTSIZE, 2*MD5DIGESTSIZE);
  cout << "MD5 of " << msg << " is " << string((const char*)hexmd5,2*MD5DIGESTSIZE) << endl;

  
  // esign signature
  byte* signMsg = (byte *)"Message to sign is getting bigger by the minutefdsfdfdsffdfsfsdfdsfdfdsfsdfdsfsdfdsfsdsfdssdfsdfdsfdsffds";
  //char* keyInput = "esig1536.dat";
  char* keyInput = "esig1023.dat";
  esignPriv privKey = esignPrivKey(keyInput);
  esignPub pubKey = esignPubKey(privKey);
  SigBuf mySignature = esignSig(signMsg, strlen((const char*)signMsg), privKey);

  // testing --> remove me!
  byte testBuf[mySignature.size()];
  memcpy((void*)testBuf,(void*)mySignature, mySignature.size());
  cout << "ESIGN signature size " << mySignature.size() << endl;
  //SigBuf testSecBuf = new SigBuf(testBuf, mySignature.size());
  SigBuf testSecBuf(testBuf, mySignature.size());
  FixedSigBuf testFixedBuf;
  testFixedBuf.Assign(testSecBuf, testSecBuf.size());
  FixedSigBuf ftBuf;
  memcpy((void*)ftBuf, (void*)testFixedBuf, testFixedBuf.size());
  SigBuf copyTest;
  copyTest.Assign(ftBuf, ftBuf.size());
  if (esignVer(signMsg, strlen((const char*)signMsg), copyTest, pubKey))
    cout << "COPYTEST! signature verification SUCCEDED" << endl;
  else
    cout << "COPYTEST! signature verification FAILED" << endl;
  
  //memcpy((void*)testSecBuf, (void*)testBuf, mySignature.size());
  cout << "sizeof(testBuf)=" << sizeof(testBuf) << endl;
  cout << "sizeof(testSecBuf)=" << sizeof(testSecBuf) << " and .size()=" << testSecBuf.size() << endl;
  cout << "sizeof(testFixedBuf)=" << sizeof(testFixedBuf) << " and .size()=" << testFixedBuf.size() << endl;
  SigBuf finalSig;
  finalSig.Assign(testFixedBuf, testFixedBuf.size());
  
  if (esignVer(signMsg, strlen((const char*)signMsg), finalSig, pubKey))
    cout << "TESTCOPY signature verification SUCCEDED" << endl;
  else
    cout << "TESTCOPY signature verification FAILED" << endl;
  
  if (esignVer(signMsg, strlen((const char*)signMsg), mySignature, pubKey))
    cout << "ESIGN signature verification SUCCEDED" << endl;
  else
    cout << "ESIGN signature verification FAILED" << endl;

  if (esignVer(signMsg, strlen((const char*)signMsg), mySignature, pubKey))
    cout << "RE-ESIGN signature verification SUCCEDED" << endl;
  else
    cout << "RE-ESIGN signature verification FAILED" << endl;

  cout << "Trying to convert ESIGN pub key to byte array" << endl;
  string tempString = pubToString(pubKey);
  string tempPrivString = privToString(privKey);
  char charKey[tempString.size()];
  memcpy(charKey, tempString.c_str(), sizeof(charKey));
  byte hexKey[sizeof(charKey)];
  memset(hexKey, 0x00, sizeof(hexKey));
  toHex((byte*)charKey, hexKey, sizeof(charKey), sizeof(hexKey));
  cout << "ESIGN public key size is: " << tempString.size() << " " << sizeof(hexKey) << endl;
  cout << "ESIGN private key size is: " <<  tempPrivString.size() << endl;
  cout << "Hex array  of ESIGN public key: " << string((const char*)hexKey, sizeof(hexKey)) << endl;
  string convString(charKey, sizeof(charKey));
  esignPub testKey = _fromStr_esignPubKey(convString);
  if (esignVer(signMsg, strlen((const char*)signMsg), mySignature, testKey))
    cout << "ARRAY copy KEY verified" << endl;
  else
    cout << "Array copy Key failed" << endl;

  /*

  // RSA signature
  byte* rsaMsg = (byte *)"Message to sign";
  char* rsaInput = "rsa1024.dat";
  rsaPriv privRSAKey = rsaPrivKey(rsaInput);
  rsaPub pubRSAKey = rsaPubKey(privRSAKey);
  SigBuf myRSASignature = rsaSig(rsaMsg, strlen((const char*)rsaMsg), privRSAKey);
  if (rsaVer(rsaMsg, strlen((const char*)rsaMsg), myRSASignature, pubRSAKey))
    cout << "RSA signature verification SUCCEDED" << endl;
  else
    cout << "RSA signature verification FAILED" << endl;

  // Rijndael encryption/decryption
  byte* plainMsg = (byte *)"My message to encrypt is even longer now";
  // the +1 is because strlen doesn't capture null char
  unsigned int plainLen = strlen((const char*)plainMsg)+1;
  cout << "About to encrypt " << plainMsg << " of size " << plainLen << endl;

  // intializes my key and iv
  byte encKey[RJ128KEYSIZE];
  byte iv[RJBLOCKSIZE];
  memset(encKey, 0x01, RJ128KEYSIZE);
  memset(iv, 0x01, RJBLOCKSIZE);
  
  // initialize my cipher buffer
  byte cipherMsg[plainLen];
  memset(cipherMsg, 0x00, plainLen);
  
  // setup my encryptors and decryptors
  cfbModeEnc myEnc = getCFBModeEnc(encKey, RJ128KEYSIZE, iv);
  cfbModeDec myDec = getCFBModeDec(encKey, RJ128KEYSIZE, iv);

  // perform encryption
  encryptCFB(plainMsg, plainLen, cipherMsg, myEnc);

  // turn my cipher into hex
  // the +1 is capture the end of the cipher buffer
  byte cipherHex[(2*plainLen)+1];
  memset(cipherHex, 0x00, (2*plainLen)+1);
  toHex(cipherMsg, cipherHex, plainLen, (2*plainLen)+1);
  cout << "My ciphertext has size " << strlen((const char*)cipherMsg) << endl;
  cout << "Rijndael cipher of " << plainMsg << " is " << cipherHex << endl;

  // recover my data
  byte origMsg[plainLen];
  memset(origMsg, 0x00, plainLen);
  decryptCFB(cipherMsg, plainLen, origMsg, myDec);
  cout << "My recovered message is " << origMsg << endl;

  // RC5 encryption/decryption
  byte plainRC5[] = "My RC5 message to encrypt is even longer now";
  unsigned int plainRC5len = strlen((const char*)plainRC5)+1;
  
  // initialize my cipher/hex/recover buffers
  byte cipherRC5[plainRC5len];
  byte hexRC5[(2*plainRC5len)+1];
  byte recoverRC5[plainRC5len];
  memset(cipherRC5, 0x00, plainRC5len);
  memset(hexRC5, 0x00, (2*plainRC5len)+1);
  memset(recoverRC5, 0x00, plainRC5len);

  // init key and IV
  byte keyRC5[RC5KEYSIZE];
  byte ivRC5[RC5BLOCKSIZE];
  memset(keyRC5, 0x01, RC5KEYSIZE);
  memset(ivRC5, 0x01, RC5BLOCKSIZE);
  
  // init encrpytors and decryptors
  cfbRC5Enc encRC5 = getRC5Enc(keyRC5, RC5KEYSIZE, ivRC5);
  cfbRC5Dec decRC5 = getRC5Dec(keyRC5, RC5KEYSIZE, ivRC5);

  // encrypt
  encryptRC5(plainRC5, plainRC5len, cipherRC5, encRC5);

  // convert to hex
  toHex(cipherRC5, hexRC5, plainRC5len, (2*plainRC5len)+1);
  cout << "RC5 cipher of " << plainRC5 << " is " << hexRC5 << endl;

  //decrypt
  decryptRC5(cipherRC5, plainRC5len, recoverRC5, decRC5);
  cout << "My recovered message is " << recoverRC5 << endl;

  */

  return 0;
}
