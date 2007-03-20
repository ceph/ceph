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

#ifndef __CRYPTOLIB_H
#define __CRYPTOLIB_H

#include<cryptopp/cryptlib.h>
//one-way functions
#include<cryptopp/sha.h>
#include<cryptopp/md5.h>
#include<cryptopp/iterhash.h>
#include<cryptopp/hex.h>
//PK signatures
#include<cryptopp/rsa.h>
#include<cryptopp/files.h>
#include<cryptopp/rng.h>
#include<cryptopp/randpool.h>
#include<cryptopp/esign.h>
//encryption/decryption
#include<cryptopp/modes.h>
#include<cryptopp/rc5.h>
#include<cryptopp/aes.h>
#include<cryptopp/rijndael.h>

#include<iostream>

using namespace std;

namespace CryptoLib {

  //typedef CryptoLib ThisClass;

  enum {MD5DIGESTSIZE=16, SHA1DIGESTSIZE=20, SHA256DIGESTSIZE=32,
	SHA384DIGESTSIZE=48, SHA512DIGESTSIZE=64};
  enum {RJ128KEYSIZE=16, RJBLOCKSIZE=16, RC5KEYSIZE=16, RC5BLOCKSIZE=8};
  //enum {ESIGNSIGSIZE=192, ESIGNKEYSIZE=404, ESIGNPRIVSIZE=674};
  enum {ESIGNSIGSIZE=128, ESIGNKEYSIZE=274, ESIGNPRIVSIZE=454};

  typedef CryptoPP::SecByteBlock SigBuf;
  typedef CryptoPP::FixedSizeSecBlock<byte,ESIGNSIGSIZE> FixedSigBuf;
  typedef CryptoPP::ESIGN<CryptoPP::SHA>::Signer esignPriv;
  typedef CryptoPP::ESIGN<CryptoPP::SHA>::Verifier esignPub;
  typedef CryptoPP::RSASSA_PKCS1v15_SHA_Signer rsaPriv;
  typedef CryptoPP::RSASSA_PKCS1v15_SHA_Verifier rsaPub;
  typedef CryptoPP::CFB_Mode<CryptoPP::Rijndael>::Encryption cfbModeEnc;
  typedef CryptoPP::CFB_Mode<CryptoPP::Rijndael>::Decryption cfbModeDec;
  typedef CryptoPP::CFB_Mode<CryptoPP::RC5>::Encryption cfbRC5Enc;
  typedef CryptoPP::CFB_Mode<CryptoPP::RC5>::Decryption cfbRC5Dec;

  // symmetric block modes
  cfbModeEnc getCFBModeEnc(byte*, const unsigned int, byte*);
  cfbModeDec getCFBModeDec(byte*, const unsigned int, byte*);
  void encryptCFB(byte*, const unsigned int, byte*, cfbModeEnc);
  void decryptCFB(byte*, const unsigned int, byte*, cfbModeDec);
  cfbRC5Enc getRC5Enc(byte*, const unsigned int, byte*);
  cfbRC5Dec getRC5Dec(byte*, const unsigned int, byte*);
  void encryptRC5(byte*, const unsigned int, byte*, cfbRC5Enc);
  void decryptRC5(byte*, const unsigned int, byte*, cfbRC5Dec);

  // asymmetric key generation
  esignPriv esignPrivKey(char*);
  esignPub esignPubKey(CryptoPP::ESIGN<CryptoPP::SHA>::Signer);
  esignPriv _fromStr_esignPrivKey(string); /* ! */
  esignPub _fromStr_esignPubKey(string); /* ! */
  rsaPriv rsaPrivKey(char*);
  rsaPub rsaPubKey(rsaPriv);
  
  
  // asymmetric signature functions
  SigBuf esignSig(byte*, const unsigned int, const esignPriv&);
  bool esignVer(byte*, const unsigned int, SigBuf, const esignPub&);
  SigBuf rsaSig(byte*, const unsigned int, rsaPriv);
  bool rsaVer(byte*, const unsigned int, SigBuf, rsaPub);

  // one-way hash functions
  void md5(const byte*, byte*, unsigned int);
  void sha1(const byte*, byte*, unsigned int);
  void sha256(const byte*, byte*, unsigned int);
  void sha384(const byte*, byte*, unsigned int);
  void sha512(const byte*, byte*, unsigned int);

  // string encoding
  void toHex(const byte*, byte*, unsigned int, unsigned int);
  string privToString(CryptoLib::esignPriv); /* ! */
  string pubToString(CryptoLib::esignPub); /* ! */
  
  /* Templated one-way hash function*/
  void hashFunc(const byte*, byte*, unsigned int,
		char*, CryptoPP::HashTransformation &);
  /* Templated signature function */
  /* Templated verification function */
  /* Templated encryption function */
  /* Templated decryption function */
  /* Templated asymmetric key generation */
  /* Templated symmetric key generation */
};

#endif
