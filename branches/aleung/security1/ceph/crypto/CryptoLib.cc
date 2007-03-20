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

using namespace CryptoPP;
using namespace std;

/**********
 * Generates a Cipher Feedback Mode
 * encryption mode for use with symmetric
 * block ciphers. This returns a encryptor mode
 * for encrypting using AC5.
 **********/
CryptoLib::cfbRC5Enc CryptoLib::getRC5Enc(byte* key,
					  const unsigned int keyLen,
					  byte* iv) {
  cfbRC5Enc cfbEncryption(key, keyLen, iv);
  return cfbEncryption;
}

/**********
 * Generates a Cipher Feedback Mode
 * decryption mode for use with symmetric
 * block ciphers. This returns a decryptor mode
 * for decrypting using RC5.
 **********/
CryptoLib::cfbRC5Dec CryptoLib::getRC5Dec(byte* key,
					       const unsigned int keyLen,
					       byte* iv) {
  cfbRC5Dec cfbDecryption(key, keyLen, iv);
  return cfbDecryption;
}

/**********
 * Encrypts a block of data using Rijndael.
 * This assumes the out buffer is already
 * allocated to the right size.
 **********/
void CryptoLib::encryptRC5(byte* plain, const unsigned int plainLen,
			   byte* cipher, CryptoLib::cfbRC5Enc cfbEncryption) {
  cfbEncryption.ProcessData(cipher, plain, plainLen);
}

/**********
 * Decrypts a block of data using Rijndael.
 * This assumes the out buffer is already
 * allocated to the right size.
 **********/
void CryptoLib::decryptRC5(byte* cipher, const unsigned int cipherLen,
			   byte* plain, CryptoLib::cfbRC5Dec cfbDecryption) {
  cfbDecryption.ProcessData(plain, cipher, cipherLen);
}

/**********
 * Generates a Cipher Feedback Mode
 * encryption mode for use with symmetric
 * block ciphers. This returns a encryptor mode
 * for encrypting using Rijndael.
 **********/
CryptoLib::cfbModeEnc CryptoLib::getCFBModeEnc(byte* key,
					       const unsigned int keyLen,
					       byte* iv) {
  cfbModeEnc cfbEncryption(key, keyLen, iv);
  return cfbEncryption;
}

/**********
 * Generates a Cipher Feedback Mode
 * decryption mode for use with symmetric
 * block ciphers. This returns a decryptor mode
 * for decrypting using Rijndael.
 **********/
CryptoLib::cfbModeDec CryptoLib::getCFBModeDec(byte* key,
					       const unsigned int keyLen,
					       byte* iv) {
  cfbModeDec cfbDecryption(key, keyLen, iv);
  return cfbDecryption;
}

/**********
 * Encrypts a block of data using Rijndael.
 * This assumes the out buffer is already
 * allocated to the right size.
 **********/
void CryptoLib::encryptCFB(byte* plain, const unsigned int plainLen,
			   byte* cipher, CryptoLib::cfbModeEnc cfbEncryption) {
  cfbEncryption.ProcessData(cipher, plain, plainLen);
}

/**********
 * Decrypts a block of data using Rijndael.
 * This assumes the out buffer is already
 * allocated to the right size.
 **********/
void CryptoLib::decryptCFB(byte* cipher, const unsigned int cipherLen,
			   byte* plain, CryptoLib::cfbModeDec cfbDecryption) {
  cfbDecryption.ProcessData(plain, cipher, cipherLen);
}

/**********
 * Generate an RSA private(Signer) key
 * from a seed input string
 **********/
CryptoLib::rsaPriv CryptoLib::rsaPrivKey(char* seedPath) {
  FileSource ss(seedPath, true, new HexDecoder);
  rsaPriv priv(ss);
  return priv;
}

/**********
 * Generate an RSA public(Verifier) key
 * from a RSA private(Signer) key
 **********/
CryptoLib::rsaPub CryptoLib::rsaPubKey(CryptoLib::rsaPriv seedKey) {
  CryptoLib::rsaPub pub(seedKey);
  return pub;
}

/**********
 * Signs a data buffer using the RSA signature scheme
 * and returns the signature as a SecByteBlock
 **********/
CryptoLib::SigBuf CryptoLib::rsaSig(byte* dataBuf,
				      const unsigned int dataLen,
				      CryptoLib::rsaPriv privKey) {
  // a linear congruential random num gen
  LC_RNG rng(time(NULL));
  SecByteBlock signature(privKey.SignatureLength());
  privKey.SignMessage(rng, dataBuf, dataLen, signature);

  return signature;
}

/**********
 * Verifies a signature using the RSA signature
 * Receives the message, its length, the signature,
 * and the public key
 **********/
bool CryptoLib::rsaVer(byte* dataBuf, const unsigned int dataLen,
			 SigBuf signature, CryptoLib::rsaPub pubKey) {
  if (pubKey.VerifyMessage(dataBuf, dataLen, signature, signature.size())) {
    return true;
  }
  return false;
}

/**********
 * Generate an ESIGN private(Signer) key
 * from a seed input file
 **********/
ESIGN<SHA>::Signer CryptoLib::esignPrivKey(char* seedPath) {
  //StringSource ss(seedData, true,
  //	 new SignerFilter(c,new HexEncoder(new ArraySink(seedData,dataLen))));
  FileSource ss(seedPath, true, new HexDecoder);
  ESIGN<SHA>::Signer priv(ss);
  return priv;
}

/**********
 * Generate an ESIGN public(Verifier) key
 * from a ESIGN private(Signer) key
 **********/
ESIGN<SHA>::Verifier CryptoLib::esignPubKey(ESIGN<SHA>::Signer seedKey) {
  ESIGN<SHA>::Verifier pub(seedKey);
  return pub;
}

/**********
 * Generate an ESIGN private (Signer) key
 * from a input string
 * This is intended be used to recover keys
 * from DEREncoded strings
 **********/
ESIGN<SHA>::Signer CryptoLib::_fromStr_esignPrivKey(string seedString) {
  StringSource s(seedString, true, new HexDecoder());
  ESIGN<SHA>::Signer pubRecover(s);
  return pubRecover;
}

/**********
 * Generate an ESIGN public (Verifier) key
 * from a input string
 * This is intended be used to recover keys
 * from DEREncoded strings
 **********/
ESIGN<SHA>::Verifier CryptoLib::_fromStr_esignPubKey(string seedString) {
  StringSource s(seedString, true, new HexDecoder());
  ESIGN<SHA>::Verifier pubRecover(s);
  return pubRecover;
}

/**********
 * Signs a data buffer using the ESIGN signature scheme
 * and returns the signature as a SecByteBlock
 **********/
CryptoLib::SigBuf CryptoLib::esignSig(byte* dataBuf,
				      const unsigned int dataLen,
				      const CryptoLib::esignPriv& privKey) {
  // a linear congruential random num gen
  LC_RNG rng(time(NULL));
  SecByteBlock signature(privKey.SignatureLength());
  //rnLibg.GenerateBlock(dataBuf, dataLen);
  privKey.SignMessage(rng, dataBuf, dataLen, signature);

  return signature;
}

/**********
 * Verifies a signature using the ESIGN signature
 * Receives the message, its length, the signature,
 * and the public key
 **********/
bool CryptoLib::esignVer(byte* dataBuf, const unsigned int dataLen,
			 SigBuf signature, const CryptoLib::esignPub& pubKey) {
  if (pubKey.VerifyMessage(dataBuf, dataLen, signature, signature.size())) {
    return true;
  }
  return false;
}

/**********
 * Public MD5 function
 * Takes in an allocated buffer,
 * hashes the buffer and stores the data
 * in an empty allocated buffer of size >=
 * DIGESTSIZE, denoted by len
 **********/
void CryptoLib::md5(const byte* dataBuf, byte* hashBuf, unsigned int dataLen) {
  MD5 c;
  hashFunc(dataBuf, hashBuf, dataLen, "MD5", c);
}

/**********
 * Public SHA-1 function
 * Takes in an allocated buffer,
 * hashes the buffer and stores the data
 * in an empty allocated buffer of size >=
 * DIGESTSIZE, denoted by len
 **********/
void CryptoLib::sha1(const byte* dataBuf, byte* hashBuf, unsigned int dataLen) {
  SHA c;
  //byte buffer[2* SHA::DIGESTSIZE];
  //StringSource f("Hash Me", true,
  //       new HashFilter(c,new HexEncoder(new ArraySink(buffer, 2*SHA::DIGESTSIZE))));
  //cout << "The FileSource hash: " << string((const char*)buffer,2*SHA::DIGESTSIZE) << " Size " << SHA::DIGESTSIZE << endl;

  hashFunc(dataBuf, hashBuf, dataLen, "SHA-1", c);
}

/**********
 * Public SHA-256 function
 * Takes in an allocated buffer,
 * hashes the buffer and stores the data
 * in an empty allocated buffer of size >=
 * DIGESTSIZE, denoted by len
 **********/
void CryptoLib::sha256(const byte* dataBuf, byte* hashBuf, unsigned int dataLen) {
  SHA256 c;
  hashFunc(dataBuf, hashBuf, dataLen, "SHA-256", c);
}

/**********
 * Public SHA-384 function
 * Takes in an allocated buffer,
 * hashes the buffer and stores the data
 * in an empty allocated buffer of size >=
 * DIGESTSIZE, denoted by len
 **********/
void CryptoLib::sha384(const byte* dataBuf, byte* hashBuf, unsigned int dataLen) {
  SHA384 c;
  hashFunc(dataBuf, hashBuf, dataLen, "SHA-384", c);
}

/**********
 * Public SHA-512 function
 * Takes in an allocated buffer,
 * hashes the buffer and stores the data
 * in an empty allocated buffer of size >=
 * DIGESTSIZE, denoted by len
 **********/
void CryptoLib::sha512(const byte* dataBuf, byte* hashBuf, unsigned int dataLen) {
  SHA512 c;
  hashFunc(dataBuf, hashBuf, dataLen, "SHA-512", c);
}

/**********
 * Public hex conversion function
 * Takes in a byte string to convert
 * The buffer is required (assumed) to be
 * of an adequate size (len).
 **********/
void CryptoLib::toHex(const byte* dataBuf, byte* hexBuf, unsigned int dataLen, unsigned int hexLen) {
  HexEncoder hexEnc;
  hexEnc.Put(dataBuf, dataLen);
  hexEnc.MessageEnd();
  hexEnc.Get(hexBuf, hexLen);
}

/**********
 * Turns keys (private)
 * into string for buffer encoding
 * User DER encoding
 **********/
string CryptoLib::privToString(CryptoLib::esignPriv privKey) {
  string priv_string;
  HexEncoder privString(new StringSink(priv_string));
  privKey.DEREncode(privString);
  privString.MessageEnd();
  return priv_string;
}

/**********
 * Turns keys (public)
 * into string for buffer encoding
 * User DER encoding
 **********/
string CryptoLib::pubToString(CryptoLib::esignPub pubKey) {
  string pub_string;
  HexEncoder pubString(new StringSink(pub_string));
  pubKey.DEREncode(pubString);
  pubString.MessageEnd();
  return pub_string;
}

//BenchMarkSignature<ESIGN<SHA> >("esig1023.dat", "ESIGN 1023", t);
//BenchMarkKeyless<SHA>("SHA-1", t);

/******
 * Performs a generic hash digest
 * Should not be used externally
 * @param dataBuf the buffer containg the data to hash
 * @param hashBuf the buffer which will contain the hash value
 * @param dataLen the length of the data buffer
 * @param scheme the hash algorithm to use
 * @param ht an uninitialized instance of the hash algorithm
 ******/
void CryptoLib::hashFunc(const byte* dataBuf, byte* hashBuf,
			 unsigned int dataLen,
			 char* scheme, HashTransformation &ht) {
  ht.CalculateDigest(hashBuf, dataBuf, dataLen);
}
