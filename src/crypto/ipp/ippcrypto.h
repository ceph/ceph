#ifndef IPPCRYPTO_H
#define IPPCRYPTO_H

#include <cstddef>

class IppCrypto
{
  public:
    IppCrypto() {}
    virtual ~IppCrypto() {}

    virtual bool ipp_enc(unsigned char* out, const unsigned char* in, size_t size, 
                    const unsigned char* iv, const unsigned char* key) = 0;
    virtual bool ipp_dec(unsigned char* out, const unsigned char* in, size_t size, 
                    const unsigned char* iv, const unsigned char* key) = 0;
};

class IppCryptoAES: public IppCrypto
{
  public:
    IppCryptoAES() {}
    virtual ~IppCryptoAES() {}

    bool ipp_enc(unsigned char* out, const unsigned char* in, size_t size,
                    const unsigned char* iv, const unsigned char* key);
    bool ipp_dec(unsigned char* out, const unsigned char* in, size_t size,
                    const unsigned char* iv, const unsigned char* key);
  private:
    static const int AES_256_KEYSIZE = 256/8;

};

class IppCryptoSM4: public IppCrypto
{
  public:
    IppCryptoSM4() {}
    virtual ~IppCryptoSM4() {}
    
    bool ipp_enc(unsigned char* out, const unsigned char* in, size_t size,
                    const unsigned char* iv, const unsigned char* key);
    bool ipp_dec(unsigned char* out, const unsigned char* in, size_t size,
                    const unsigned char* iv, const unsigned char* key);
  private:
    static const int SM4_128_KEYSIZE = 128/8;

};



#endif //IPPCRYPTO_H
