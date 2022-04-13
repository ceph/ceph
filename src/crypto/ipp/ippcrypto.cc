/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Intel Corporation
 *
 * Author: Hui Han <hui.han@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "ippcrypto.h"
#include "ippcp.h"

bool IppCryptoAES::ipp_enc(unsigned char* out, const unsigned char* in, size_t size,
		const unsigned char* iv, const unsigned char* key)
{
    /* Size of AES context structure. It will be set up in ippsAESGetSize(). */
    int ctxSize = 0;
    /* Internal function status */
    IppStatus status = ippStsNoErr;
    /* Pointer to AES context structure */
    IppsAESSpec* pAES = 0;

    /* Size of AES context structure. */
    ippsAESGetSize(&ctxSize);

    /* Allocate memory for AES context structure */
    pAES = (IppsAESSpec*)(new Ipp8u[ctxSize]);
    if (NULL == pAES) {
        //derr << "Cannot allocate memory for AES context" << endl;
        return -1;
    }

    ippsAESInit(key, AES_256_KEYSIZE, pAES, ctxSize);
 
    /* Encryption */
    status = ippsAESEncryptCBC(in, out, size, pAES, iv);
    if(ippStsNoErr != status){
        //derr << "ippsAESEncryptCBC failed" << endl;
	if (pAES) delete [] (Ipp8u*)pAES;
        return false;
    }

    if (pAES) delete [] (Ipp8u*)pAES;

    return true;
}

bool IppCryptoAES::ipp_dec(unsigned char* out, const unsigned char* in, size_t size, 
                                    const unsigned char* iv, const unsigned char* key)
{
    /* Size of AES context structure. It will be set up in ippsAESGetSize(). */
    int ctxSize = 0;
    /* Internal function status */
    IppStatus status = ippStsNoErr;
    /* Pointer to AES context structure */
    IppsAESSpec* pAES = 0;

    /* Size of AES context structure. */
    ippsAESGetSize(&ctxSize);

    /* Allocate memory for AES context structure */
    pAES = (IppsAESSpec*)(new Ipp8u[ctxSize]);
    if (NULL == pAES) {
        return -1;
    }
    
    ippsAESInit(key, AES_256_KEYSIZE, pAES, ctxSize);
 
    /* Encryption */
    status = ippsAESDecryptCBC(in, out, size, pAES, iv);
    if(ippStsNoErr != status){
	if (pAES) delete [] (Ipp8u*)pAES;
        return false;
    }

    if (pAES) delete [] (Ipp8u*)pAES;

    return true;
}

bool IppCryptoSM4::ipp_enc(unsigned char* out, const unsigned char* in, size_t size, 
                                    const unsigned char* iv, const unsigned char* key)
{
    /* Size of AES context structure. It will be set up in ippsAESGetSize(). */
    int ctxSize = 0;
    /* Internal function status */
    IppStatus status = ippStsNoErr;
    /* Pointer to AES context structure */
    IppsSMS4Spec* pSMS4 = 0;

    /* Size of AES context structure. */
    ippsSMS4GetSize(&ctxSize);

    /* Allocate memory for AES context structure */
    pSMS4 = (IppsSMS4Spec*)(new Ipp8u[ctxSize]);
    if (NULL == pSMS4) {
        return -1;
    }

    ippsSMS4Init(key, SM4_128_KEYSIZE, pSMS4, ctxSize);
 
    /* Encryption */
    status = ippsSMS4EncryptCBC(in, out, size, pSMS4, iv);
    if(ippStsNoErr != status){
        if( pSMS4 ) delete [] (Ipp8u*)pSMS4;
        return false;
    }

    if (pSMS4) delete [] (Ipp8u*)pSMS4;

    return true;
}

bool IppCryptoSM4::ipp_dec(unsigned char* out, const unsigned char* in, size_t size, 
                                    const unsigned char* iv, const unsigned char* key)
{
    /* Size of AES context structure. It will be set up in ippsAESGetSize(). */
    int ctxSize = 0;
    /* Internal function status */
    IppStatus status = ippStsNoErr;
    /* Pointer to AES context structure */
    IppsSMS4Spec* pSMS4 = 0;

    /* Size of AES context structure. */
    ippsSMS4GetSize(&ctxSize);

    /* Allocate memory for AES context structure */
    pSMS4 = (IppsSMS4Spec*)(new Ipp8u[ctxSize]);
    if (NULL == pSMS4) {
        return -1;
    }
    
    ippsSMS4Init(key, SM4_128_KEYSIZE, pSMS4, ctxSize);
 
    /* Encryption */
    status = ippsSMS4DecryptCBC(in, out, size, pSMS4, iv);
    if(ippStsNoErr != status){
        if( pSMS4 ) delete [] (Ipp8u*)pSMS4;
        return false;
    }

    if (pSMS4) delete [] (Ipp8u*)pSMS4;

    return true;
}
