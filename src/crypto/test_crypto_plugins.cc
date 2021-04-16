/*
	- put this file under ceph/src/crypto/..
	- build:  g++ test_crypto_plugins.cc -o test_crypto_plugins -I.. -I../.. -I../../src -I../../build/include -I../../build/boost/include -std=c++17 -L../../build/lib -lceph_crypto_openssl -lceph_crypto_isal -lceph_crypto_ipsec -lceph-common -lpthread
	- export LD_LIBRARY_PATH=../../build/lib
	- ./test_crypto_plugins 
*/
#include <iostream>
#include <string.h>
#include <sys/time.h>
#include "openssl/openssl_crypto_accel.h"
#include "isa-l/isal_crypto_accel.h"
#include "ipsec_mb/ipsec_crypto_accel.h"
using namespace std;

//use the same memory for all the tests, this is to make every tests fair
size_t textlen = (4*1024);
size_t loop_cnt = (1024*1024*2);

unsigned char* in;
unsigned char* out_encrypted;
unsigned char* out_decrypted;
unsigned char iv[CryptoAccel::AES_256_IVSIZE];
unsigned char key[CryptoAccel::AES_256_KEYSIZE];

void print_result(struct timeval &tv1, struct timeval &tv2,
					size_t textlen, size_t loop_cnt,
					bool encrypt, bool result)
{
	unsigned long du = (1000000 * tv2.tv_sec + tv2.tv_usec)-(1000000 * tv1.tv_sec + tv1.tv_usec);
	printf("%s [%s], len:%ld, loop_cnt:%ld, duration=%ld us, throughput:%ld MB/s\n",
		encrypt ? "Encryption" : "Decryption",
		result ? "success" : "fail",
		textlen,
		loop_cnt,
		du,
		du ? (textlen*loop_cnt/du) : -1
		);
}

#define print_data() printf("Data: in[0] 0x%X, encrypt_out[0] 0x%X, decrypt_out[0] 0x%X\n", in[0], out_encrypted[0], out_decrypted[0])

void test_one_accel(CryptoAccel *accel)
{
	struct timeval tv1, tv2;
	bool b;

	memset(out_encrypted, 0, textlen);
	memset(out_decrypted, 0, textlen);
	
	print_data();
	gettimeofday(&tv1,NULL);
	for(size_t i = 0; i<loop_cnt; i++)
	{
		b = accel->cbc_encrypt(out_encrypted, in, textlen, iv, key);
	}
	gettimeofday(&tv2,NULL);
	print_result(tv1, tv2, textlen, loop_cnt, true, b);
	print_data();

	gettimeofday(&tv1,NULL);
	for(size_t i = 0; i<loop_cnt; i++)
	{
		b = accel->cbc_decrypt(out_decrypted, out_encrypted, textlen, iv, key);
	}
	gettimeofday(&tv2,NULL);
	print_result(tv1, tv2, textlen, loop_cnt, false, b);
	print_data();
}

void test_one_accel_mb(CryptoAccel *accel)
{
	struct timeval tv1, tv2;
	bool b;
	CryptoEngine *engine = accel->get_engine(key);
    	//std::unique_ptr<CryptoEngine> eng(engine);

	memset(out_encrypted, 0, textlen);
	memset(out_decrypted, 0, textlen);
	
	print_data();
	gettimeofday(&tv1,NULL);
	for(size_t i = 0; i<loop_cnt; i++)
	{
		b = accel->cbc_encrypt_mb(engine, out_encrypted, in, textlen, iv);
	}
	accel->flush(engine);
	gettimeofday(&tv2,NULL);
	print_result(tv1, tv2, textlen, loop_cnt, true, b);
	print_data();

	gettimeofday(&tv1,NULL);
	for(size_t i = 0; i<loop_cnt; i++)
	{
		b = accel->cbc_decrypt_mb(engine, out_decrypted, out_encrypted, textlen, iv);
	}
	accel->flush(engine);
	gettimeofday(&tv2,NULL);
	print_result(tv1, tv2, textlen, loop_cnt, false, b);
	print_data();

	accel->put_engine(engine);
}

int main(int argc, char *argv[])
{
	int i;

	if (argc == 2 && !strcmp(argv[1], "-h"))
	{
		printf("cmd: test textlen loop_cnt\n");
		return 0;
	}

	if (argc == 3)
	{
		textlen = atoi(argv[1]);
		loop_cnt = atoi(argv[2]);
	}

	OpenSSLCryptoAccel accel_openssl;
	ISALCryptoAccel accel_isal;
	IPSECCryptoAccel accel_ipsec;

	in = (unsigned char*)malloc(textlen);
	out_encrypted = (unsigned char*)malloc(textlen);
	out_decrypted = (unsigned char*)malloc(textlen);

	// initialize key
	for(i = 0; i<CryptoAccel::AES_256_KEYSIZE; i++) {
		key[i] = i*3;
	}
	// initialize IV
	for(i = 0; i<CryptoAccel::AES_256_IVSIZE; i++) {
		iv[i] = i*5;
	}
	// initialize the input plain text
	for(i = 0; i<textlen; i++) {
		in[i] = i;
	}

	printf("---------- testing crypto plugin - openssl -------------\n");
	test_one_accel(&accel_openssl);

	printf("---------- testing crypto plugin - isal ----------------\n");
	test_one_accel(&accel_isal);

	printf("---------- testing crypto plugin - ipsec ---------------\n");
	test_one_accel(&accel_ipsec);

	printf("---------- testing crypto plugin - openssl mb ----------\n");
	test_one_accel_mb(&accel_openssl);

	printf("---------- testing crypto plugin - isal mb -------------\n");
	test_one_accel_mb(&accel_isal);

	printf("---------- testing crypto plugin - ipsec mb ------------\n");
	test_one_accel_mb(&accel_ipsec);
}
