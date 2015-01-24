/******************************************************************************

  SUMMARY: TestErasureCodeShec

   COPYRIGHT(C) 2014 FUJITSU LIMITED.

*******************************************************************************/


#include <errno.h>
#include <pthread.h>

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"

#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/shec/ErasureCodeShec.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

void* thread1(void* pParam);
void* thread2(void* pParam);
void* thread3(void* pParam);
void* thread4(void* pParam);
void* thread5(void* pParam);

static int flag = 0;

TEST(ErasureCodeShec, init_1)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["directory"] = "/usr/lib64/ceph/erasure-code";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
	delete parameters;
}

TEST(ErasureCodeShec, init_2)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-root"] = "test";
	(*parameters)["ruleset-failure-domain"] = "host";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "8";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("test", shec->ruleset_root.c_str());
	EXPECT_STREQ("host", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_3)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "16";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(16u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_4)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "32";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(32u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_5)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	//plugin指定なし
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_6)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "jerasure";	//異常値
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_7)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "abc";	//異常値
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_8)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["directory"] = "/usr/lib64/";	//異常値
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_9)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-root"] = "abc";	//異常値
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_10)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "abc";	//異常値
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_11)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "abc";		//異常値
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init_12)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "-1";	//異常値
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_13)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "abc";
	(*parameters)["k"] = "0.1";	//異常値
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_14)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "a";		//異常値
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_15)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	//k 指定なし
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_16)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "-1";		//異常値
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_17)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "0.1";		//異常値
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_18)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "a";		//異常値
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_19)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	//m　指定なし
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_20)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "-1";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_21)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "0.1";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_22)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "a";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_23)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	//c 指定なし
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_24)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "1";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cに指定した値が代入されていることを確認
	EXPECT_EQ(6u,shec->k);
	EXPECT_EQ(4u,shec->m);
	EXPECT_EQ(3u,shec->c);
	//wにデフォルト値が代入されていることを確認
	EXPECT_EQ(8u,shec->w);
	delete shec;
}

TEST(ErasureCodeShec, init_25)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "-1";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cに指定した値が代入されていることを確認
	EXPECT_EQ(6u,shec->k);
	EXPECT_EQ(4u,shec->m);
	EXPECT_EQ(3u,shec->c);
	//wにデフォルト値が代入されていることを確認
	EXPECT_EQ(8u,shec->w);
	delete shec;
}

TEST(ErasureCodeShec, init_26)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "0.1";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cに指定した値が代入されていることを確認
	EXPECT_EQ(6u,shec->k);
	EXPECT_EQ(4u,shec->m);
	EXPECT_EQ(3u,shec->c);
	//wにデフォルト値が代入されていることを確認
	EXPECT_EQ(8u,shec->w);
	delete shec;
}

TEST(ErasureCodeShec, init_27)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "a";		//異常値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cに指定した値が代入されていることを確認
	EXPECT_EQ(6u,shec->k);
	EXPECT_EQ(4u,shec->m);
	EXPECT_EQ(3u,shec->c);
	//wにデフォルト値が代入されていることを確認
	EXPECT_EQ(8u,shec->w);

	delete shec;
}

TEST(ErasureCodeShec, init_28)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "10";	//mより大きい値
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_29)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	//k　指定なし
	//m　指定なし
	//c　指定なし
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cにデフォルト値が代入されていることを確認
	EXPECT_EQ(2u,shec->k);
	EXPECT_EQ(1u,shec->m);
	EXPECT_EQ(1u,shec->c);

	delete shec;
}

TEST(ErasureCodeShec, init_30)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "12";
	(*parameters)["m"] = "8";
	(*parameters)["c"] = "8";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていることを確認
	EXPECT_EQ(0,r);
	//k,m,cにデフォルト値が代入されていることを確認
	EXPECT_EQ(12u,shec->k);
	EXPECT_EQ(8u,shec->m);
	EXPECT_EQ(8u,shec->c);

	delete shec;
}

TEST(ErasureCodeShec, init_31)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "13";
	(*parameters)["m"] = "7";
	(*parameters)["c"] = "7";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_32)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "7";
	(*parameters)["m"] = "13";
	(*parameters)["c"] = "13";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_33)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "12";
	(*parameters)["m"] = "9";
	(*parameters)["c"] = "8";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init_34)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "8";
	(*parameters)["m"] = "12";
	(*parameters)["c"] = "12";
	//init実行
	int r = shec->init(*parameters);

	//matrixが作られていないことを確認
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, init2_1)	//OSD数の指定ができない
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init2_2)	//OSD数の指定ができない
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

/*
TEST(ErasureCodeShec, init2_3)	//OSD数の指定ができない
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init実行
	int r = shec->init(*parameters);

	//k,m,cにデフォルト値が代入されていることを確認
	EXPECT_EQ(2u,shec->k);
	EXPECT_EQ(1u,shec->m);
	EXPECT_EQ(1u,shec->c);
	EXPECT_EQ(0,r);
	delete shec;
}
*/

TEST(ErasureCodeShec, init2_4)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);
	int r = shec->init(*parameters);	//initを2回起動

	//パラメータ確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, init2_5)
{
	//全て正常値
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	map<std::string, std::string> *parameters2 = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "host";
	(*parameters)["k"] = "10";
	(*parameters)["m"] = "6";
	(*parameters)["c"] = "5";
	(*parameters)["w"] = "16";
	//init実行
	int r = shec->init(*parameters);

	//値を変えてinit再実行
	(*parameters2)["plugin"] = "shec";
	(*parameters2)["technique"] = "";
	(*parameters2)["ruleset-failure-domain"] = "osd";
	(*parameters2)["k"] = "6";
	(*parameters2)["m"] = "4";
	(*parameters2)["c"] = "3";
	shec->init(*parameters2);

	//値が上書きされていることを確認
	EXPECT_EQ(6u, shec->k);
	EXPECT_EQ(4u, shec->m);
	EXPECT_EQ(3u, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_EQ(ErasureCodeShec::MULTIPLE, shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_EQ(0,r);

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数に値を代入
	want_to_decode.insert(0);
	available_chunks.insert(0);
	available_chunks.insert(1);
	available_chunks.insert(2);

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_TRUE(minimum_chunks.size());

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数に値を代入
	for (int i=0;i<10;i++){
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_TRUE(minimum_chunks.size());

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	for (int i=0;i<32;i++){		//k+mより多い要素数
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(-EINVAL,r);
	EXPECT_EQ(0,minimum_chunks.size());
	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_4)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	for (int i=0;i<9;i++){
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}
	want_to_decode.insert(100);		//k+m-1より大きい値
	available_chunks.insert(100);	//k+m-1より大きい値

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_5)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	for (int i=0;i<10;i++){
		want_to_decode.insert(i);
	}
	for (int i=0;i<32;i++){		//k+mより多い要素数
		available_chunks.insert(i);
	}

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_6)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	for (int i=0;i<9;i++){
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}
	available_chunks.insert(100);		//k+m-1より大きい値

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode_7)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(1);
	want_to_decode.insert(3);
	want_to_decode.insert(5);	//available_chunksに含まれない値
	available_chunks.insert(1);
	available_chunks.insert(3);
	available_chunks.insert(6);

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}


TEST(ErasureCodeShec, minimum_to_decode_8)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	//minimum_chunks を NULL で渡す

	//引数の値を代入
	for (int i=0;i<10;i++){
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,NULL);
	EXPECT_EQ(-EINVAL,r);

	delete shec;
}


TEST(ErasureCodeShec, minimum_to_decode_9)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks,minimum;

	//引数の値を代入
	for (int i=0;i<10;i++){
		want_to_decode.insert(i);
		available_chunks.insert(i);
	}
	shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	minimum = minimum_chunks;		//正常値を保存
	for (int i=100;i<120;i++){
		minimum_chunks.insert(i);	//minimum_chunksに余分なデータを入れる
	}

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_EQ(minimum,minimum_chunks);	//正常値と比較

	delete shec;
}

TEST(ErasureCodeShec, minimum_to_decode2_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	available_chunks.insert(0);
	available_chunks.insert(1);
	available_chunks.insert(2);

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_TRUE(minimum_chunks.size());

	delete shec;
}

/*
TEST(ErasureCodeShec, minimum_to_decode2_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init未実行

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	available_chunks.insert(0);
	available_chunks.insert(1);
	available_chunks.insert(2);

	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_NE(0,r);

	delete shec;
}
*/

TEST(ErasureCodeShec, minimum_to_decode2_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decodeの引数宣言
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	want_to_decode.insert(2);
	available_chunks.insert(0);
	available_chunks.insert(1);
	available_chunks.insert(2);
	available_chunks.insert(3);

	//スレッド起動
	pthread_t tid;
	flag = 0;
	pthread_create(&tid,NULL,thread1,shec);
	while(flag == 0){
		usleep(1);
	}
	sleep(1);
	printf("*** test start ***\n");
	//minimum_to_decodeの実行
	int r = shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_EQ(want_to_decode,minimum_chunks);
	printf("*** test end ***\n");
	//スレッドの停止待ち
	flag = 0;
	pthread_join(tid,NULL);

	delete shec;
}


TEST(ErasureCodeShec, minimum_to_decode_with_cost_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decode_with_costの引数宣言
	set<int> want_to_decode;
	map<int,int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	available_chunks[0] = 0;
	available_chunks[1] = 1;
	available_chunks[2] = 2;

	//minimum_to_decode_with_costの実行
	int r = shec->minimum_to_decode_with_cost(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_TRUE(minimum_chunks.size());

	delete shec;
}

/*
TEST(ErasureCodeShec, minimum_to_decode_with_cost_2_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init未実行

	//minimum_to_decode_with_costの引数宣言
	set<int> want_to_decode;
	map<int,int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	available_chunks[0] = 0;
	available_chunks[1] = 1;
	available_chunks[2] = 2;

	minimum_to_decode_with_costの実行
	int r = shec->minimum_to_decode_with_cost(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_NE(0,r);
	delete shec;
}
*/

TEST(ErasureCodeShec, minimum_to_decode_with_cost_2_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//minimum_to_decode_with_costの引数宣言
	set<int> want_to_decode;
	map<int,int> available_chunks;
	set<int> minimum_chunks;

	//引数の値を代入
	want_to_decode.insert(0);
	want_to_decode.insert(2);
	available_chunks[0] = 0;
	available_chunks[1] = 1;
	available_chunks[2] = 2;
	available_chunks[3] = 3;

	//スレッドの起動
	pthread_t tid;
	flag = 0;
	pthread_create(&tid,NULL,thread2,shec);
	while(flag == 0){
		usleep(1);
	}
	sleep(1);
	printf("*** test start ***\n");
	//minimum_to_decode_with_costの実行
	int r = shec->minimum_to_decode_with_cost(want_to_decode,available_chunks,&minimum_chunks);
	EXPECT_EQ(0,r);
	EXPECT_EQ(want_to_decode,minimum_chunks);
	printf("*** test end ***\n");
	//スレッドの停止待ち
	flag = 0;
	pthread_join(tid,NULL);

	delete shec;
}


TEST(ErasureCodeShec, encode_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"															//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;

	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}


	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;
	decoded.clear();
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(32u, decoded[0].length());
/*
	//decodedを画面に表示
//	map<int,bufferlist>::iterator itr;

	for ( itr = decoded.begin();itr != decoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}
*/
	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	r = shec->decode_concat(encoded, &out2);
	std::cout << "r:" << r << std::endl;
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

//	std::cout << "in:" << in << std::endl;			//元データを表示
//	std::cout << "out1:" << out1 << std::endl;		//encode後のデータを表示
//	std::cout << "out2:" << out2 << std::endl;
//	std::cout << "usable:" << usable << std::endl;	//decode後のデータを表示

	delete shec;
}

TEST(ErasureCodeShec, encode_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

/*
	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;
	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}
*/

	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(32u, decoded[0].length());

	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	shec->decode_concat(encoded, &out2);
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

	/*
		std::cout << "in:" << in << std::endl;			//元データを表示
		std::cout << "out1:" << out1 << std::endl;		//encode後のデータを表示
		std::cout << "usable:" << usable << std::endl;	//decode後のデータを表示
	*/

	delete shec;
}

TEST(ErasureCodeShec, encode_3)
{
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	bufferlist in;
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			);
	set<int> want_to_encode;
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);
	want_to_encode.insert(10);
	want_to_encode.insert(11);
	map<int, bufferlist> encoded;
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

/*
	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;

	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}
*/

	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	shec->decode_concat(encoded, &out2);
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

/*
	std::cout << "in:" << in << std::endl;			//元データを表示
	std::cout << "out1:" << out1 << std::endl;		//encode後のデータを表示
	std::cout << "usable:" << usable << std::endl;	//decode後のデータを表示
*/
	delete shec;
}

TEST(ErasureCodeShec, encode_4)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			);
	for(unsigned int i = 0; i < shec->get_chunk_count()-1; i++)
		want_to_encode.insert(i);
	want_to_encode.insert(100);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count()-1, encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

/*
	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;

	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}
*/

	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	shec->decode_concat(encoded, &out2);
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

/*
	std::cout << "in:" << in << std::endl;			//元データを表示
	std::cout << "out1:" << out1 << std::endl;		//encode後のデータを表示
	std::cout << "usable:" << usable << std::endl;	//decode後のデータを表示
*/
	delete shec;
}

/*
TEST(ErasureCodeShec, encode_6)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);
	int r = shec->encode(want_to_encode, NULL, &encoded) //inbuf=NULL
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(0, encoded[0].length());


	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;

	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}

	delete shec;
}
*/

TEST(ErasureCodeShec, encode_8)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, NULL);	//encoded = NULL
	EXPECT_EQ(-EINVAL, r);

	delete shec;
}



TEST(ErasureCodeShec, encode_9)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);
	for (int i = 0;i<100;i++)
	{
		encoded[i].append("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
	}
//	std::cout << "encoded:" << encoded << std::endl;

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(-EINVAL, r);

	delete shec;
}


TEST(ErasureCodeShec, encode2_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"															//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;
	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}

	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(32u, decoded[0].length());

	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	shec->decode_concat(encoded, &out2);
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

	std::cout << "in:" << in << std::endl;			//元データを表示
	std::cout << "out1:" << out1 << std::endl;		//encode後のデータを表示
	std::cout << "usable:" << usable << std::endl;	//decode後のデータを表示

	delete shec;
}

/*
TEST(ErasureCodeShec, encode2_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init未実行

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"															//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(-EINVAL, r);

	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;
	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}

	delete shec;
}
*/

TEST(ErasureCodeShec, encode2_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"															//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//スレッドの起動
	pthread_t tid;
	flag = 0;
	pthread_create(&tid,NULL,thread4,shec);
	while(flag == 0){
		usleep(1);
	}
	sleep(1);
	printf("*** test start ***\n");
	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());
	printf("*** test end ***\n");
	//スレッドの停止待ち
	flag = 0;
	pthread_join(tid,NULL);

	//encodedを画面に表示
	map<int,bufferlist>::iterator itr;
	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
	{
		std::cout << itr->first << ": " << itr->second << std::endl;
	}

	//decode
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;

	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
	EXPECT_EQ(0,r);
	EXPECT_EQ(2u, decoded.size());
	EXPECT_EQ(32u, decoded[0].length());

	bufferlist out1,out2,usable;
	//encode結果をout1にまとめる
	for (unsigned int i = 0; i < encoded.size(); i++)
	  out1.append(encoded[i]);
	//docode結果をout2にまとめる
	shec->decode_concat(encoded, &out2);
	//out2をpadding前のデータ長に合わせる
	usable.substr_of(out2, 0, in.length());
	EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
	EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

	std::cout << "in:" << in << std::endl;
	std::cout << "out1:" << out1 << std::endl;
	std::cout << "usable:" << usable << std::endl;

	delete shec;
}

TEST(ErasureCodeShec, decode_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(2u, decoded.size());

		//結果の確認
		bufferlist out;
		shec->decode_concat(encoded, &out);
		bufferlist usable;
		usable.substr_of(out, 0, in.length());
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}

TEST(ErasureCodeShec, decode_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"	//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeを実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(2u, decoded.size());

		//結果の確認
		bufferlist out;
		shec->decode_concat(encoded, &out);
		bufferlist usable;
		usable.substr_of(out, 0, in.length());
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}

TEST(ErasureCodeShec, decode_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };	//k+mより多い要素数
		map<int, bufferlist> decoded;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+11), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(10u, decoded.size());
		EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

		bufferlist out1,out2,usable;
		//encode結果をout1にまとめる
		for (unsigned int i = 0; i < encoded.size(); i++)
		  out1.append(encoded[i]);
		//docode結果をout2にまとめる
		shec->decode_concat(encoded, &out2);
		//out2をpadding前のデータ長に合わせる
		usable.substr_of(out2, 0, in.length());
		EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}

TEST(ErasureCodeShec, decode_4)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 100 };	//100:k+mより大きい値
		map<int, bufferlist> decoded;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+9), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(10u, decoded.size());
		EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

		bufferlist out1,out2,usable;
		//encode結果をout1にまとめる
		for (unsigned int i = 0; i < encoded.size(); i++)
		  out1.append(encoded[i]);
		//docode結果をout2にまとめる
		shec->decode_concat(encoded, &out2);
		//out2をpadding前のデータ長に合わせる
		usable.substr_of(out2, 0, in.length());
		EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}

/*
TEST(ErasureCodeShec, decode_6)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//decodeの引数宣言
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;

	//decodeの実行
//	map<int, bufferlist> inchunks;
	EXPECT_NE(0,shec->decode(set<int>(want_to_decode, want_to_decode+2), NULL, &decoded));

	delete shec;
}
*/

TEST(ErasureCodeShec, decode_7)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//want_to_decodeと一致しないキーのリストを作成
		bufferlist buf;
		buf.append("abc");
		encoded[100] = buf;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(2u, decoded.size());
		EXPECT_EQ(shec->get_chunk_size(in.length()), decoded[0].length());

		bufferlist out1,out2,usable;
		//encode結果をout1にまとめる
		for (unsigned int i = 0; i < encoded.size(); i++)
		  out1.append(encoded[i]);
		//docode結果をout2にまとめる
		shec->decode_concat(encoded, &out2);
		//out2をpadding前のデータ長に合わせる
		usable.substr_of(out2, 0, in.length());
		EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}


TEST(ErasureCodeShec, decode_8)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		//decodeの実行
		 //decoded = NULL
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, NULL);
		EXPECT_NE(0,r);
	}

	delete shec;
}


TEST(ErasureCodeShec, decode_9)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//decodedに余分なデータを代入
		bufferlist buf;
		buf.append("a");
		for (int i=0;i<100;i++)
		{
			decoded[i] = buf;
		}

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_NE(0,r);
/*
		//decodedを画面に表示
		map<int,bufferlist>::iterator itr;
		for ( itr = decoded.begin();itr != decoded.end(); itr++ )
		{
			std::cout << itr->first << ": " << itr->second << std::endl;
		}
*/
	}

	delete shec;
}

TEST(ErasureCodeShec, decode2_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(2u, decoded.size());

		//結果の確認
		bufferlist out;
		shec->decode_concat(encoded, &out);
		bufferlist usable;
		usable.substr_of(out, 0, in.length());
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
	}

	delete shec;
}

/*
TEST(ErasureCodeShec, decode2_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	// init未実行

	//encodedの作成
	map<int, bufferlist> encoded;
	bufferlist buf;
	buf.append("ABCDEFGH");
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		encoded[i] = buf;

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0 };
		map<int, bufferlist> decoded;

		//decodeの実行
		EXPECT_NE(0,shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded));
	}

	delete shec;
}
*/

TEST(ErasureCodeShec, decode2_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	// all chunks are available
	{
		//decodeの引数宣言
		int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		map<int, bufferlist> decoded;

		//スレッドの起動
		pthread_t tid;
		flag = 0;
		pthread_create(&tid,NULL,thread4,shec);
		while(flag == 0){
			usleep(1);
		}
		sleep(1);
		printf("*** test start ***\n");
		//decodeの実行
		r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		EXPECT_EQ(0,r);
		EXPECT_EQ(2u, decoded.size());
		printf("*** test end ***\n");
		//スレッドの停止待ち
		flag = 0;
		pthread_join(tid,NULL);

		//結果の確認
		bufferlist out;
		shec->decode_concat(encoded, &out);
		bufferlist usable;
		usable.substr_of(out, 0, in.length());
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較

	}

	delete shec;
}

TEST(ErasureCodeShec, decode2_4)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//引数の値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	int r = shec->encode(want_to_encode, in, &encoded);
	EXPECT_EQ(0, r);
	EXPECT_EQ(shec->get_chunk_count(), encoded.size());
	EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

	//decodeの引数宣言
	int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	map<int, bufferlist> decoded;

	// cannot recover
	bufferlist out;
	map<int, bufferlist> degraded;
	degraded[0] = encoded[0];

	//decodeの実行
	r = shec->decode(set<int>(want_to_decode, want_to_decode+2), degraded, &decoded);
	EXPECT_EQ(-1, r);

	delete shec;
}

TEST(ErasureCodeShec, create_ruleset_1_2)
{
	//rulesetの作成
	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//create_rulesetの引数宣言
	stringstream ss;

	//create_rulesetの実行
	int r = shec->create_ruleset("myrule", *crush, &ss);
	EXPECT_EQ(0, r);
	EXPECT_STREQ("myrule",crush->rule_name_map[0].c_str());

	//rule_name_mapを画面に表示
	map<int32_t,string>::iterator itr;
	for ( itr = crush->rule_name_map.begin();itr != crush->rule_name_map.end(); itr++ )
	{
		std::cout <<"+++ rule_name_map[" << itr->first << "]: " << itr->second << " +++\n";
	}

	//同名で再実行
	r = shec->create_ruleset("myrule", *crush, &ss);
	EXPECT_EQ(-EEXIST, r);

	delete shec,crush;
}

/*
TEST(ErasureCodeShec, create_ruleset_3)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//create_rulesetの引数宣言
	stringstream ss;
	CrushWrapper *crush = NULL;
	int r = shec->create_ruleset("myrule", *crush, &ss);
	EXPECT_NE(0, r);	//crush = NULL

	delete shec;
}
*/


TEST(ErasureCodeShec, create_ruleset_4)
{
	//rulesetの作成
	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//create_rulesetの実行
	int r = shec->create_ruleset("myrule", *crush, NULL);	//ss = NULL
	EXPECT_EQ(0, r);

	delete shec,crush;
}


TEST(ErasureCodeShec, create_ruleset2_1)
{
	//rulesetの作成
	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//create_rulesetの引数宣言
	stringstream ss;

	//create_rulesetの実行
	int r = shec->create_ruleset("myrule", *crush, &ss);
	EXPECT_EQ(0, r);
	EXPECT_STREQ("myrule",crush->rule_name_map[0].c_str());

	//rule_name_mapを画面に表示
	map<int32_t,string>::iterator itr;
	for ( itr = crush->rule_name_map.begin();itr != crush->rule_name_map.end(); itr++ )
	{
		std::cout <<"+++ rule_name_map[" << itr->first << "]: " << itr->second << " +++\n";
	}

	delete shec,crush;
}

TEST(ErasureCodeShec, create_ruleset2_2)
{
	//rulesetの作成
	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	// init未実行

	//create_rulesetの引数宣言
	stringstream ss;

	//create_rulesetの実行
	int r = shec->create_ruleset("myrule", *crush, &ss);
	EXPECT_EQ(0, r);

	delete shec,crush;
}

struct Create_ruleset2_3_Param{
	ErasureCodeShec *shec;
	CrushWrapper *crush;
};

TEST(ErasureCodeShec, create_ruleset2_3)
{
	//rulesetの作成
	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//create_rulesetの引数宣言
	stringstream ss;

	//スレッドの起動
	pthread_t tid;
	flag = 0;
	pthread_create(&tid,NULL,thread3,shec);
	while(flag == 0){
		usleep(1);
	}
	sleep(1);
	printf("*** test start ***\n");
	//create_rulesetの実行
	int r = (shec->create_ruleset("myrule", *crush, &ss));
	EXPECT_TRUE(r >= 0);
	printf("*** test end ***\n");
	//スレッドの停止待ち
	flag = 0;
	pthread_join(tid,NULL);

	//rule_name_mapを画面に表示
	map<int32_t,string>::iterator itr;
	for ( itr = crush->rule_name_map.begin();itr != crush->rule_name_map.end(); itr++ )
	{
		std::cout <<"+++ rule_name_map[" << itr->first << "]: " << itr->second << " +++\n";
	}

	delete shec,crush;
}

TEST(ErasureCodeShec, get_chunk_count_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//get_chunk_countの実行
	EXPECT_EQ(10u, shec->get_chunk_count());

	delete shec;
}

TEST(ErasureCodeShec, get_chunk_count_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init未実行

	//get_chunk_countの実行
	EXPECT_NE(10u, shec->get_chunk_count());

	delete shec;
}

TEST(ErasureCodeShec, get_data_chunk_count_1)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	shec->init(*parameters);

	//get_data_chunk_countの実行
	EXPECT_EQ(6u, shec->get_data_chunk_count());

	delete shec;
}

TEST(ErasureCodeShec, get_data_chunk_count_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	//init未実行

	//get_data_chunk_countの実行
	EXPECT_NE(6u, shec->get_data_chunk_count());

	delete shec;
}

TEST(ErasureCodeShec, get_chunk_size_1_2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "8";
	shec->init(*parameters);

	//k*w*4で割り切れる数（192=6*8*4）を渡してget_chunk_sizeを実行
	EXPECT_EQ(32u, shec->get_chunk_size(192));
	//k*w*4で割り切れない数(190=6*8*4-2)を渡してget_chunk_sizeを実行
	EXPECT_EQ(32u, shec->get_chunk_size(190));

	delete shec;
}

/*
TEST(ErasureCodeShec, get_chunk_size2)
{
	//init
	ErasureCodeShecTableCache tcache;
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
	map < std::string, std::string > *parameters = new map<std::string,
			std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = "6";
	(*parameters)["m"] = "4";
	(*parameters)["c"] = "3";
	(*parameters)["w"] = "8";
	//init未実行

	//k*w*4で割り切れる数（192=6*8*4）を渡してget_chunk_sizeを実行
	EXPECT_EQ(32u, shec->get_chunk_size(192));
	//k*w*4で割り切れない数(190=6*8*4-2)を渡してget_chunk_sizeを実行
	EXPECT_EQ(32u, shec->get_chunk_size(190));

	delete shec;
}
*/

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

void* thread1(void* pParam)
{
	ErasureCodeShec* shec = (ErasureCodeShec*)pParam;
	set<int> want_to_decode;
	set<int> available_chunks;
	set<int> minimum_chunks;

	want_to_decode.insert(0);
	want_to_decode.insert(1);
	available_chunks.insert(0);
	available_chunks.insert(1);
	available_chunks.insert(2);

	printf("*** thread loop start ***\n");
	flag = 1;
	while(flag == 1)
	{
		shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks);
	}
	printf("*** thread loop end ***\n");
}

void* thread2(void* pParam)
{
	ErasureCodeShec* shec = (ErasureCodeShec*)pParam;
	set<int> want_to_decode;
	map<int,int> available_chunks;
	set<int> minimum_chunks;

	want_to_decode.insert(0);
	want_to_decode.insert(1);
	available_chunks[0] = 0;
	available_chunks[1] = 1;
	available_chunks[2] = 2;

	printf("*** thread loop start ***\n");
	flag = 1;
	while(flag == 1)
	{
		shec->minimum_to_decode_with_cost(want_to_decode,available_chunks,&minimum_chunks);
		minimum_chunks.clear();
	}
	printf("*** thread loop end ***\n");
}

void* thread3(void* pParam)
{
	ErasureCodeShec* shec = (ErasureCodeShec*)pParam;

	CrushWrapper *crush = new CrushWrapper;
	crush->create();
	crush->set_type_name(2, "root");
	crush->set_type_name(1, "host");
	crush->set_type_name(0, "osd");

	int rootno;
	crush->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1, 5, 0, NULL, NULL, &rootno);
	crush->set_item_name(rootno, "default");

	map<string,string> loc;
	loc["root"] = "default";

	int num_host = 2;
	int num_osd = 5;
	int osd = 0;
	for (int h = 0; h < num_host; ++h) {
		loc["host"] = string("host-") + stringify(h);
		for (int o = 0; o < num_osd; ++o, ++osd) {
			crush->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
		}
	}

	stringstream ss;
	int i = 0;
	char name[30];

	printf("*** thread loop start ***\n");
	flag = 1;
	while(flag == 1)
	{
		sprintf(name,"myrule%d",i);
		shec->create_ruleset(name,*crush,&ss);
		i++;
	}
	printf("*** thread loop end ***\n");
}

void* thread4(void* pParam)
{
	ErasureCodeShec* shec = (ErasureCodeShec*)pParam;

	bufferlist in;
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			);
	set<int> want_to_encode;
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	map<int, bufferlist> encoded;

	printf("*** thread loop start ***\n");
	flag = 1;
	while(flag == 1)
	{
		shec->encode(want_to_encode, in, &encoded);
		encoded.clear();
	}
	printf("*** thread loop end ***\n");
}

void* thread5(void* pParam)
{
	ErasureCodeShec* shec = (ErasureCodeShec*)pParam;

	bufferlist in;
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//248
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//310
			);
	set<int> want_to_encode;
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);
	map<int,bufferlist> encoded;
	shec->encode(want_to_encode, in, &encoded);

	int want_to_decode[] = { 0, 1, 2, 3, 4, 5};
	map<int, bufferlist> decoded;

	printf("*** thread loop start ***\n");
	flag = 1;
	while(flag == 1)
	{
		shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);
		decoded.clear();
	}
	printf("*** thread loop end ***\n");
}
