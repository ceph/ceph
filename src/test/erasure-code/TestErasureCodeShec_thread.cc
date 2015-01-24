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
#include "erasure-code/shec/ErasureCodeShecTableCache.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

//extern "C" {
//#include "jerasure/include/galois.h"
//
//extern gf_t *gfp_array[];
//extern int  gfp_is_composite[];
//}

void* thread1(void* pParam);


class TestParam{
public:
  string k,m,c,w;
};

TEST(ErasureCodeShec, thread)
{
  __erasure_code_init("shec", "");

  TestParam param1,param2,param3,param4,param5;
  param1.k = "6";
  param1.m = "4";
  param1.c = "3";
  param1.w = "8";

  param2.k = "4";
  param2.m = "3";
  param2.c = "2";
  param2.w = "16";

  param3.k = "10";
  param3.m = "8";
  param3.c = "4";
  param3.w = "32";

  param4.k = "5";
  param4.m = "5";
  param4.c = "5";
  param4.w = "8";

  param5.k = "9";
  param5.m = "9";
  param5.c = "6";
  param5.w = "16";

  //スレッドの起動
  pthread_t tid1,tid2,tid3,tid4,tid5;
  pthread_create(&tid1,NULL,thread1,(void*)&param1);
  std::cout << "thread1 start " << std::endl;
  //	pthread_create(&tid2,NULL,thread1,(void*)&param2);
  //	std::cout << "thread2 start " << std::endl;
  //	pthread_create(&tid3,NULL,thread1,(void*)&param3);
  //	std::cout << "thread3 start " << std::endl;
  //	pthread_create(&tid4,NULL,thread1,(void*)&param4);
  //	std::cout << "thread4 start " << std::endl;
  //	pthread_create(&tid5,NULL,thread1,(void*)&param5);
  //	std::cout << "thread5 start " << std::endl;
  //スレッドの停止待ち
  pthread_join(tid1,NULL);
  //	pthread_join(tid2,NULL);
  //	pthread_join(tid3,NULL);
  //	pthread_join(tid4,NULL);
  //	pthread_join(tid5,NULL);
}


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
  TestParam* param = (TestParam*)pParam;

  time_t start,end;
  int r;

  //encode
  bufferlist in;
  set<int> want_to_encode;
  map<int, bufferlist> encoded;

  in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"  //length = 62
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"  //124
	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"  //186
	    "012345"                                                          //192
	    );

  //decode
  int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
  map<int, bufferlist> decoded;
  bufferlist out1,out2,usable;

  time(&start);
  time(&end);
  int test_sec = 60;
  ErasureCodeShecTableCache tcache;

  while(test_sec >= (end - start))
    {
      //init
      ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(tcache,ErasureCodeShec::MULTIPLE);
      map<std::string, std::string> *parameters = new map<std::string, std::string>();
      (*parameters)["plugin"] = "shec";
      (*parameters)["technique"] = "multiple";
      (*parameters)["ruleset-failure-domain"] = "osd";
      (*parameters)["k"] = param->k;
      (*parameters)["m"] = param->m;
      (*parameters)["c"] = param->c;
      (*parameters)["w"] = param->w;
      shec->init(*parameters);

      int i_k = std::atoi(param->k.c_str());
      int i_m = std::atoi(param->m.c_str());
      int i_c = std::atoi(param->c.c_str());
      int i_w = std::atoi(param->w.c_str());

      EXPECT_EQ(i_k, shec->k);
      EXPECT_EQ(i_m, shec->m);
      EXPECT_EQ(i_c, shec->c);
      EXPECT_EQ(i_w, shec->w);
      //		EXPECT_STREQ("multiple", shec->technique);
      EXPECT_STREQ("default", shec->ruleset_root.c_str());
      EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
      EXPECT_TRUE(shec->matrix != NULL);
      if ((shec->matrix == NULL)){
	std::cout << "matrix is null" << std::endl;
	// error
	break;
      }

      //encode
      for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
	want_to_encode.insert(i);
      r = shec->encode(want_to_encode, in, &encoded);

      EXPECT_EQ(0, r);
      EXPECT_EQ(shec->get_chunk_count(), encoded.size());
      EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

      if (r != 0){
	std::cout << "error in encode" << std::endl;
	//error
	break;
      }

      //encoded
      //      map<int,bufferlist>::iterator itr;
      //      for ( itr = encoded.begin();itr != encoded.end(); itr++ )
      //	{
      //	  std::cout << itr->first << ": " << itr->second << std::endl;
      //	}

      //decode
      r = shec->decode(set<int>(want_to_decode, want_to_decode+2), encoded, &decoded);

      EXPECT_EQ(0,r);
      EXPECT_EQ(2u, decoded.size());
      //		EXPECT_EQ(32u, decoded[0].length());

      if (r != 0){
	std::cout << "error in decode" << std::endl;
	//error
	break;
      }

      //encode結果をout1にまとめる
      for (unsigned int i = 0; i < encoded.size(); i++)
	out1.append(encoded[i]);
      //docode結果をout2にまとめる
      shec->decode_concat(encoded, &out2);
      //out2をpadding前のデータ長に合わせる
      usable.substr_of(out2, 0, in.length());
      EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
      EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較
      if(out1 == in || !(usable == in)){
	std::cout << "encode(decode) result is not correct" << std::endl;
	break;
      }
      //		std::cout << "in:" << in << std::endl;
      //		std::cout << "out1:" << out1 << std::endl;
      //		std::cout << "usable:" << usable << std::endl;

      delete shec;
      delete parameters;
      want_to_encode.clear();
      encoded.clear();
      decoded.clear();
      out1.clear();
      out2.clear();
      usable.clear();

      time(&end);
    }
}
