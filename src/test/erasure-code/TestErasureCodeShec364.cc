/******************************************************************************

  SUMMARY: TestErasureCodeShec 364 pattern

   COPYRIGHT(C) 2014 FUJITSU LIMITED.

*******************************************************************************/


#include <errno.h>
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"

#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/shec/ErasureCodeShec.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "test/erasure-code/csv.h"

#include "test/erasure-code/combination.hpp"

struct _param {
	char* k;
	char* m;
	char* c;
	char* ch_size;
} ;
struct _param param[301];

class ParameterTest : public ::testing::TestWithParam<struct _param> {

};



TEST_P(ParameterTest, parameter364)
{
	//パラメータを受け取る
	char* k = GetParam().k;
	char* m = GetParam().m;
	char* c = GetParam().c;
	int c_size = atoi(GetParam().ch_size);
	int i_k = atoi(k);
	int i_m = atoi(m);
	int i_c = atoi(c);

	//initの準備
	ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde("");
	map<std::string, std::string> *parameters = new map<std::string, std::string>();
	(*parameters)["plugin"] = "shec";
	(*parameters)["technique"] = "";
	(*parameters)["directory"] = "/usr/lib64/ceph/erasure-code";
	(*parameters)["ruleset-failure-domain"] = "osd";
	(*parameters)["k"] = k;
	(*parameters)["m"] = m;
	(*parameters)["c"] = c;
	//initの実行
	shec->init(*parameters);

	//パラメータ(k,m,l)を表示
//	cout<< "k = " << shec->k << ", m = " << shec->m << ", c = " << shec->c << "\n";

	//initの設定内容を確認
	EXPECT_EQ(i_k, shec->k);
	EXPECT_EQ(i_m, shec->m);
	EXPECT_EQ(i_c, shec->c);
	EXPECT_EQ(8u, shec->w);
	EXPECT_STREQ("", shec->technique);
	EXPECT_STREQ("default", shec->ruleset_root.c_str());
	EXPECT_STREQ("osd", shec->ruleset_failure_domain.c_str());
	EXPECT_TRUE(shec->matrix != NULL);

	//k+m個の中から1～c個を選ぶ組合せ
	//minimum_to_decodeの引数宣言
	set<int> want_to_decode, available_chunks, minimum_chunks;
	std::vector<int> w_to_d;
	for (int w = 1; w <= i_c; w++) {
		const int r = w;		// k+m個の中からr個を選ぶ組合せ

		//組合せを作るための配列を作成
		for (int i = 0; i < shec->get_chunk_count(); ++i) {
			w_to_d.push_back(i);
		}
		do {
			// 全ての組み合わせを出力
//			std::cout << "[ " << w_to_d[0];
//			for (unsigned int i = 1; i < r; ++i) {
//				std::cout << ", " << w_to_d[i];
//			}
//			std::cout << " ]" << std::endl;
			//minimum_to_decodeの引数に値を代入
			for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
				available_chunks.insert(i);
			for (unsigned int i=0; i<r; i++){
//				cout << "w_to_d[i]:" << w_to_d.at(i) << "\n";
				want_to_decode.insert(w_to_d.at(i));
				//available_chunksからwant_to_decodeを取り除く
				available_chunks.erase(w_to_d.at(i));
			}
			//引数の表示
			std::cout<< "k = " << shec->k << ", m = " << shec->m << ", c = " << shec->c << std::endl;
			std::cout << "want_to_decode:" << want_to_decode << std::endl;
			std::cout << "available_chunks:" << available_chunks << std::endl;
			//minimum_to_decodeの実行
			EXPECT_EQ(0,shec->minimum_to_decode(want_to_decode,available_chunks,&minimum_chunks));
			EXPECT_TRUE(minimum_chunks.size());
			want_to_decode.clear();
			available_chunks.clear();
			minimum_chunks.clear();
		} while (btb::next_combination(w_to_d.begin(), w_to_d.begin() + r, w_to_d.end())); //name spaceはbtb
		w_to_d.clear();
	}






	//minimum_to_decode_with_costの引数宣言
	set<int> want_to_decode_with_cost, minimum_chunks_with_cost;
	map<int,int> available_chunks_with_cost;

	//minimum_to_decode_with_costの引数に値を代入
	for(unsigned int i = 0; i < 1; i++)
		want_to_decode_with_cost.insert(i);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		available_chunks_with_cost[i] = i;

	//minimum_to_decode_with_costの実行
	EXPECT_EQ(0,shec->minimum_to_decode_with_cost(want_to_decode_with_cost,available_chunks_with_cost,&minimum_chunks_with_cost));
	EXPECT_TRUE(minimum_chunks_with_cost.size());

	//encodeの引数宣言
	bufferlist in;
	set<int> want_to_encode;
	map<int, bufferlist> encoded;

	//encodeの引数に値を代入
	in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//length = 62
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//124
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"	//186
			"012345"															//192
			);
	for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
		want_to_encode.insert(i);

	//encodeの実行
	EXPECT_EQ(0, shec->encode(want_to_encode, in, &encoded));
	EXPECT_EQ(i_k+i_m, encoded.size());
	EXPECT_EQ(c_size, encoded[0].length());

	//encodedを画面に表示
//	map<int,bufferlist>::iterator itr;
//	for ( itr = encoded.begin();itr != encoded.end(); itr++ )
//	{
//		cout << itr->first << ": " << itr->second << "\n";
//	}

	//decodeの引数宣言
		int want_to_decode2[i_k+i_m];
		map<int, bufferlist> decoded;

		//decodeの引数に値を代入
		for(unsigned int i = 0; i < shec->get_chunk_count(); i++)
			want_to_decode2[i] = i;

		//decodeの実行
		EXPECT_EQ(0,shec->decode(set<int>(want_to_decode2, want_to_decode2+2), encoded, &decoded));
		EXPECT_EQ(2u, decoded.size());
		EXPECT_EQ(c_size, decoded[0].length());

		//encode,decodeの結果確認に使用する変数宣言
		bufferlist out1,out2;

		//encodeの結果をout1にまとめる
		for (unsigned int i = 0; i < encoded.size(); i++)
		  out1.append(encoded[i]);

		//inとout1を表示
	//	cout << "in: " << in << "\n";
	//	cout << "out1: " << out1 << "\n";

		//decode結果をout2にまとめる
		shec->decode_concat(encoded, &out2);

		//データ長をpudding前に戻す
		bufferlist usable;

		usable.substr_of(out2, 0, in.length());

		EXPECT_FALSE(out1 == in); //元データとencode後のデータ比較
		EXPECT_TRUE(usable == in); //元データとdecode後のデータ比較



	//create_rulesetの準備
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

	//create_rulesetの引数宣言
	stringstream ss;

	//create_rulesetの実行
	EXPECT_EQ(0, shec->create_ruleset("myrule", *crush, &ss));
	EXPECT_STREQ("myrule",crush->rule_name_map[0].c_str());

	//rulesetの一覧を画面に表示
//		map<int32_t,string>::iterator itr2;
//		for ( itr2 = c->rule_name_map.begin();itr2 != c->rule_name_map.end(); itr2++ )
//		{
//			cout <<"+++ rule_name_map[" << itr2->first << "]: " << itr2->second << " +++\n";
//		}

	//get_chunk_countの実行
	EXPECT_EQ(i_k+i_m, shec->get_chunk_count());

	//get_data_chunk_countの実行
	EXPECT_EQ(i_k, shec->get_data_chunk_count());

	//get_chunk_sizeの実行
	EXPECT_EQ(c_size, shec->get_chunk_size(192));

	delete shec;
}



INSTANTIATE_TEST_CASE_P(Test,ParameterTest,::testing::ValuesIn(param));


int main(int argc, char **argv) {
	int r;

	// 標準入出力をファイルに変更
	FILE* fp_in = freopen("shec_kmc_parameter.csv", "r", stdin);
	if (fp_in != NULL) {
		CSV::Data d;
		cin >> d;
		for (int i = 0; i < d.row_count(); i++) {
			param[i].k = (char*) (d.get(i).get(0).line[0].c_str());
			param[i].m = (char*) (d.get(i).get(1).line[0].c_str());
			param[i].c = (char*) (d.get(i).get(2).line[0].c_str());
			param[i].ch_size = (char*) (d.get(i).get(3).line[0].c_str());	// get_chunk_sizeの結果
//			cout << param[i].k << " " << param[i].m << " " << param[i].c << "\n";	// k,m,cを標準出力に表示
		}

		vector<const char*> args;
		argv_to_vec(argc, (const char **) argv, args);

		global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
		common_init_finish(g_ceph_context);

		::testing::InitGoogleTest(&argc, argv);
		r = RUN_ALL_TESTS(); //テスト実行

		// ファイルを閉じる
		fclose(fp_in);
	} else {
		cout << "fp_in == NULL\n";
		r = -1;
	}
	return r;
}
