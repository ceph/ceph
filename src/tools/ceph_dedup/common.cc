#include "common.h"

string get_opts_pool_name(const po::variables_map &opts) {
  if (opts.count("pool")) {
    return opts["pool"].as<string>();
  }
  cerr << "must specify pool name" << std::endl;
  exit(1);
}

string get_opts_chunk_algo(const po::variables_map &opts) {
  if (opts.count("chunk-algorithm")) {
    string chunk_algo = opts["chunk-algorithm"].as<string>();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
    return chunk_algo;
  }
  cerr << "must specify chunk-algorithm" << std::endl;
  exit(1);
}

string get_opts_fp_algo(const po::variables_map &opts) {
  if (opts.count("fingerprint-algorithm")) {
    string fp_algo = opts["fingerprint-algorithm"].as<string>();
    if (fp_algo != "sha1"
	&& fp_algo != "sha256" && fp_algo != "sha512") {
      cerr << "unrecognized fingerprint-algorithm " << fp_algo << std::endl;
      exit(1);
    }
    return fp_algo;
  }
  cout << "SHA1 is set as fingerprint algorithm by default" << std::endl;
  return string("sha1");
}

string get_opts_op_name(const po::variables_map &opts) {
  if (opts.count("op")) {
    return opts["op"].as<string>();
  } else {
    cerr << "must specify op" << std::endl;
    exit(1);
  }
}

string get_opts_chunk_pool(const po::variables_map &opts) {
  if (opts.count("chunk-pool")) {
    return opts["chunk-pool"].as<string>();
  } else {
    cerr << "must specify --chunk-pool" << std::endl;
    exit(1);
  }
}

string get_opts_object_name(const po::variables_map &opts) {
  if (opts.count("object")) {
    return opts["object"].as<string>();
  } else {
    cerr << "must specify object" << std::endl;
    exit(1);
  }
}

int get_opts_max_thread(const po::variables_map &opts) {
  if (opts.count("max-thread")) {
    return opts["max-thread"].as<int>();
  } else {
    cout << "2 is set as the number of threads by default" << std::endl;
    return 2;
  }
}

int get_opts_report_period(const po::variables_map &opts) {
  if (opts.count("report-period")) {
    return opts["report-period"].as<int>();
  } else {
    cout << "10 seconds is set as report period by default" << std::endl;
    return 10;
  }
}

string make_pool_str(string pool, string var, string val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + val + string("\"}");
}

string make_pool_str(string pool, string var, int val)
{
  return make_pool_str(pool, var, stringify(val));
}
