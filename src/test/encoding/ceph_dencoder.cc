#include <errno.h>
#include "include/types.h"
#include "include/encoding.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"

#define TYPE(t)
#include "types.h"
#undef TYPE

void usage()
{
  cerr << "usage: ceph-dencoder --list-types" << std::endl;
  cerr << "       ceph-dencoder <class> [commands ...]" << std::endl;
  cerr << "  -i encfile    read encoded data from encfile\n";
  cerr << "  decode        decode into in-core object\n";
  cerr << "  encode        encode in-core object\n";
  cerr << "  -o outfile    write encoded data to outfile\n";
  cerr << "  -f features   set feature bits used for encoding\n";
  cerr << "  dump_json     dump in-core object as json\n";
}

struct Dencoder {
  virtual ~Dencoder() {}
  virtual string decode(bufferlist bl) = 0;
  virtual void encode(bufferlist& out, uint64_t features) = 0;
  virtual void dump(Formatter *f) = 0;
  //virtual void print(ostream& out) = 0;
};

template<class T>
class DencoderImpl : public Dencoder {
  T m_object;

public:
  DencoderImpl() {}

  string decode(bufferlist bl) {
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(m_object, p);
    }
    catch (buffer::error& e) {
      return e.what();
    }
    if (!p.end())
      return "stray data at end of buffer";
    return string();
  }

  void encode(bufferlist& out, uint64_t features) {
    out.clear();
    ::encode(m_object, out, features);
  }

  void dump(Formatter *f) {
    m_object.dump(f);
  }

  //void print(ostream& out) {
  //out << m_object << std::endl;
  //}
};



int main(int argc, const char **argv)
{
  // dencoders
  map<string,Dencoder*> dencoders;

#define T_STR(x) #x
#define T_STRINGIFY(x) T_STR(x)
#define TYPE(t) dencoders[T_STRINGIFY(t)] = new DencoderImpl<t>;
#include "types.h"
#undef TYPE
#undef T_STR
#undef T_STRRINGIFY

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  std::vector<const char*>::iterator i = args.begin();
  if (i == args.end()) {
    usage();
    exit(1);
  }
  string cname = *i;
  if (cname == string("--list-types")) {
    for (map<string,Dencoder*>::iterator p = dencoders.begin();
	 p != dencoders.end();
	 ++p)
      cout << p->first << std::endl;
    exit(0);
  } 
  if (cname == string("--get-features")) {
    cout << CEPH_FEATURES_SUPPORTED_DEFAULT << std::endl;
    exit(0);
  } 

  if (!dencoders.count(cname)) {
    cerr << "class '" << cname << "' unknown" << std::endl;
    exit(1);
  }
  Dencoder *den = dencoders[cname];

  uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  bufferlist encbl;
  for (i++; i != args.end(); ++i) {
    string err;
    if (*i == string("encode")) {
      den->encode(encbl, features);
    } else if (*i == string("decode")) {
      err = den->decode(encbl);
    } else if (*i == string("dump_json")) {
      JSONFormatter jf(true);
      jf.open_object_section("object");
      den->dump(&jf);
      jf.close_section();
      jf.flush(cout);
      cout << std::endl;
      //} else if (*i == string("print")) {
      //den->print(cout);
    } else if (*i == string("-f")) {
      i++;
      if (i == args.end()) {
	usage();
	exit(1);
      }
      features = atoi(*i);
    } else if (*i == string("-i")) {
      i++;
      if (i == args.end()) {
	usage();
	exit(1);
      }
      encbl.read_file(*i, &err);
    } else if (*i == string("-o")) {
      i++;
      if (i == args.end()) {
	usage();
	exit(1);
      }
      int fd = ::open(*i, O_WRONLY|O_CREAT|O_TRUNC, 0644);
      if (fd < 0) {
	cerr << "error opening " << *i << " for write: " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      int r = encbl.write_fd(fd);
      if (r < 0) {
	cerr << "error writing " << *i << ": " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      ::close(fd);
    } else {
      cerr << "unknown option '" << *i << "'" << std::endl;
      usage();
      exit(1);
    }      
    if (err.length()) {
      cout << err << std::endl;
      exit(1);
    }
  }
  return 0;
}
