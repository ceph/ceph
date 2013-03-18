#include <errno.h>
#include "include/types.h"
#include "ceph_ver.h"
#include "include/encoding.h"
#include "include/ceph_features.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"
#include "msg/Message.h"
#include "include/assert.h"

#define TYPE(t)
#define TYPEWITHSTRAYDATA(t)
#define TYPE_FEATUREFUL(t)
#define MESSAGE(t)
#include "types.h"
#undef TYPE
#undef TYPEWITHSTRAYDATA
#undef TYPE_FEATUREFUL
#undef MESSAGE

void usage(ostream &out)
{
  out << "usage: ceph-dencoder [commands ...]" << std::endl;
  out << "\n";
  out << "  version            print version string (to stdout)\n";
  out << "\n";
  out << "  import <encfile>   read encoded data from encfile\n";
  out << "  export <outfile>    write encoded data to outfile\n";
  out << "\n";
  out << "  set_features <num>  set feature bits used for encoding\n";
  out << "  get_features        print feature bits (int) to stdout\n";
  out << "\n";
  out << "  list_types          list supported types\n";
  out << "  type <classname>    select in-memory type\n";
  out << "  decode              decode into in-memory object\n";
  out << "  encode              encode in-memory object\n";
  out << "  dump_json           dump in-memory object as json (to stdout)\n";
  out << "\n";
  out << "  count_tests         print number of generated test objects (to stdout)\n";
  out << "  select_test <n>     select generated test object as in-memory object\n";
}
struct Dencoder {
  virtual ~Dencoder() {}
  virtual string decode(bufferlist bl) = 0;
  virtual void encode(bufferlist& out, uint64_t features) = 0;
  virtual void dump(Formatter *f) = 0;
  virtual void generate() = 0;
  virtual int num_generated() = 0;
  virtual string select_generated(unsigned n) = 0;
  //virtual void print(ostream& out) = 0;
};

template<class T>
class DencoderBase : public Dencoder {
protected:
  T* m_object;
  list<T*> m_list;
  bool stray_okay;

public:
  DencoderBase(bool stray_okay) : m_object(new T), stray_okay(stray_okay) {}
  ~DencoderBase() {
    delete m_object;
  }

  string decode(bufferlist bl) {
    bufferlist::iterator p = bl.begin();
    try {
      m_object->decode(p);
    }
    catch (buffer::error& e) {
      return e.what();
    }
    if (!stray_okay && !p.end())
      return "stray data at end of buffer";
    return string();
  }

  virtual void encode(bufferlist& out, uint64_t features) = 0;

  void dump(Formatter *f) {
    m_object->dump(f);
  }

  void generate() {
    T::generate_test_instances(m_list);
  }
  int num_generated() {
    return m_list.size();
  }
  string select_generated(unsigned i) {
    // allow 0- or 1-based (by wrapping)
    if (i == 0)
      i = m_list.size();
    if (i > m_list.size())
      return "invalid id for generated object";
    typename list<T*>::iterator p = m_list.begin();
    for (i--; i > 0 && p != m_list.end(); ++p, --i) ;
    m_object = *p;
    return string();
  }
};

template<class T>
class DencoderImplNoFeature : public DencoderBase<T> {
public:
  DencoderImplNoFeature(bool stray_ok) : DencoderBase<T>(stray_ok) {}
  virtual void encode(bufferlist& out, uint64_t features) {
    out.clear();
    this->m_object->encode(out);
  }
};

template<class T>
class DencoderImplFeatureful : public DencoderBase<T> {
public:
  DencoderImplFeatureful(bool stray_ok) : DencoderBase<T>(stray_ok) {}
  virtual void encode(bufferlist& out, uint64_t features) {
    out.clear();
    ::encode(*(this->m_object), out, features);
  }
};


template<class T>
class MessageDencoderImpl : public Dencoder {
  T *m_object;
  list<T*> m_list;

public:
  MessageDencoderImpl() {
    m_object = new T;
  }
  ~MessageDencoderImpl() {
    m_object->put();
  }

  string decode(bufferlist bl) {
    bufferlist::iterator p = bl.begin();
    try {
      Message *n = decode_message(g_ceph_context, p);
      if (!n)
	throw std::runtime_error("failed to decode");
      if (n->get_type() != m_object->get_type()) {
	stringstream ss;
	ss << "decoded type " << n->get_type() << " instead of expected " << m_object->get_type();
	throw std::runtime_error(ss.str());
      }
      m_object->put();
      m_object = static_cast<T *>(n);
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
    encode_message(m_object, features, out);
  }

  void dump(Formatter *f) {
    m_object->dump(f);
  }

  void generate() {
    //T::generate_test_instances(m_list);
  }
  int num_generated() {
    return m_list.size();
  }
  string select_generated(unsigned i) {
    // allow 0- or 1-based (by wrapping)
    if (i == 0)
      i = m_list.size();
    if (i > m_list.size())
      return "invalid id for generated object";
    typename list<T*>::iterator p = m_list.begin();
    for (i--; i > 0 && p != m_list.end(); ++p, --i) ;
    m_object->put();
    m_object = *p;
    return string();
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
#define TYPE(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeature<t>(false);
#define TYPEWITHSTRAYDATA(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeature<t>(true);
#define TYPE_FEATUREFUL(t) dencoders[T_STRINGIFY(t)] = new DencoderImplFeatureful<t>(false);
#define MESSAGE(t) dencoders[T_STRINGIFY(t)] = new MessageDencoderImpl<t>;
#include "types.h"
#undef TYPE
#undef TYPEWITHSTRAYDATA
#undef TYPE_FEATUREFUL
#undef T_STR
#undef T_STRRINGIFY

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  Dencoder *den = NULL;
  uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  bufferlist encbl;

  if (args.empty()) {
    usage(cerr);
    exit(1);
  }
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    string err;

    if (*i == string("help") || *i == string("-h") || *i == string("--help")) {
      usage(cout);
      exit(0);
    } else if (*i == string("version")) {
      cout << CEPH_GIT_NICE_VER << std::endl;
    } else if (*i == string("list_types")) {
      for (map<string,Dencoder*>::iterator p = dencoders.begin();
	   p != dencoders.end();
	   ++p)
	cout << p->first << std::endl;
      exit(0);
    } else if (*i == string("type")) {
      ++i;
      if (i == args.end()) {
	usage(cerr);
	exit(1);
      }
      string cname = *i;
      if (!dencoders.count(cname)) {
	cerr << "class '" << cname << "' unknown" << std::endl;
	exit(1);
      }
      den = dencoders[cname];
      den->generate();
    } else if (*i == string("get_features")) {
      cout << CEPH_FEATURES_SUPPORTED_DEFAULT << std::endl;
      exit(0);
    } else if (*i == string("set_features")) {
      ++i;
      if (i == args.end()) {
	usage(cerr);
	exit(1);
      }
      features = atoi(*i);

    } else if (*i == string("encode")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      den->encode(encbl, features);
    } else if (*i == string("decode")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      err = den->decode(encbl);
    } else if (*i == string("dump_json")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      JSONFormatter jf(true);
      jf.open_object_section("object");
      den->dump(&jf);
      jf.close_section();
      jf.flush(cout);
      cout << std::endl;

    } else if (*i == string("import")) {
      ++i;
      if (i == args.end()) {
	usage(cerr);
	exit(1);
      }
      encbl.read_file(*i, &err);
    } else if (*i == string("export")) {
      ++i;
      if (i == args.end()) {
	usage(cerr);
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

    } else if (*i == string("count_tests")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      cout << den->num_generated() << std::endl;
    } else if (*i == string("select_test")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      ++i;
      if (i == args.end()) {
	usage(cerr);
	exit(1);
      }
      int n = atoi(*i);
      err = den->select_generated(n);      
    } else {
      cerr << "unknown option '" << *i << "'" << std::endl;
      usage(cerr);
      exit(1);
    }      
    if (err.length()) {
      cerr << "error: " << err << std::endl;
      exit(1);
    }
  }
  return 0;
}
