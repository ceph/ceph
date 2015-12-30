// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


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
#define TYPE_STRAYDATA(t)
#define TYPE_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL(t)
#define TYPE_FEATUREFUL_STRAYDATA(t)
#define TYPE_FEATUREFUL_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL_NOCOPY(t)
#define TYPE_NOCOPY(t)
#define MESSAGE(t)
#include "types.h"
#undef TYPE
#undef TYPE_STRAYDATA
#undef TYPE_NONDETERMINISTIC
#undef TYPE_NOCOPY
#undef TYPE_FEATUREFUL
#undef TYPE_FEATUREFUL_STRAYDATA
#undef TYPE_FEATUREFUL_NONDETERMINISTIC
#undef TYPE_FEATUREFUL_NOCOPY
#undef MESSAGE

#define MB(m) ((m) * 1024 * 1024)

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
  out << "  skip <num>          skip <num> leading bytes before decoding\n";
  out << "  decode              decode into in-memory object\n";
  out << "  encode              encode in-memory object\n";
  out << "  dump_json           dump in-memory object as json (to stdout)\n";
  out << "\n";
  out << "  copy                copy object (via operator=)\n";
  out << "  copy_ctor           copy object (via copy ctor)\n";
  out << "\n";
  out << "  count_tests         print number of generated test objects (to stdout)\n";
  out << "  select_test <n>     select generated test object as in-memory object\n";
  out << "  is_deterministic    exit w/ success if type encodes deterministically\n";
}
struct Dencoder {
  virtual ~Dencoder() {}
  virtual string decode(bufferlist bl, uint64_t seek) = 0;
  virtual void encode(bufferlist& out, uint64_t features) = 0;
  virtual void dump(ceph::Formatter *f) = 0;
  virtual void copy() {
    cerr << "copy operator= not supported" << std::endl;
  }
  virtual void copy_ctor() {
    cerr << "copy ctor not supported" << std::endl;
  }
  virtual void generate() = 0;
  virtual int num_generated() = 0;
  virtual string select_generated(unsigned n) = 0;
  virtual bool is_deterministic() = 0;
  //virtual void print(ostream& out) = 0;
};

template<class T>
class DencoderBase : public Dencoder {
protected:
  T* m_object;
  list<T*> m_list;
  bool stray_okay;
  bool nondeterministic;

public:
  DencoderBase(bool stray_okay, bool nondeterministic)
    : m_object(new T),
      stray_okay(stray_okay),
      nondeterministic(nondeterministic) {}
  ~DencoderBase() {
    delete m_object;
  }

  string decode(bufferlist bl, uint64_t seek) {
    bufferlist::iterator p = bl.begin();
    p.seek(seek);
    try {
      m_object->decode(p);
    }
    catch (buffer::error& e) {
      return e.what();
    }
    if (!stray_okay && !p.end()) {
      ostringstream ss;
      ss << "stray data at end of buffer, offset " << p.get_off();
      return ss.str();
    }
    return string();
  }

  virtual void encode(bufferlist& out, uint64_t features) = 0;

  void dump(ceph::Formatter *f) {
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
    if ((i == 0) || (i > m_list.size()))
      return "invalid id for generated object";
    typename list<T*>::iterator p = m_list.begin();
    for (i--; i > 0 && p != m_list.end(); ++p, --i) ;
    m_object = *p;
    return string();
  }

  bool is_deterministic() {
    return !nondeterministic;
  }
};

template<class T>
class DencoderImplNoFeatureNoCopy : public DencoderBase<T> {
public:
  DencoderImplNoFeatureNoCopy(bool stray_ok, bool nondeterministic)
    : DencoderBase<T>(stray_ok, nondeterministic) {}
  virtual void encode(bufferlist& out, uint64_t features) {
    out.clear();
    this->m_object->encode(out);
  }
};

template<class T>
class DencoderImplNoFeature : public DencoderImplNoFeatureNoCopy<T> {
public:
  DencoderImplNoFeature(bool stray_ok, bool nondeterministic)
    : DencoderImplNoFeatureNoCopy<T>(stray_ok, nondeterministic) {}
  void copy() {
    T *n = new T;
    *n = *this->m_object;
    delete this->m_object;
    this->m_object = n;
  }
  void copy_ctor() {
    T *n = new T(*this->m_object);
    delete this->m_object;
    this->m_object = n;
  }
};

template<class T>
class DencoderImplFeaturefulNoCopy : public DencoderBase<T> {
public:
  DencoderImplFeaturefulNoCopy(bool stray_ok, bool nondeterministic)
    : DencoderBase<T>(stray_ok, nondeterministic) {}
  virtual void encode(bufferlist& out, uint64_t features) {
    out.clear();
    ::encode(*(this->m_object), out, features);
  }
};

template<class T>
class DencoderImplFeatureful : public DencoderImplFeaturefulNoCopy<T> {
public:
  DencoderImplFeatureful(bool stray_ok, bool nondeterministic)
    : DencoderImplFeaturefulNoCopy<T>(stray_ok, nondeterministic) {}
  void copy() {
    T *n = new T;
    *n = *this->m_object;
    delete this->m_object;
    this->m_object = n;
  }
  void copy_ctor() {
    T *n = new T(*this->m_object);
    delete this->m_object;
    this->m_object = n;
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

  string decode(bufferlist bl, uint64_t seek) {
    bufferlist::iterator p = bl.begin();
    p.seek(seek);
    try {
      Message *n = decode_message(g_ceph_context, 0, p);
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
    if (!p.end()) {
      ostringstream ss;
      ss << "stray data at end of buffer, offset " << p.get_off();
      return ss.str();
    }
    return string();
  }

  void encode(bufferlist& out, uint64_t features) {
    out.clear();
    encode_message(m_object, features, out);
  }

  void dump(ceph::Formatter *f) {
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
    if ((i == 0) || (i > m_list.size()))
      return "invalid id for generated object";
    typename list<T*>::iterator p = m_list.begin();
    for (i--; i > 0 && p != m_list.end(); ++p, --i) ;
    m_object->put();
    m_object = *p;
    return string();
  }
  bool is_deterministic() {
    return true;
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
#define TYPE(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeature<t>(false, false);
#define TYPE_STRAYDATA(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeature<t>(true, false);
#define TYPE_NONDETERMINISTIC(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeature<t>(false, true);
#define TYPE_FEATUREFUL(t) dencoders[T_STRINGIFY(t)] = new DencoderImplFeatureful<t>(false, false);
#define TYPE_FEATUREFUL_STRAYDATA(t) dencoders[T_STRINGIFY(t)] = new DencoderImplFeatureful<t>(true, false);
#define TYPE_FEATUREFUL_NONDETERMINISTIC(t) dencoders[T_STRINGIFY(t)] = new DencoderImplFeatureful<t>(false, true);
#define TYPE_FEATUREFUL_NOCOPY(t) dencoders[T_STRINGIFY(t)] = new DencoderImplFeaturefulNoCopy<t>(false, false);
#define TYPE_NOCOPY(t) dencoders[T_STRINGIFY(t)] = new DencoderImplNoFeatureNoCopy<t>(false, false);
#define MESSAGE(t) dencoders[T_STRINGIFY(t)] = new MessageDencoderImpl<t>;
#include "types.h"
#undef TYPE
#undef TYPE_STRAYDATA
#undef TYPE_NONDETERMINISTIC
#undef TYPE_NOCOPY
#undef TYPE_FEATUREFUL
#undef TYPE_FEATUREFUL_STRAYDATA
#undef TYPE_FEATUREFUL_NONDETERMINISTIC
#undef TYPE_FEATUREFUL_NOCOPY
#undef T_STR
#undef T_STRINGIFY

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  Dencoder *den = NULL;
  uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  bufferlist encbl;
  uint64_t skip = 0;

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
    } else if (*i == string("skip")) {
      ++i;
      if (i == args.end()) {
	usage(cerr);
	exit(1);
      }
      skip = atoi(*i);
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
      den->encode(encbl, features | CEPH_FEATURE_RESERVED); // hack for OSDMap
    } else if (*i == string("decode")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      err = den->decode(encbl, skip);
    } else if (*i == string("copy_ctor")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      den->copy_ctor();
    } else if (*i == string("copy")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      den->copy();
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
      int r;
      if (*i == string("-")) {
        *i = "stdin";
	// Read up to 1mb if stdin specified
	r = encbl.read_fd(STDIN_FILENO, MB(1));
      } else {
	r = encbl.read_file(*i, &err);
      }
      if (r < 0) {
        cerr << "error reading " << *i << ": " << err << std::endl;
        exit(1);
      }

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
    } else if (*i == string("is_deterministic")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	usage(cerr);
	exit(1);
      }
      if (den->is_deterministic())
	exit(0);
      else
	exit(1);
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
