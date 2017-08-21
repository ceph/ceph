#include <iostream>
#include <fstream>

#include "common/Formatter.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"

int main(int argc, const char **argv)
{
  string srcfn = "map.txt";
  ifstream in(srcfn.c_str());
  if (!in.is_open()) {
    cerr << "input file " << srcfn << " not found" << std::endl;
    return -1;
  }
  CrushWrapper crush;
  CrushCompiler cc(crush, cerr, 1);
  cc.compile(in, srcfn.c_str());
  boost::scoped_ptr<ceph::Formatter> f(ceph::Formatter::create("json-pretty", "json-pretty", "json-pretty"));
  f->open_object_section("crush_map");
  crush.dump(f.get());
  f->close_section();
  f->flush(cout);
  cout << "\n";
}
