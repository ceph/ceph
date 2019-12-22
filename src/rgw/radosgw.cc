// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//

extern int radosgw_Main(int, const char **);

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, char **argv)
{
  return radosgw_Main(argc, const_cast<const char **>(argv));
}
