// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_JERASURE_H
#define CEPH_ERASURE_CODE_JERASURE_H

#include "erasure-code/ErasureCode.h"

#define DEFAULT_RULESET_ROOT "default"
#define DEFAULT_RULESET_FAILURE_DOMAIN "host"

class ErasureCodeJerasure : public ErasureCode {
public:
  int k;
  std::string DEFAULT_K;
  int m;
  std::string DEFAULT_M;
  int w;
  std::string DEFAULT_W;
  const char *technique;
  string ruleset_root;
  string ruleset_failure_domain;
  bool per_chunk_alignment;

  explicit ErasureCodeJerasure(const char *_technique) :
    k(0),
    DEFAULT_K("2"),
    m(0),
    DEFAULT_M("1"),
    w(0),
    DEFAULT_W("8"),
    technique(_technique),
    ruleset_root(DEFAULT_RULESET_ROOT),
    ruleset_failure_domain(DEFAULT_RULESET_FAILURE_DOMAIN),
    per_chunk_alignment(false)
  {}

  virtual ~ErasureCodeJerasure() {}
  
  virtual int create_ruleset(const string &name,
			     CrushWrapper &crush,
			     ostream *ss) const;

  virtual unsigned int get_chunk_count() const {
    return k + m;
  }

  virtual unsigned int get_data_chunk_count() const {
    return k;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  virtual int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded);

  virtual int decode_chunks(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded);

  virtual int init(ErasureCodeProfile &profile, ostream *ss);

  virtual void jerasure_encode(char **data,
                               char **coding,
                               int blocksize) = 0;
  virtual int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize) = 0;
  virtual unsigned get_alignment() const = 0;
  virtual void prepare() = 0;
  static bool is_prime(int value);
protected:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

class ErasureCodeJerasureReedSolomonVandermonde : public ErasureCodeJerasure {
public:
  int *matrix;

  ErasureCodeJerasureReedSolomonVandermonde() :
    ErasureCodeJerasure("reed_sol_van"),
    matrix(0)
  {
    DEFAULT_K = "7";
    DEFAULT_M = "3";
    DEFAULT_W = "8";
  }
  virtual ~ErasureCodeJerasureReedSolomonVandermonde() {
    if (matrix)
      free(matrix);
  }

  virtual void jerasure_encode(char **data,
                               char **coding,
                               int blocksize);
  virtual int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize);
  virtual unsigned get_alignment() const;
  virtual void prepare();
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

class ErasureCodeJerasureReedSolomonRAID6 : public ErasureCodeJerasure {
public:
  int *matrix;

  ErasureCodeJerasureReedSolomonRAID6() :
    ErasureCodeJerasure("reed_sol_r6_op"),
    matrix(0)
  {
    DEFAULT_K = "7";
    DEFAULT_W = "8";
  }
  virtual ~ErasureCodeJerasureReedSolomonRAID6() {
    if (matrix)
      free(matrix);
  }

  virtual void jerasure_encode(char **data,
                               char **coding,
                               int blocksize);
  virtual int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize);
  virtual unsigned get_alignment() const;
  virtual void prepare();
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

#define DEFAULT_PACKETSIZE "2048"

class ErasureCodeJerasureCauchy : public ErasureCodeJerasure {
public:
  int *bitmatrix;
  int **schedule;
  int packetsize;

  explicit ErasureCodeJerasureCauchy(const char *technique) :
    ErasureCodeJerasure(technique),
    bitmatrix(0),
    schedule(0),
    packetsize(0)
  {
    DEFAULT_K = "7";
    DEFAULT_M = "3";
    DEFAULT_W = "8";
  }
  virtual ~ErasureCodeJerasureCauchy() {
    if (bitmatrix)
      free(bitmatrix);
    if (schedule)
      free(schedule);
  }

  virtual void jerasure_encode(char **data,
                               char **coding,
                               int blocksize);
  virtual int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize);
  virtual unsigned get_alignment() const;
  void prepare_schedule(int *matrix);
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

class ErasureCodeJerasureCauchyOrig : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyOrig() :
    ErasureCodeJerasureCauchy("cauchy_orig")
  {}

  virtual void prepare();
};

class ErasureCodeJerasureCauchyGood : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyGood() :
    ErasureCodeJerasureCauchy("cauchy_good")
  {}

  virtual void prepare();
};

class ErasureCodeJerasureLiberation : public ErasureCodeJerasure {
public:
  int *bitmatrix;
  int **schedule;
  int packetsize;

  explicit ErasureCodeJerasureLiberation(const char *technique = "liberation") :
    ErasureCodeJerasure(technique),
    bitmatrix(0),
    schedule(0),
    packetsize(0)
  {
    DEFAULT_K = "2";
    DEFAULT_M = "2";
    DEFAULT_W = "7";
  }
  virtual ~ErasureCodeJerasureLiberation();

  virtual void jerasure_encode(char **data,
                               char **coding,
                               int blocksize);
  virtual int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize);
  virtual unsigned get_alignment() const;
  virtual bool check_k(ostream *ss) const;
  virtual bool check_w(ostream *ss) const;
  virtual bool check_packetsize_set(ostream *ss) const;
  virtual bool check_packetsize(ostream *ss) const;
  virtual int revert_to_default(ErasureCodeProfile &profile,
				ostream *ss);
  virtual void prepare();
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

class ErasureCodeJerasureBlaumRoth : public ErasureCodeJerasureLiberation {
public:
  ErasureCodeJerasureBlaumRoth() :
    ErasureCodeJerasureLiberation("blaum_roth")
  {
  }

  virtual bool check_w(ostream *ss) const;
  virtual void prepare();
};

class ErasureCodeJerasureLiber8tion : public ErasureCodeJerasureLiberation {
public:
  ErasureCodeJerasureLiber8tion() :
    ErasureCodeJerasureLiberation("liber8tion")
  {
    DEFAULT_K = "2";
    DEFAULT_M = "2";
    DEFAULT_W = "8";
  }

  virtual void prepare();
private:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss);
};

#endif
