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

class ErasureCodeJerasure : public ceph::ErasureCode {
public:
  int k;
  std::string DEFAULT_K;
  int m;
  std::string DEFAULT_M;
  int w;
  std::string DEFAULT_W;
  const char *technique;
  std::string rule_root;
  std::string rule_failure_domain;
  bool per_chunk_alignment;

  explicit ErasureCodeJerasure(const char *_technique) :
    k(0),
    DEFAULT_K("2"),
    m(0),
    DEFAULT_M("1"),
    w(0),
    DEFAULT_W("8"),
    technique(_technique),
    per_chunk_alignment(false)
  {}

  ~ErasureCodeJerasure() override {}
  
  unsigned int get_chunk_count() const override {
    return k + m;
  }

  unsigned int get_data_chunk_count() const override {
    return k;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  int encode_chunks(const std::set<int> &want_to_encode,
		    std::map<int, ceph::buffer::list> *encoded) override;

  int decode_chunks(const std::set<int> &want_to_read,
		    const std::map<int, ceph::buffer::list> &chunks,
		    std::map<int, ceph::buffer::list> *decoded) override;

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

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
  virtual int parse(ceph::ErasureCodeProfile &profile, std::ostream *ss);
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
  ~ErasureCodeJerasureReedSolomonVandermonde() override {
    if (matrix)
      free(matrix);
  }

  void jerasure_encode(char **data,
                               char **coding,
                               int blocksize) override;
  int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize) override;
  unsigned get_alignment() const override;
  void prepare() override;
private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
};

class ErasureCodeJerasureReedSolomonRAID6 : public ErasureCodeJerasure {
public:
  int *matrix;

  ErasureCodeJerasureReedSolomonRAID6() :
    ErasureCodeJerasure("reed_sol_r6_op"),
    matrix(0)
  {
    DEFAULT_K = "7";
    DEFAULT_M = "2";
    DEFAULT_W = "8";
  }
  ~ErasureCodeJerasureReedSolomonRAID6() override {
    if (matrix)
      free(matrix);
  }

  void jerasure_encode(char **data,
                               char **coding,
                               int blocksize) override;
  int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize) override;
  unsigned get_alignment() const override;
  void prepare() override;
private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
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
  ~ErasureCodeJerasureCauchy() override;

  void jerasure_encode(char **data,
                               char **coding,
                               int blocksize) override;
  int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize) override;
  unsigned get_alignment() const override;
  void prepare_schedule(int *matrix);
private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
};

class ErasureCodeJerasureCauchyOrig : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyOrig() :
    ErasureCodeJerasureCauchy("cauchy_orig")
  {}

  void prepare() override;
};

class ErasureCodeJerasureCauchyGood : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyGood() :
    ErasureCodeJerasureCauchy("cauchy_good")
  {}

  void prepare() override;
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
  ~ErasureCodeJerasureLiberation() override;

  void jerasure_encode(char **data,
                               char **coding,
                               int blocksize) override;
  int jerasure_decode(int *erasures,
                               char **data,
                               char **coding,
                               int blocksize) override;
  unsigned get_alignment() const override;
  virtual bool check_k(std::ostream *ss) const;
  virtual bool check_w(std::ostream *ss) const;
  virtual bool check_packetsize_set(std::ostream *ss) const;
  virtual bool check_packetsize(std::ostream *ss) const;
  virtual int revert_to_default(ceph::ErasureCodeProfile& profile,
				std::ostream *ss);
  void prepare() override;
private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
};

class ErasureCodeJerasureBlaumRoth : public ErasureCodeJerasureLiberation {
public:
  ErasureCodeJerasureBlaumRoth() :
    ErasureCodeJerasureLiberation("blaum_roth")
  {
  }

  bool check_w(std::ostream *ss) const override;
  void prepare() override;
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

  void prepare() override;
private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
};

#endif
