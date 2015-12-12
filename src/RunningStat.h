// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef H_RunninStat
#define H_RunninStat

class RunningStat {
  private:
    int m_n;
    double m_oldM, m_newM, m_oldS, m_newS;
  public:
    RunningStat();
    void Clear();
    void Push(double x);
    int NumDataValues() const;
    double Mean() const;
    double Variance() const;
    double StandardDeviation() const;
};

#endif
