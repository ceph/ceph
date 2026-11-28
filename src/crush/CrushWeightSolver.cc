// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "crush/CrushWeightSolver.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <numeric>

namespace ceph::crush {

namespace {

const double tolerance = 1e-10;
const int max_iterations = 2000;

// Gauss-Legendre rule on (-1, 1)
struct gauss_legendre_t {
  static constexpr int points = 8;
  double x[points];
  double w[points];
  gauss_legendre_t() {
    for (int i = 0; i < (points + 1) / 2; i++) {
      // Newton's method on the Legendre polynomial of degree `points`
      double z = std::cos(M_PI * (i + 0.75) / (points + 0.5));
      double dp = 0;
      for (int it = 0; it < 100; it++) {
        double p0 = 1, p1 = z;
        for (int k = 2; k <= points; k++) {
          double p2 = ((2 * k - 1) * z * p1 - (k - 1) * p0) / k;
          p0 = p1;
          p1 = p2;
        }
        dp = points * (z * p1 - p0) / (z * z - 1);
        double dz = p1 / dp;
        z -= dz;
        if (std::abs(dz) < 1e-15) {
          break;
        }
      }
      x[i] = -z;
      x[points - 1 - i] = z;
      w[i] = w[points - 1 - i] = 2 / ((1 - z * z) * dp * dp);
    }
  }
};

/*
 * Panels to integrate lam_i * exp(-lam_i * t) * g(t) over t >= 0, where
 * g(t) is the probability that fewer than r of the other items fired by t.
 * g varies on the scale of 1 / total (the rate at which items fire) and is
 * negligible after about r of them fired, so: even panels over that range,
 * then doubling ones until exp(-lam_i * t) is negligible too.
 */
std::vector<double> panel_edges(double lam_i, double total, unsigned r)
{
  const double width = 2 / total;
  const double bulk = (r + 10 * std::sqrt(r) + 20) / total;
  const double end = std::max(bulk, 45 / lam_i);
  std::vector<double> edges;
  for (double t = 0; t < bulk; t += width) {
    edges.push_back(t);
  }
  edges.push_back(bulk);
  while (edges.back() < end) {
    const double last = edges.back() - edges[edges.size() - 2];
    edges.push_back(std::min(end, edges.back() + 2 * last));
  }
  return edges;
}

// probabilities that sum to 1; false if there is nothing to pick from
bool normalize(std::vector<double>& v)
{
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  if (!(sum > 0)) {
    return false;
  }
  for (auto& x : v) {
    x /= sum;
  }
  return true;
}

bool feasible(const std::vector<double>& t, unsigned r)
{
  unsigned nonzero = 0;
  for (auto x : t) {
    if (x < 0 || !std::isfinite(x)) {
      return false;
    }
    if (x > 0) {
      nonzero++;
    }
    if (x * r > 1 + 1e-9) {
      return false;
    }
  }
  return nonzero >= r;
}

std::vector<double> scaled(std::vector<double> p, double sum)
{
  for (auto& x : p) {
    x *= sum;
  }
  return p;
}

/*
 * Probabilities at position k = p.size() - 1, given the weights of
 * positions 0..k. Walks every ordered choice of the earlier positions.
 *
 * P(i at k) = sum over earlier choices S without i of
 *             P(S) * p[k][i] / (1 - p[k](S))
 *           = p[k][i] * (total - with[i])
 * where total sums P(S) / (1 - p[k](S)) over all S and with[i] over the S
 * that contain i.
 */
struct position_walk_t {
  const std::vector<std::vector<double>>& p;
  const unsigned k;
  const unsigned n;
  double total = 0;
  std::vector<double> with;
  std::vector<unsigned> chosen;
  std::vector<char> used;

  explicit position_walk_t(const std::vector<std::vector<double>>& p)
    : p(p), k(p.size() - 1), n(p[0].size()), with(n, 0.0), used(n, 0) {}

  void walk(unsigned depth, double prob) {
    if (depth == k) {
      double taken = 0;
      for (auto j : chosen) {
        taken += p[k][j];
      }
      double v = prob / (1 - taken);
      total += v;
      for (auto j : chosen) {
        with[j] += v;
      }
      return;
    }
    double taken = 0;
    for (auto j : chosen) {
      taken += p[depth][j];
    }
    const double rest = 1 - taken;
    for (unsigned j = 0; j < n; j++) {
      if (used[j] || p[depth][j] <= 0) {
        continue;
      }
      used[j] = 1;
      chosen.push_back(j);
      walk(depth + 1, prob * p[depth][j] / rest);
      chosen.pop_back();
      used[j] = 0;
    }
  }

  std::vector<double> run() {
    walk(0, 1.0);
    std::vector<double> m(n);
    for (unsigned i = 0; i < n; i++) {
      m[i] = p[k][i] * (total - with[i]);
    }
    return m;
  }
};

} // anonymous namespace

std::vector<double> inclusion_probabilities(const std::vector<double>& lam,
                                            unsigned r)
{
  static const gauss_legendre_t gl;
  const unsigned n = lam.size();
  std::vector<double> out(n, 0.0);
  double total = 0;
  for (auto x : lam) {
    if (x > 0) {
      total += x;
    }
  }
  if (r == 0 || !(total > 0)) {
    return out;
  }
  // items of equal weight have equal probabilities
  std::map<double, double> done;
  std::vector<double> dp(r);
  for (unsigned i = 0; i < n; i++) {
    if (lam[i] <= 0) {
      continue;
    }
    if (auto p = done.find(lam[i]); p != done.end()) {
      out[i] = p->second;
      continue;
    }
    double sum = 0;
    const auto edges = panel_edges(lam[i], total, r);
    for (unsigned panel = 0; panel + 1 < edges.size(); panel++) {
      const double half = (edges[panel + 1] - edges[panel]) / 2;
      for (int node = 0; node < gauss_legendre_t::points; node++) {
        const double t = edges[panel] + (gl.x[node] + 1) * half;
        // dp[c]: probability that c of the other items fired before t
        std::fill(dp.begin(), dp.end(), 0.0);
        dp[0] = 1;
        for (unsigned j = 0; j < n; j++) {
          if (j == i || lam[j] <= 0) {
            continue;
          }
          const double fired = -std::expm1(-lam[j] * t);
          for (unsigned c = r - 1; c > 0; c--) {
            dp[c] = dp[c] * (1 - fired) + dp[c - 1] * fired;
          }
          dp[0] *= 1 - fired;
        }
        sum += gl.w[node] * half * lam[i] * std::exp(-lam[i] * t) *
          std::accumulate(dp.begin(), dp.end(), 0.0);
      }
    }
    out[i] = sum;
    done[lam[i]] = sum;
  }
  return out;
}

std::vector<double> solve_flat_weights(const std::vector<double>& w,
                                       unsigned r,
                                       double max_work)
{
  const double total = std::accumulate(w.begin(), w.end(), 0.0);
  std::vector<double> t = w;
  if (r == 0 || !normalize(t) || !feasible(t, r)) {
    return {};
  }
  if (r == 1) {
    return w;
  }
  {
    std::vector<double> distinct;
    for (auto x : t) {
      if (x > 0) {
        distinct.push_back(x);
      }
    }
    const double n = distinct.size();
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()),
                   distinct.end());
    if (distinct.size() * n * r > max_work) {
      return {};
    }
  }
  std::vector<double> lam = t;
  for (int it = 0; it < max_iterations; it++) {
    auto inc = inclusion_probabilities(lam, r);
    double err = 0;
    for (unsigned i = 0; i < t.size(); i++) {
      err = std::max(err, std::abs(inc[i] - r * t[i]));
    }
    if (err < tolerance) {
      return scaled(lam, total);
    }
    for (unsigned i = 0; i < t.size(); i++) {
      if (t[i] <= 0) {
        continue;
      }
      const double target = r * t[i];
      // step on the weight and, damped, on the odds of being left out:
      // an item close to being in every pick needs a very large weight
      lam[i] *= target / inc[i] *
        std::sqrt((1 - inc[i] + 1e-15) / (1 - target + 1e-15));
    }
    normalize(lam);
  }
  return {};
}

std::vector<std::vector<double>> position_probabilities(
  const std::vector<std::vector<double>>& p)
{
  std::vector<std::vector<double>> out;
  for (unsigned k = 0; k < p.size(); k++) {
    std::vector<std::vector<double>> head(p.begin(), p.begin() + k + 1);
    for (auto& v : head) {
      normalize(v);
    }
    out.push_back(k == 0 ? head[0] : position_walk_t(head).run());
  }
  return out;
}

std::vector<std::vector<double>> solve_positional_weights(
  const std::vector<double>& w,
  unsigned r,
  double max_work)
{
  const double total = std::accumulate(w.begin(), w.end(), 0.0);
  std::vector<double> t = w;
  if (r == 0 || !normalize(t) || !feasible(t, r)) {
    return {};
  }
  const double n = std::count_if(t.begin(), t.end(),
                                 [](double x) { return x > 0; });
  if (r > 1 && std::pow(n, r - 1) * r > max_work) {
    return {};
  }
  std::vector<std::vector<double>> p;
  p.push_back(t);
  for (unsigned k = 1; k < r; k++) {
    p.push_back(t);
    bool converged = false;
    for (int it = 0; it < max_iterations; it++) {
      auto m = position_walk_t(p).run();
      double err = 0;
      for (unsigned i = 0; i < t.size(); i++) {
        err = std::max(err, std::abs(m[i] - t[i]));
      }
      if (err < tolerance) {
        converged = true;
        break;
      }
      for (unsigned i = 0; i < t.size(); i++) {
        if (t[i] > 0) {
          p[k][i] *= t[i] / m[i];
        }
      }
      normalize(p[k]);
    }
    if (!converged) {
      return {};
    }
  }
  for (auto& v : p) {
    v = scaled(v, total);
  }
  return p;
}

} // namespace ceph::crush
