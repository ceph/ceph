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

#pragma once

#include <vector>

/**
 * Weight-sets that correct the CRUSH multi-pick anomaly.
 *
 * When a rule picks r distinct items from a straw2 bucket, each pick is a
 * weighted draw that is repeated if it collides with an earlier pick. That is
 * sampling without replacement, and with the bucket's weights at every
 * position it gives low-weight items more than their share, w_i / W
 * (https://tracker.ceph.com/issues/15653).
 *
 * The functions here compute the weights to draw with instead, from the
 * bucket's weights alone. Their results are meant to be stored as a
 * weight-set (choose_args) of the bucket.
 *
 * A layout is feasible if no item has more than 1/r of the total weight;
 * otherwise an item would need more than one replica of every PG. Items with
 * a weight of 0 keep a weight of 0.
 */
namespace ceph::crush {

/**
 * For each item, the probability that it is among the first r distinct
 * items drawn with weights lam (successive sampling).
 *
 * Item i can be thought of as firing after an Exp(lam_i) time; the first r
 * to fire are chosen. So this is the expectation, over the time item i
 * fires, of the probability that fewer than r others fired earlier: a
 * Poisson-binomial tail, integrated numerically. About 300 * c * n * r
 * operations for n items with c distinct weights.
 */
std::vector<double> inclusion_probabilities(const std::vector<double>& lam,
                                            unsigned r);

/**
 * One weight per item such that the probability that an item is among the r
 * chosen is r * w_i / W.
 *
 * Valid for any rule that picks r distinct items of the bucket with the same
 * weights at every position, firstn or indep. The total per item is exact;
 * the share of an item at one position, the first for instance, is not.
 *
 * The cost grows with the number of distinct weights: an iteration takes
 * about 300 * c * n * r operations for n items with c distinct weights, a
 * few milliseconds for hundreds of hosts of a few sizes, and seconds for a
 * thousand items that all differ.
 *
 * @param w item weights
 * @param r number of items picked from the bucket
 * @param max_work give up if c * n * r exceeds this; the default allows
 *                 about a second
 * @return weights with the same sum as w, or empty if the layout is not
 *         feasible, the iteration did not converge or it exceeds max_work
 */
std::vector<double> solve_flat_weights(const std::vector<double>& w,
                                       unsigned r,
                                       double max_work = 3e5);

/**
 * Weights for each position such that the probability that an item is picked
 * at position k is w_i / W, for every k < r.
 *
 * Only for firstn: there, position k draws with the weights of position k
 * until it finds an item that positions 0..k-1 did not pick. The cost of
 * position k is O(n^k) per iteration, so this is for small r.
 *
 * @param w item weights
 * @param r number of items picked from the bucket
 * @param max_work give up if a position needs more than about this many
 *                 operations per iteration
 * @return r vectors of weights, each with the same sum as w; empty if the
 *         layout is not feasible, did not converge or exceeds max_work
 */
std::vector<std::vector<double>> solve_positional_weights(
  const std::vector<double>& w,
  unsigned r,
  double max_work = 2e8);

/**
 * The probability that each item is picked at each position, when position
 * k draws with weights p[k] until it finds an item that earlier positions
 * did not pick. Exact, O(n^positions); for tests and small buckets.
 */
std::vector<std::vector<double>> position_probabilities(
  const std::vector<std::vector<double>>& p);

} // namespace ceph::crush
