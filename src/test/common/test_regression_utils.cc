// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <vector>
#include <cmath>
#include "common/regression_utils.h"
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>

using namespace boost::numeric::ublas;

std::vector<double> generate_rand_vector(int size, int max_value) {
  std::srand(std::time(0));
  std::vector<double> rand_vec;
  for (int i = 0; i < size; i++) {
    double rand_value = std::rand() % max_value;
    rand_vec.push_back(rand_value);
  }
  return rand_vec;
}

matrix<double> generate_rand_matrix(int size1, int size2, int max_value) {
  std::srand(std::time(0));
  matrix<double> rand_m(size1, size2);
  for (int i = 0; i < size1; i++) {
    for (int j = 0; j < size2; j++) {
      double rand_value = std::rand() % max_value;
      rand_m(i, j) = rand_value;
    }
  }
  return rand_m;
}

std::vector<double> exp_vector(std::vector<double> x) {
  std::vector<double> exp_vec;
  for (int i = 0; i < x.size(); i++) {
    exp_vec.push_back(std::exp(x[i]));
  }
  return exp_vec;
}

bool is_almost_equal(double x1, double x2, double precision) {
  if (std::abs(x1 - x2) < precision) {
    return true;
  }
  return false;
}

TEST(matrix_op, matrix_inverse) {
  int matrix_size = 2;    // has to be 2x2
  matrix<double> random_square_m = generate_rand_matrix(matrix_size, matrix_size, 1000);
  matrix<double> random_square_m_inv = ceph::matrix_inverse(random_square_m);
  // the inverse matrix should have the same size
  ASSERT_EQ(random_square_m_inv.size1(), random_square_m.size1());
  ASSERT_EQ(random_square_m_inv.size2(), random_square_m.size2());
  matrix<double> matrix_prod = prod(random_square_m, random_square_m_inv);
  // the product should be an identity matrix
  for ( int i = 0; i < matrix_prod.size1(); i++){
    for (int j = 0; j < matrix_prod.size2(); j++){
      if (i == j) {
        ASSERT_TRUE(is_almost_equal(matrix_prod(i, j), 1, 1e-9));    // i == j -> 1
      } else {
        ASSERT_TRUE(is_almost_equal(matrix_prod(i, j), 0, 1e-9));    // i <> j -> 0
      }
    }
  }
}

TEST(regression, log_regression) {
  // y = ln(x)
  std::vector<double> y = generate_rand_vector(200, 100);
  std::vector<double> x = exp_vector(y);

  double theta[2];    // y = theta[0] + theta[1] * ln(x)
  ceph::regression(x, y, theta);
  ASSERT_TRUE(is_almost_equal(theta[0], 0, 1e-9)); // theta[0] = 0
  ASSERT_TRUE(is_almost_equal(theta[1], 1, 1e-9)); // theta[1] = 1
}

TEST(regression, find_slope_location) {
  // y = ln(x)
  std::vector<double> y = generate_rand_vector(200, 100);
  std::vector<double> x = exp_vector(y);

  double target_slope = 5;
  double x_target = ceph::find_slope_on_curve(x, y, target_slope);
  ASSERT_TRUE(is_almost_equal(x_target, 0.2, 1e-9));  // y'(0.2) = 5
}
