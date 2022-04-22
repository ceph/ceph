// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <iostream>
#include <vector>
#include <cmath>
#include <boost/numeric/ublas/matrix.hpp>

#define Z_P 2.33  // z score for 99th percentile


namespace ceph {
  /***
  * Calculate the inverse of a 2x2 matrix.
  * @param matrix<double>& m, an square 2x2 matrix
  * @return the inverse of the m (m^-1)
  */
  static boost::numeric::ublas::matrix<double>
  matrix_inverse(boost::numeric::ublas::matrix<double> &m) {
    assert(m.size1() == m.size2() &&
           "Can only calculate the inverse of square matrices");
    assert(m.size1() == 2 && m.size2() == 2 && "Only for 2x2 matrices");
    boost::numeric::ublas::matrix<double> m_inverse(2, 2);
    const double a = m(0, 0);
    const double b = m(0, 1);
    const double c = m(1, 0);
    const double d = m(1, 1);
    const double determinant = 1.0 / ((a * d) - (b * c));
    m_inverse(0, 0) = d * determinant;
    m_inverse(0, 1) = -b * determinant;
    m_inverse(1, 0) = -c * determinant;
    m_inverse(1, 1) = a * determinant;
    return m_inverse;
  }

  /***
  * Find a logarithmic function in form of "y = a + b * ln(x)" which fits
  * the given points (x_values and y_values).
  * @param std::vector<double> x_values, x values for sample points
  * @param std::vector<double> y_values, y values for sample points
  * @param double theta[2], holds the a and b as output (theta[0] = a and theta[1] = b)
  */
  static void regression(
    const std::vector<double> &x_values,
    const std::vector<double> &y_values,
    double theta[2]) {
    assert(x_values.size() == y_values.size() &&
           "x and y values vectors should have a same size.");
    const int n = x_values.size();

    boost::numeric::ublas::matrix<double> y_m(n, 1);
    for (int i = 0; i < n; i++) {
      y_m(i, 0) = y_values[i];
    }

    boost::numeric::ublas::scalar_matrix<double> sm(n, 2, 1);
    boost::numeric::ublas::matrix<double> x_new_m(sm);
    for (int i = 0; i < n; i++) {
      x_new_m(i, 0) = 1;
      x_new_m(i, 1) = std::log(x_values[i]);
    }
    boost::numeric::ublas::matrix<double> x_new_trans_m = boost::numeric::ublas::trans(
      x_new_m);
    boost::numeric::ublas::matrix<double> x_new_trans_dot_x_new_m = boost::numeric::ublas::prod(
      x_new_trans_m, x_new_m);
    boost::numeric::ublas::matrix<double> temp_1_m = matrix_inverse(
      x_new_trans_dot_x_new_m);
    boost::numeric::ublas::matrix<double> temp_2_m = boost::numeric::ublas::prod(
      x_new_trans_m, y_m);
    boost::numeric::ublas::matrix<double> theta_m = boost::numeric::ublas::prod(
      temp_1_m, temp_2_m);
    theta[0] = theta_m(0, 0);
    theta[1] = theta_m(1, 0);
  }

  /***
  * Finds the x location on a fitted logarithmic curve on sample points where
  * the slope is equal to target_slope
  * @param x_values, x values for sample points
  * @param y_values, y values for sample points
  * @param target_slope, the slope that we are looking for
  * @return the x location where the slope of the curve is target_slope
  */
  static double find_slope_on_curve(
    const std::vector<double> &x_values,
    const std::vector<double> &y_values,
    double target_slope) {
    assert(x_values.size() == y_values.size() &&
           "x and y values vectors should have a same size.");
    assert(target_slope != 0 &&
           "The target slope of zero will result to a inf x, try a nonzero value.");
    assert(target_slope >= 0 &&
           "The target slope for a logarithmic function should be positive.");
    double theta[2];    // theta[0] + theta[1] * ln(x)
    regression(x_values, y_values,
                           theta);     // find the logarithmic function using regression
    double target_x = theta[1] /
                      target_slope;  // find the x where the slope is close to target_slope
    return target_x;
  }

  /***
  * Finds the mu and std parameters of the lognormal distribution from its mode
  * and x boundaries.
  * @param mode, the mode of the distribution.
  * @param min_x, x lower boundary of distribution (zero percentile)
  * @param max_x, x upper boundary of distribution (99th percentile)
  * @param params, holds the calculated distribution parameters (mu and std) as
  * output (params[0] = mu and params[1] = std)
  */
  static void
  find_log_normal_dist_params(double mode, double min_x, double max_x,
                              double params[2]) {
    assert(min_x < max_x && "The min_x should be smaller than max_x");
    assert(mode >= min_x && mode < max_x &&
           "The mode should be between min_x and max_x");
    double max_x_normalized = max_x - min_x;
    double mode_normalized = mode - min_x;
    double std_dev = (-Z_P + std::sqrt(
      Z_P * Z_P + 4 * std::log(max_x_normalized) -
      4 * std::log(mode_normalized))) / 2;
    double mu = std::log(max_x_normalized) - Z_P * std_dev;
    params[0] = mu;
    params[1] = std_dev;
  }
}
