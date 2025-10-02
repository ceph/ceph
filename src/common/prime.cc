/*
 * Copyright (C) 2025 IBM
 */

#include "common/prime.h"

//This is not a performance implementation
//This is a function that will return true if the int n is a prime number and false if not.
bool is_prime(int n) {
    if (n <= 1) return false;
    for (int i = 2; i*i <= n; ++i) {
        if (n % i == 0) {
          return false;
        }
    }
    return true;
}