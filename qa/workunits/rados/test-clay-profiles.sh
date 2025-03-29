#!/bin/bash

# Ceph CLI commands and validation script for CLAY profile corner cases

# Variables
PROFILE_NAME="test_clay_profile"

expect_false()
{
  if "$@"; then
    echo "+++++ expected failure, but got success! cmd: $@"
    exit 1
  else
    echo "+++++ expected failure"
    echo -e "\n\n"
  fi
}

expect_true()
{
  if "$@"; then
    ceph osd erasure-code-profile rm "$PROFILE_NAME" > /dev/null 2>&1 || true
    echo -e "\n\n"
  else
    echo "+++++ function returned non-zero, we failed"
    exit 1
  fi
}

ecp_create_test() {
  local k=$1
  local m=$2
  local d=$3
  local description=$4

  echo "Testing with k=$k, m=$m, d=$d: $description"
  ceph osd erasure-code-profile set "$PROFILE_NAME" \
    plugin=clay \
    k=$k \
    m=$m \
    d=$d
  local result=$?
  if [ $result -ne 0 ]; then
    echo "Failed to create profile with k=$k, m=$m, d=$d"
  fi
  return $result
}

# 1. Boundary Values for d
expect_false ecp_create_test 3 2 3 "Lower boundary (should fail)"
expect_true ecp_create_test 3 2 4 "Exact lower bound (should succeed)"
expect_false ecp_create_test 3 2 5 "Exact upper bound (should failed)"

# 2. Invalid d Values
expect_false ecp_create_test 3 2 -1 "Negative d (should fail)"
expect_false ecp_create_test 3 2 0 "Zero d (should fail)"
expect_false ecp_create_test 3 2 10 "Unreasonably large d (should fail)"

# 3. Invalid Total Number of Chunks
expect_false ecp_create_test 250 5 255 "Exceeds total chunk limit (should fail)"

# 4. Valid but Edge Cases
expect_false ecp_create_test 1 1 2 "Minimum viable configuration (should fail)"
expect_false ecp_create_test 253 1 252 "Maximum viable configuration (should fail)"
expect_true ecp_create_test 10 2 11 "Highly imbalanced configuration (should succeed)"

# 5. Invalid Techniques expected to fail
expect_false ceph osd erasure-code-profile set "$PROFILE_NAME" \
  plugin=clay \
  k=3 \
  m=2 \
  d=4 \
  scalar_mds=isa \
  technique=liber8tion

# 6. Mixed Valid/Invalid Configurations
expect_false ecp_create_test 3 2 0 "Invalid d with valid k and m (should fail)"
expect_false ecp_create_test 0 2 3 "Valid d with invalid k (should fail)"
expect_false ecp_create_test 3 0 4 "Valid d with invalid m (should fail)"


expect_true ecp_create_test 10 5 13 "Valid configuration with padding (should succeed)"
expect_false ecp_create_test 250 4 253 "Invalid configuration exceeding chunk limit (should fail)"
expect_true ecp_create_test 12 3 14 "Configuration without padding (should succeed)"
