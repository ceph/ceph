#!/bin/bash
# Clean up test buckets left over from previous runs
S3TEST_CONF="${S3TEST_CONF:-../s3tests.conf}"
export S3TEST_CONF
cargo run --example cleanup 2>&1
