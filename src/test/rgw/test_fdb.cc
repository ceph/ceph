
#include <catch2/catch_config.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#define CATCH_CONFIG_MAIN

#include "rgw/fdb/fdb.h"

TEST_CASE("fdb basic", "[rgw][fdb]") {

 REQUIRE_THROWS_AS([] { throw ceph::libfdb::fdb_exception(0); }(),
                   ceph::libfdb::fdb_exception);
}

