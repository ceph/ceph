// RGW CLS: Independent Catch2 entry point for focused wire tests.
#include <catch2/catch_session.hpp>

int main(const int argc, char *argv[])
{
    return Catch::Session {}.run(argc, argv);
}
