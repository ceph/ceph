#include <errno.h>

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include <fmt/format.h>

#include "test/client/TestClient.h"


TEST_F(TestClient, SingleTargetMdsCommand) {
    auto mds_spec = "a";
    auto cmd = "{\"prefix\": \"session ls\", \"format\": \"json\"}";
    bufferlist inbl;
    bufferlist outbl;
    std::string outs;
    std::vector<std::string> cmdv;
    C_SaferCond cond;

    cmdv.push_back(cmd);
    int r = client->mds_command(mds_spec, cmdv, inbl, &outbl, &outs, &cond);
    r = cond.wait();

    std::cout << "SingleTargetMdsCommand: " << outbl.c_str() << std::endl;

    ASSERT_TRUE(r == 0 || r == -38);
}

TEST_F(TestClient, MultiTargetMdsCommand) {
    auto mds_spec = "*";
    auto cmd = "{\"prefix\": \"session ls\", \"format\": \"json\"}";
    bufferlist inbl;
    bufferlist outbl;
    std::string outs;
    std::vector<std::string> cmdv;
    C_SaferCond cond;

    cmdv.push_back(cmd);
    std::cout << "MultiTargetMds: " << std::endl;
    int r = client->mds_command(mds_spec, cmdv, inbl, &outbl, &outs, &cond);
    r = cond.wait();

    std::cout << "MultiTargetMdsCommand: " << outbl.c_str() << std::endl;

    ASSERT_TRUE(r == 0 || r == -38);
}
