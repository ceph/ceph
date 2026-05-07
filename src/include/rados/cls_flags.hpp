#pragma once

constexpr int CLS_METHOD_RD      = 0x1; /// method executes read operations
constexpr int CLS_METHOD_WR      = 0x2; /// method executes write operations
constexpr int CLS_METHOD_PROMOTE = 0x8; /// method cannot be proxied to base tier