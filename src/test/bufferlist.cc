#include "include/buffer.h"
#include "include/encoding.h"

#include "gtest/gtest.h"


TEST(BufferList, EmptyAppend) {
	bufferlist bl;
	bufferptr ptr;
	bl.push_back(ptr);
	ASSERT_EQ(bl.begin().end(), 1);
}
