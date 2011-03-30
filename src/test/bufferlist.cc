#include "include/buffer.h"
#include "include/encoding.h"

#include "gtest/gtest.h"
#include "stdlib.h"

#define MAX_TEST 1000000


TEST(BufferList, EmptyAppend) {
	bufferlist bl;
	bufferptr ptr;
	bl.push_back(ptr);
	ASSERT_EQ(bl.begin().end(), 1);
}

TEST(BufferList, TestPtrAppend) {
	bufferlist bl;
	char correct[MAX_TEST];
	int curpos = 0;
	int length = random() % 5 > 0 ? random() % 1000 : 0;
	while (curpos + length < MAX_TEST) {
		if (!length) {
			bufferptr ptr;
			bl.push_back(ptr);
		} else {
			char *current = correct + curpos;
			for (int i = 0; i < length; ++i) {
				char next = random() % 255;
				correct[curpos++] = next;
			}
			bufferptr ptr(current, length);
			bl.append(ptr);
		}
		length = random() % 5 > 0 ? random() % 1000 : 0;
	}
	ASSERT_EQ(memcmp(bl.c_str(), correct, curpos), 0);
}

TEST(BufferList, TestDirectAppend) {
	bufferlist bl;
	char correct[MAX_TEST];
	int curpos = 0;
	int length = random() % 5 > 0 ? random() % 1000 : 0;
	while (curpos + length < MAX_TEST) {
		char *current = correct + curpos;
		for (int i = 0; i < length; ++i) {
			char next = random() % 255;
			correct[curpos++] = next;
		}
		bl.append(current, length);
		length = random() % 5 > 0 ? random() % 1000 : 0;
	}
	ASSERT_EQ(memcmp(bl.c_str(), correct, curpos), 0);
}
