#!/bin/sh

CXX=$1

shift

echo "#include <utility>
#if defined(_LIBCPP_VERSION)
#define MYRESULT libc++
#elif defined(__GLIBCXX__)
#define MYRESULT libstdc++
#else
#define MYRESULT unknown
#endif

HelloFriendsTheAnsWerIs MYRESULT" | ${CXX} -E -xc++ $* - | \
    grep "HelloFriendsTheAnsWerIs" | cut -f 2 -d ' ' | tr -d '\n'
