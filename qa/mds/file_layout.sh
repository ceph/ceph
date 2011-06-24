#!/bin/sh -ex

MB=1048576
twoMB=$((2*MB))

mkdir layout_test
./cephfs layout_test show_layout
./cephfs layout_test set_layout -u $MB -c 1 -s $MB
touch layout_test/file1
./cephfs layout_test/file1 show_layout
`echo "hello, I'm a file" > layout_test/file1`
./cephfs layout_test/file1 show_layout
touch layout_test/file2
./cephfs layout_test/file2 show_layout
./cephfs layout_test/file2 set_layout -u $MB -c 2 -s $twoMB
./cephfs layout_test/file2 show_layout