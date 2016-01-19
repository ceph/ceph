#!/bin/bash

ceph mds set allow_new_snaps true --yes-i-really-mean-it

echo "Create dir 100 to 199 ..."
for i in $(seq 100 199); do
	echo "    create dir $i"
	mkdir "$i"
	for y in $(seq 10 20); do
		echo "This is a test file before any snapshot was taken." >"$i/$y"
	done
done

echo "Take first snapshot .snap/test1"
mkdir .snap/test1

echo "Create dir 200 to 299 ..."
for i in $(seq 200 299); do
	echo "    create dir $i"
        mkdir $i
        for y in $(seq 20 29); do
                echo "This is a test file. Created after .snap/test1" >"$i/$y"
        done
done

echo "Create a snapshot in every first level dir ..."
for dir in $(ls); do
	echo "    create $dir/.snap/snap-subdir-test"
	mkdir "$dir/.snap/snap-subdir-test"
	for y in $(seq 30 39); do
		echo "        create $dir/$y file after the snapshot"
                echo "This is a test file. Created after $dir/.snap/snap-subdir-test" >"$dir/$y"
        done
done

echo "Take second snapshot .snap/test2"
mkdir .snap/test2

echo "Copy content of .snap/test1 to copyofsnap1 ..."
mkdir copyofsnap1
cp -Rv .snap/test1 copyofsnap1/


echo "Take third snapshot .snap/test3"
mkdir .snap/test3

echo "Delete the snapshots..."

find ./ -type d -print | \
        xargs -I% -n1 find %/.snap -mindepth 1 -maxdepth 1 \
                         \( ! -name "_*" \) -print 2>/dev/null

find ./ -type d -print | \
	xargs -I% -n1 find %/.snap -mindepth 1 -maxdepth 1 \
                         \( ! -name "_*" \) -print 2>/dev/null | \
	xargs -n1 rmdir

echo "Delete all the files and directories ..."
rm -Rfv ./*

echo OK
