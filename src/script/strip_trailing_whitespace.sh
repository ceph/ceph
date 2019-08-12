#!/bin/sh

sed -i 's/[ \t]*$//' $1
sed -i 's/^        /\t/' $1
