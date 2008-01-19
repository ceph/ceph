#!/bin/sh

cat $1 | sed 's/[ \t]*$//' > $1.new && mv $1.new $1
