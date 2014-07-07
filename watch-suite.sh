#!/bin/sh

watch "pwd ; echo \`teuthology-ls . | grep -c pass\` passes ; teuthology-ls . | grep -v pass"

