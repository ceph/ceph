#!/bin/sh

watch "pwd ; echo \`teuthology-ls --archive-dir . | grep -c pass\` passes ; teuthology-ls --archive-dir . | grep -v pass"

