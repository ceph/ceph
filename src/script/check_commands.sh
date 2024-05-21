#!/bin/sh

git grep -e COMMAND\( -e COMMAND_WITH_FLAG\( | grep -o "(\"[a-zA-Z ]*\"" | grep -o "[a-zA-Z ]*" | sort | uniq > commands.txt
missing_test=false
good_tests=""
bad_tests=""
while read cmd; do
    if git grep -q "$cmd" -- src/test qa/; then
	good_tests="$good_tests '$cmd'"
    else
	echo "'$cmd' has no apparent tests"
	missing_test=true
	bad_tests="$bad_tests '$cmd'"
    fi
done < commands.txt

if [ "$missing_test" == true ]; then
    echo "Missing tests!" $bad_tests
    exit 1;
fi
