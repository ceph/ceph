#!/usr/bin/env bash
set -eu

script="$(dirname "$0")/concurrent.sh"
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
export TEST_SYS="$test_dir/sys/bus/rbd"
mkdir -p "$TEST_SYS/devices"
# Keep all device discovery inside this fixture, never the host's sysfs.
sed "s@/sys/bus/rbd@$TEST_SYS@g" "$script" > "$test_dir/concurrent.sh"

rbd() {
	case "$1" in
		create)
			mkdir -p "$TEST_SYS/devices/0"
			printf '%s\n' "$2" > "$TEST_SYS/devices/0/name"
			mkdir -p "$TEST_SYS/devices/1"
			printf 'other-image\n' > "$TEST_SYS/devices/1/name"
			if [ "$TEST_MODE" = create_failure ]; then
				return 1
			fi
			;;
		ls)
			printf 'unrelated-image\n'
			[ -z "${NAMES_DIR:-}" ] ||
				find "$NAMES_DIR" -maxdepth 1 -type f -printf '%f\n'
			;;
		rm)
			[ "$2" != unrelated-image ] || {
				printf 'attempted unrelated image removal\n' > "$TEST_SYS/unrelated-deleted"
				return 1
			}
			[ "$TEST_MODE" != remove_failure ] || return 1
			;;
		*) return 1 ;;
	esac
}

sudo() {
	if [ "$2" = map ] && [ "$TEST_MODE" = map_failure ]; then
		return 1
	fi
	return 0
}

dd() {
	case "$*" in
		*"if=/dev/rbd1"*)
			[ "$TEST_MODE" != read_failure ]
			return
			;;
		*"/dev/rbd"*) return 0 ;;
	esac
	command dd "$@"
}

cmp() {
	[ "$TEST_MODE" != mismatch ]
}

cut() {
	if [ "$*" = "-d / -f 6" ]; then
		while IFS= read -r path; do
			device_dir="${path%/name}"
			printf '%s\n' "${device_dir##*/}"
		done
	else
		command cut "$@"
	fi
}
export -f rbd sudo dd cmp cut

for mode in create_failure map_failure read_failure mismatch remove_failure success; do
	export TEST_MODE="$mode"
	rm -rf "$TEST_SYS/devices"
	rm -f "$TEST_SYS/unrelated-deleted"
	mkdir -p "$TEST_SYS/devices"
	if output=$(bash "$test_dir/concurrent.sh" -i 1 -c 1 -d 0 2>&1); then
		result=0
	else
		result=$?
	fi
	if [ "$mode" = success ]; then
		[ "$result" -eq 0 ] || {
			printf '%s: expected success, got %s\n%s\n' "$mode" "$result" "$output" >&2
			exit 1
		}
	else
		[ "$result" -ne 0 ] || {
			printf '%s: operation failed but workunit exited 0\n' "$mode" >&2
			exit 1
		}
	fi
	[ ! -e "$TEST_SYS/unrelated-deleted" ] || {
		printf '%s: cleanup attempted to remove an unrelated image\n' "$mode" >&2
		exit 1
	}
	printf '%s: exit %s\n' "$mode" "$result"
done
