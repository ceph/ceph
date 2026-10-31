#!/usr/bin/env bash
set -eu

script="$(dirname "$0")/concurrent.sh"
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
export TEST_SYS="$test_dir/sys/bus/rbd"
export TEST_DEV="$test_dir/dev"
export TEST_IMAGES="$test_dir/images"
# Use real file I/O and comparison, entirely inside the fixture.
sed -e "s@/sys/bus/rbd@$TEST_SYS@g" \
    -e "s@/dev/rbd@$TEST_DEV/rbd@g" "$script" > "$test_dir/concurrent.sh"

wait_for_file() {
	local attempt
	for attempt in $(seq 1 500); do
		[ ! -e "$1" ] || return 0
		sleep 0.01
	done
	printf 'timed out waiting for %s\n' "$1" >&2
	return 1
}

rbd() {
	local image id device
	case "$1" in
		create)
			# An unrelated mapping appears after setup's initial snapshot.
			mkdir -p "$TEST_SYS/devices/999"
			printf 'unrelated-image\n' > "$TEST_SYS/devices/999/name"
			[ "$TEST_MODE" != create_failure ] || return 1
			touch "$TEST_IMAGES/$2"
			;;
		map)
			[ "$TEST_MODE" != map_failure ] || return 1
			[ "$TEST_MODE" != missing_id ] || return 0
			image="$2"
			# Allocate distinct IDs for concurrent fake maps.
			id=$(
				exec 8> "$TEST_SYS/allocator.lock"
				flock -x 8 || exit 1
				id=$(cat "$TEST_SYS/next_id")
				printf '%s\n' "$((id + 1))" > "$TEST_SYS/next_id"
				printf '%s\n' "$id"
			) || return 1
			mkdir "$TEST_SYS/devices/$id"
			printf '%s\n' "$image" > "$TEST_SYS/devices/$id/name"
			truncate -s 16M "$TEST_DEV/rbd$id"
			touch "$TEST_SYS/mapped-$id"
			if [ "$TEST_MODE" = unwritten_peer ] && [ "$id" -eq 0 ]; then
				wait_for_file "$TEST_SYS/mapped-1" || return 1
			fi
			;;
		unmap)
			[ "$TEST_MODE" != unmap_failure ] || return 1
			device="${2##*/}"
			id="${device#rbd}"
			rm -rf "$TEST_SYS/devices/$id"
			rm -f "$TEST_DEV/$device"
			touch "$TEST_SYS/unmapped-$id"
			;;
		ls)
			printf 'unrelated-image\n'
			;;
		rm)
			[ "$2" != unrelated-image ] || {
				touch "$TEST_SYS/unrelated-deleted"
				return 1
			}
			[ -e "$TEST_IMAGES/$2" ] || {
				touch "$TEST_SYS/unrelated-deleted"
				return 1
			}
			[ "$TEST_MODE" != remove_failure ] || return 1
			rm "$TEST_IMAGES/$2"
			;;
		*) return 1 ;;
	esac
}

sudo() {
	[ "$1" = rbd ] || return 1
	shift
	rbd "$@"
}

dd() {
	local arg input="" output=""
	for arg in "$@"; do
		case "$arg" in
			if=*) input="${arg#if=}" ;;
			of=*) output="${arg#of=}" ;;
		esac
	done
	case "$output" in
		"$TEST_DEV"/rbd*)
			[ "$TEST_MODE" != write_failure ] || return 1
			if [ "$TEST_MODE" = unwritten_peer ] && [ "$output" = "$TEST_DEV/rbd1" ]; then
				# Device 1 is mapped but stays unwritten until pass 0 exits.
				wait_for_file "$TEST_SYS/unmapped-0" || return 1
			fi
			command dd "$@" conv=notrunc || return 1
			if [ "$TEST_MODE" = mismatch ]; then
				# Corrupt the fixture; the actual cmp must catch it.
				truncate -s 0 "$output"
				truncate -s 16M "$output"
			fi
			;;
		*)
			case "$input" in
				"$TEST_DEV"/rbd*)
					[ "$TEST_MODE" != read_failure ] || return 1
					if [ "$TEST_MODE" = read_failure_middle ] && [[ " $* " = *" skip=1983 "* ]]; then
						return 1
					fi
					;;
			esac
			command dd "$@"
			;;
	esac
}
# The substituted sysfs path has a different number of components.
cut() {
	if [ "$*" = "-d / -f 6" ]; then
		local path device_dir
		while IFS= read -r path; do
			device_dir="${path%/name}"
			printf '%s\n' "${device_dir##*/}"
		done
	else
		command cut "$@"
	fi
}
export -f wait_for_file rbd sudo dd cut

for mode in create_failure map_failure missing_id write_failure read_failure read_failure_middle mismatch unmap_failure remove_failure success concurrent_success unwritten_peer; do
	export TEST_MODE="$mode"
	rm -rf "$TEST_SYS" "$TEST_DEV" "$TEST_IMAGES"
	mkdir -p "$TEST_SYS/devices" "$TEST_DEV" "$TEST_IMAGES"
	printf '0\n' > "$TEST_SYS/next_id"
	touch "$TEST_IMAGES/unrelated-image"
	count=1
	case "$mode" in
		concurrent_success) count=4 ;;
		unwritten_peer) count=2 ;;
	esac
	if output=$(bash "$test_dir/concurrent.sh" -i 1 -c "$count" -d 0 2>&1); then
		result=0
	else
		result=$?
	fi
	expected=2
	case "$mode" in
		success|concurrent_success|unwritten_peer) expected=0 ;;
	esac
	[ "$result" -eq "$expected" ] || {
		printf '%s: expected %s, got %s\n%s\n' "$mode" "$expected" "$result" "$output" >&2
		exit 1
	}
	[ ! -e "$TEST_SYS/unrelated-deleted" ] &&
	[ -e "$TEST_IMAGES/unrelated-image" ] &&
	[ -e "$TEST_SYS/devices/999/name" ] || {
		printf '%s: cleanup attempted to remove an unrelated image\n' "$mode" >&2
		exit 1
	}
	printf '%s: exit %s\n' "$mode" "$result"
done

# Exercise the lifecycle helpers directly with a reader held inside the
# critical section, and a remover attempting to unmap the same image.
sed '/^parseargs "\$@"/,$d' "$test_dir/concurrent.sh" > "$test_dir/functions.sh"
if output=$(bash -s -- "$test_dir/functions.sh" "$test_dir" 2>&1 <<'LIFECYCLE'
source "$1"
STATE_DIR="$2/state"
mkdir "$STATE_DIR"
printf '0\n' > "$STATE_DIR/image.test.ready"
rbd_read_image() {
	touch "$STATE_DIR/reading"
	wait_for_file "$STATE_DIR/release-reader"
	[ ! -e "$STATE_DIR/unmapped" ]
}
rbd_unmap_image() { touch "$STATE_DIR/unmapped"; }
rbd_destroy_image() { :; }
rbd_read_ready_image image.test &
reader=$!
wait_for_file "$STATE_DIR/reading"
# Start teardown while the read is held. flock -n independently
# confirms that its exclusive lock cannot yet be acquired.
rbd_remove_ready_image image.test 0 &
remover=$!
if flock -n -x "$STATE_DIR/image.test.lock" true; then
	exit 1
fi
[ ! -e "$STATE_DIR/unmapped" ]
touch "$STATE_DIR/release-reader"
wait "$reader"
wait "$remover"
[ -e "$STATE_DIR/unmapped" ]
[ ! -e "$STATE_DIR/image.test.ready" ]
# A reader with a stale discovery must skip the removed image.
rbd_read_image() { return 99; }
rbd_read_ready_image image.test
LIFECYCLE
); then
	printf 'read_unmap_overlap: exit 0\nstale_reader: exit 0\n'
else
	printf 'lifecycle regression failed\n%s\n' "$output" >&2
	exit 1
fi
