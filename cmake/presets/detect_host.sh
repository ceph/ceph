#!/usr/bin/env bash
# detect_host.sh - platform-specific CMake settings shared by do_cmake.sh and presets.
#
# Source this file, then call detect_host_settings to populate variables or
# write_host_json / build_host_cmake_args to consume them.

detect_host_settings() {
  PYBUILD="3"
  CXX_COMPILER="g++"
  C_COMPILER="gcc"
  WITH_SCCACHE=""
  WITH_CCACHE=""
  HOST_RADOSGW_AMQP_ENDPOINT=""
  HOST_RADOSGW_KAFKA_ENDPOINT=""

  if [ -r /etc/os-release ]; then
    # shellcheck disable=SC1091
    source /etc/os-release
    case "$ID" in
      fedora)
        if [ "$VERSION_ID" -ge "43" ]; then
          PYBUILD="3.14"
        elif [ "$VERSION_ID" -ge "41" ]; then
          PYBUILD="3.13"
        elif [ "$VERSION_ID" -ge "39" ]; then
          PYBUILD="3.12"
        else
          PYBUILD="3.11"
        fi
        ;;
      almalinux|rocky|rhel|centos)
        MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
        if [ "$MAJOR_VER" -ge "10" ]; then
          PYBUILD="3.12"
        elif [ "$MAJOR_VER" -ge "9" ]; then
          PYBUILD="3.9"
        elif [ "$MAJOR_VER" -ge "8" ]; then
          PYBUILD="3.6"
        fi
        ;;
      opensuse*|suse|sles)
        PYBUILD="3"
        HOST_RADOSGW_AMQP_ENDPOINT="OFF"
        HOST_RADOSGW_KAFKA_ENDPOINT="OFF"
        ;;
      ubuntu)
        MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
        if [ "$MAJOR_VER" -ge "26" ]; then
          PYBUILD="3.14"
        elif [ "$MAJOR_VER" -ge "24" ]; then
          PYBUILD="3.12"
        elif [ "$MAJOR_VER" -ge "22" ]; then
          PYBUILD="3.10"
        fi
        ;;
    esac
  elif [ "$(uname)" = "FreeBSD" ]; then
    PYBUILD="3"
    HOST_RADOSGW_AMQP_ENDPOINT="OFF"
    HOST_RADOSGW_KAFKA_ENDPOINT="OFF"
  else
    echo "Unknown release" >&2
    return 1
  fi

  if type sccache > /dev/null 2>&1; then
    WITH_SCCACHE="ON"
  elif type ccache > /dev/null 2>&1; then
    WITH_CCACHE="ON"
  fi

  for i in $(seq 20 -1 11); do
    if type -t "gcc-$i" > /dev/null; then
      CXX_COMPILER="g++-$i"
      C_COMPILER="gcc-$i"
      break
    fi
  done
}

_host_cache_var_lines() {
  detect_host_settings

  local pairs=(
    "WITH_PYTHON3=${PYBUILD}"
    "CMAKE_CXX_COMPILER=${CXX_COMPILER}"
    "CMAKE_C_COMPILER=${C_COMPILER}"
  )

  if [ -n "$WITH_SCCACHE" ]; then
    pairs+=("WITH_SCCACHE=${WITH_SCCACHE}")
  elif [ -n "$WITH_CCACHE" ]; then
    pairs+=("WITH_CCACHE=${WITH_CCACHE}")
  fi
  if [ -n "$HOST_RADOSGW_AMQP_ENDPOINT" ]; then
    pairs+=("WITH_RADOSGW_AMQP_ENDPOINT=${HOST_RADOSGW_AMQP_ENDPOINT}")
  fi
  if [ -n "$HOST_RADOSGW_KAFKA_ENDPOINT" ]; then
    pairs+=("WITH_RADOSGW_KAFKA_ENDPOINT=${HOST_RADOSGW_KAFKA_ENDPOINT}")
  fi

  local first=true
  for pair in "${pairs[@]}"; do
    local key="${pair%%=*}"
    local value="${pair#*=}"
    if $first; then
      first=false
    else
      echo ","
    fi
    printf '        "%s": "%s"' "$key" "$value"
  done
  echo
}

write_host_json() {
  local outfile="${1:?output file required}"
  local cache_vars
  cache_vars=$(_host_cache_var_lines)

  mkdir -p "$(dirname "$outfile")"
  cat >"$outfile" <<EOF
{
  "version": 10,
  "configurePresets": [
    {
      "name": "_host",
      "hidden": true,
      "cacheVariables": {
${cache_vars}
      }
    }
  ]
}
EOF
}

build_host_cmake_args() {
  detect_host_settings

  local args=""
  args+=" -DWITH_PYTHON3=${PYBUILD}"
  if [ -n "$WITH_SCCACHE" ]; then
    args+=" -DWITH_SCCACHE=${WITH_SCCACHE}"
  elif [ -n "$WITH_CCACHE" ]; then
    args+=" -DWITH_CCACHE=${WITH_CCACHE}"
  fi
  args+=" -DCMAKE_CXX_COMPILER=${CXX_COMPILER}"
  args+=" -DCMAKE_C_COMPILER=${C_COMPILER}"
  if [ -n "$HOST_RADOSGW_AMQP_ENDPOINT" ]; then
    args+=" -DWITH_RADOSGW_AMQP_ENDPOINT=${HOST_RADOSGW_AMQP_ENDPOINT}"
  fi
  if [ -n "$HOST_RADOSGW_KAFKA_ENDPOINT" ]; then
    args+=" -DWITH_RADOSGW_KAFKA_ENDPOINT=${HOST_RADOSGW_KAFKA_ENDPOINT}"
  fi
  echo "$args"
}

bootstrap_local_preset_file() {
  local target="$1"
  local example="$2"

  if [ -f "$target" ] || [ ! -f "$example" ]; then
    return 0
  fi

  cp "$example" "$target"
  echo "Created ${target} from ${example}" >&2
}

bootstrap_local_presets() {
  local dir="${1:?preset directory required}"

  bootstrap_local_preset_file "${dir}/user.json" "${dir}/user.json.example"
  bootstrap_local_preset_file "${dir}/host.json" "${dir}/host.json.example"
}
