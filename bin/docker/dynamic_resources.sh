#!/usr/bin/env bash
set -e

MYPATH="$(dirname "$0")"

function get_heap_size {
  # Get the max heap used by a jvm which used all the ram available to the container
  CONTAINER_MEMORY_IN_BYTES=$(java -XshowSettings:vm -version \
    |& awk '/Max\. Heap Size \(Estimated\): [0-9KMG]+/{ print $5}' \
    | gawk -f "${MYPATH}"/to_bytes.gawk)

  # use max of 31G memory, java performs much better with Compressed Ordinary Object Pointers
  DEFAULT_MEMORY_CEILING=$((31 * 2**30))
  if [ "${CONTAINER_MEMORY_IN_BYTES}" -lt "${DEFAULT_MEMORY_CEILING}" ]; then
    if [ -z $CONTAINER_HEAP_PERCENT ]; then
      CONTAINER_HEAP_PERCENT=0.50
    fi

    CONTAINER_MEMORY_IN_MB=$((${CONTAINER_MEMORY_IN_BYTES}/1024**2))
    CONTAINER_HEAP_MAX=$(echo "${CONTAINER_MEMORY_IN_MB} ${CONTAINER_HEAP_PERCENT}" | awk '{ printf "%d", $1 * $2 }')

    echo "${CONTAINER_HEAP_MAX}"
  fi
}