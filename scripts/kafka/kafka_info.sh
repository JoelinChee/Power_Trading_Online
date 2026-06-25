#!/usr/bin/env bash

set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

usage() {
    cat <<EOF
Usage: $(basename "$0") RECORDING.bin

Show basic information for a kafka_record.py bin recording.

Environment:
    PYTHON_BIN   Python interpreter used for parsing. Default: python3
EOF
}

if [[ $# -ne 1 || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

RECORDING="$1"
if [[ ! -f "$RECORDING" ]]; then
    echo "Recording file not found: $RECORDING" >&2
    exit 1
fi

echo "File: $RECORDING"
ls -lh "$RECORDING"
"$PYTHON_BIN" - "$RECORDING" <<'PY'
from __future__ import annotations

import json
import struct
import sys
from collections import Counter
from pathlib import Path

MAGIC = b"PTO_KAFKA_BIN_V1\n"
NULL_BYTES_LENGTH = -1


def read_exact(fp, size: int) -> bytes:
    data = fp.read(size)
    if len(data) != size:
        raise EOFError("unexpected end of recording")
    return data


def skip_bytes(fp) -> None:
    length = struct.unpack(">i", read_exact(fp, 4))[0]
    if length == NULL_BYTES_LENGTH:
        return
    if length < 0:
        raise ValueError(f"invalid byte field length: {length}")
    read_exact(fp, length)


path = Path(sys.argv[1])
counts: Counter[str] = Counter()
preview: list[dict[str, object]] = []
total = 0

with path.open("rb") as fp:
    if read_exact(fp, len(MAGIC)) != MAGIC:
        raise SystemExit("Unsupported recording format; expected PTO_KAFKA_BIN_V1")
    while True:
        header_length_data = fp.read(4)
        if not header_length_data:
            break
        if len(header_length_data) != 4:
            raise EOFError("truncated record header length")
        header_length = struct.unpack(">I", header_length_data)[0]
        header = json.loads(read_exact(fp, header_length).decode("utf-8"))
        skip_bytes(fp)
        skip_bytes(fp)

        topic = str(header.get("topic", ""))
        counts[topic] += 1
        total += 1
        if len(preview) < 10:
            preview.append(header)

print(f"Bytes: {path.stat().st_size}")
print(f"Records: {total}")
print()
print("Topics:")
for topic, count in sorted(counts.items()):
    print(f"  {count:8d}  {topic}")
print()
print("Preview:")
for item in preview:
    print(
        "  {topic}[{partition}] offset={offset} timestamp={timestamp}".format(
            topic=item.get("topic", ""),
            partition=item.get("partition", ""),
            offset=item.get("offset", ""),
            timestamp=item.get("timestamp", ""),
        )
    )
PY