#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

VERSION="$1"

go test -bench . -benchtime 5s -count 4 -cpu 4 > ./profiles/$VERSION/bench.txt

