#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

VERSION="$1"

go tool trace -http :8000 "./profiles/$VERSION/trace.out"