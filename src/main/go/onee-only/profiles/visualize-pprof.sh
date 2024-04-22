#!/bin/sh

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <version> <profile>"
    exit 1
fi

VERSION="$1"
PROFILE="$2"

go tool pprof -http :8000 "./profiles/$VERSION/$PROFILE.pprof"