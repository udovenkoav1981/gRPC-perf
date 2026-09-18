#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
flatc --go -o generated schema/request.fbs schema/response.fbs
