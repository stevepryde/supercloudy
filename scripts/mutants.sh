#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$root"

if ! version=$(cargo mutants --version 2>/dev/null) || [ "$version" != 'cargo-mutants 26.2.0' ]; then
    printf 'Install the supported mutation tool: cargo install cargo-mutants --version 26.2.0 --locked\n' >&2
    exit 1
fi

args=()
output=''
listing=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output|-o)
            if [ "$#" -lt 2 ] || [ -z "$2" ]; then
                printf '%s requires an output directory.\n' "$1" >&2
                exit 1
            fi
            output=$2
            shift 2
            ;;
        --output=*) output=${1#*=}; shift ;;
        -o?*) output=${1#-o}; shift ;;
        --list|--list-files|--help|-h|--version|--emit-schema|--emit-schema=*)
            listing=1
            args+=("$1")
            shift
            ;;
        --)
            args+=("$@")
            break
            ;;
        *) args+=("$1"); shift ;;
    esac
done

if [ "$listing" -eq 1 ]; then
    if [ -n "$output" ]; then
        exec cargo mutants --output "$output" "${args[@]}"
    fi
    exec cargo mutants "${args[@]}"
fi

# Logs contain mutated source, including deliberately invalid synthetic credentials.
# The repository scanner checks ignored files too, so keep all output outside it.
if [ -z "$output" ]; then
    output=$(mktemp -d /tmp/supercloudy-mutants.XXXXXX)
else
    mkdir -p -- "$output"
    output=$(cd -- "$output" && pwd -P)
fi
case "$output/" in
    "$root/"*) printf 'Mutation output must be outside the repository.\n' >&2; exit 1 ;;
esac
printf 'Mutation results: %s/mutants.out\n' "$output" >&2

# Avoid serial no-argument CI runs; explicit -j or environment settings win.
export CARGO_MUTANTS_JOBS="${CARGO_MUTANTS_JOBS:-4}"

status=0
cargo mutants --output "$output" "${args[@]}" || status=$?
# Only 2 means missed mutants in cargo-mutants 26.2.0. Preserve all other
# statuses, including successful empty --in-diff runs that produce no report,
# timeouts (3), baseline failures (4), invalid arguments/diffs and signals.
if [ "$status" -ne 2 ]; then
    exit "$status"
fi

missed=$output/mutants.out/missed.txt
if [ ! -f "$missed" ]; then
    printf 'cargo-mutants did not produce %s\n' "$missed" >&2
    exit 1
fi

# Match whole descriptions literally, including location and replacement. Reasons
# permit reviewable equivalent mutants without excluding them from future runs.
if ! awk -F '\t' '
    FILENAME == ARGV[1] {
        if ($0 ~ /^[[:space:]]*(#|$)/) next
        if ($1 == "" || NF < 2 || $2 !~ /[^[:space:]]/) {
            printf "Missing survivor reason at %s:%d\n", FILENAME, FNR > "/dev/stderr"
            invalid = 1
            next
        }
        reasoned[$1] = 1
        next
    }
    NF && !($0 in reasoned) {
        printf "Unreasoned survivor: %s\n", $0 > "/dev/stderr"
        missed = 1
    }
    END { exit invalid || missed }
' "$root/mutants-survivors.txt" "$missed"; then
    exit 2
fi
