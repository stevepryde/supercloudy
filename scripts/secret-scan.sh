#!/usr/bin/env bash
# Scan regular working-tree files, the Git index, and reachable history.
# Only locations are reported; matching contents are never printed.
set -euo pipefail

root=${1:-"$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"}
cd -- "$root"
failed=0

for command in git find awk mktemp; do
    command -v "$command" >/dev/null || { printf 'Missing tool: %s\n' "$command" >&2; exit 2; }
done
paths=$(mktemp)
trap 'rm -f -- "$paths"' EXIT

credential_path() {
    case "/$1" in
        */.env*|*/credentials|*/credentials.*|*credentials.json|*service-account*.json|\
        *.pem|*.key|*.p12|*.pfx|*/id_rsa*|*/id_ed25519*|*/.netrc|*/.npmrc)
            return 0 ;;
        *) return 1 ;;
    esac
}

scan_content() {
    LC_ALL=C awk '
    {
        line = $0
        hit = line ~ /(AKIA|ASIA)[A-Z0-9]{16}/ ||
              line ~ /-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY-----/ ||
              line ~ /gh[pousr]_[A-Za-z0-9]{36,}/ ||
              line ~ /github_pat_[A-Za-z0-9_]{50,}/ ||
              line ~ /xox[baprs]-[A-Za-z0-9-]{20,}/ ||
              line ~ /AIza[A-Za-z0-9_-]{35}/
        # Literal credential assignments. The only permitted literal is test.
        assignment = "(aws_access_key_id|aws_secret_access_key|aws_session_token|api_key|api_token|access_token|auth_token|client_secret|password)[\"\047]?[[:space:]]*[:=][[:space:]]*"
        remaining = line
        while (match(tolower(remaining), assignment)) {
            remaining = substr(remaining, RSTART + RLENGTH)
            quote = substr(remaining, 1, 1)
            if (quote == "\"" || quote == "\047") {
                remaining = substr(remaining, 2)
                closing = index(remaining, quote)
                value = closing ? substr(remaining, 1, closing - 1) : remaining
                remaining = closing ? substr(remaining, closing + 1) : ""
            } else {
                value = remaining
                sub(/[[:space:],;].*$/, "", value)
                remaining = substr(remaining, length(value) + 1)
            }
            if (value != "test") hit = 1
        }
        if (hit) { printf "  line %d: possible secret [REDACTED]\n", NR; found = 1 }
    }
    END { exit found ? 1 : 0 }
    '
}

find . -type d \( -name .git -o -name target \) -prune -o -type f ! -name .git -print0 > "$paths"
while IFS= read -r -d '' path; do
    if credential_path "$path"; then
        printf 'Forbidden credential file: %s\n' "$path" >&2
        failed=1
    fi
    if ! locations=$(scan_content < "$path"); then
        printf 'Possible secret in tree: %s\n%s\n' "$path" "$locations" >&2
        failed=1
    fi
done < "$paths"

# A source archive can be scanned without Git metadata. In a checkout, inspect
# every reachable commit, including secrets deleted from the current tree.
if [ -e .git ]; then
    git ls-files -z > "$paths"
    while IFS= read -r -d '' path; do
        if credential_path "$path"; then
            printf 'Forbidden credential file in index: %s\n' "$path" >&2
            failed=1
        fi
        if ! locations=$(git show ":$path" | scan_content); then
            printf 'Possible secret in index: %s\n%s\n' "$path" "$locations" >&2
            failed=1
        fi
    done < "$paths"
    revisions=$(git rev-list --all)
    while IFS= read -r revision; do
        [ -n "$revision" ] || continue
        git ls-tree -r --name-only -z "$revision" > "$paths"
        while IFS= read -r -d '' path; do
            if credential_path "$path"; then
                printf 'Forbidden credential file in history: %s:%s\n' "$revision" "$path" >&2
                failed=1
            fi
            if ! locations=$(git show "$revision:$path" | scan_content); then
                printf 'Possible secret in history: %s:%s\n%s\n' "$revision" "$path" "$locations" >&2
                failed=1
            fi
        done < "$paths"
    done <<< "$revisions"
fi

if [ "$failed" -ne 0 ]; then
    exit 1
fi
printf 'Secret scan passed (tree, index and available Git history).\n'
