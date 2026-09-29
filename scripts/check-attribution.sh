#!/usr/bin/env bash
# Reject AI-assistant attribution in commits and pull requests.
#
#   check-attribution.sh commits RANGE   every commit in RANGE (A..B, or a rev)
#   check-attribution.sh text FILE       PR title/description text ("-" = stdin)
#
# Fails on a trailer (Co-authored-by, Signed-off-by, ...) or a
# "Generated with/by" line that names an AI tool, on an AI session trailer or
# link, and on a commit author or committer that is an AI tool. Plain prose
# that happens to mention a tool (say, a file named CLAUDE.md) is allowed.
set -euo pipefail

fail() {
    echo "attribution check: $*" >&2
    exit 1
}

# Whole-word, case-insensitive. Extend this list rather than special-casing.
ai='claude|anthropic|chatgpt|openai|gpt(-?[0-9][a-z0-9.]*)?|codex|copilot|gemini|bard|grok|llama|mistral|deepseek|qwen|cursor|devin|aider|cody|tabnine|codewhisperer|windsurf|sol|astra|luna'
ai_word="(^|[^[:alnum:]])($ai)([^[:alnum:]]|$)"
ai_domain='@([a-z0-9-]+\.)*(anthropic\.com|openai\.com|cursor\.(sh|com)|devin\.ai|x\.ai)>?$'
trailer='^[[:space:]]*[A-Za-z]+(-[A-Za-z]+)*-by[[:space:]]*:'
session='(claude\.ai|chatgpt\.com|chat\.openai\.com|gemini\.google\.com|copilot\.microsoft\.com)/'

# Print each offending line of stdin, prefixed with $1.
scan_lines() {
    local label="$1"
    grep -inE -- "($trailer.*$ai_word)|($trailer.*$ai_domain)|(generated[[:space:]]+(with|by).*$ai_word)|(^[[:space:]]*($ai)-[A-Za-z-]+[[:space:]]*:)|$session" \
        | sed "s|^|$label: line |" || true
}

# AI bot accounts (copilot-swe-agent[bot], ...) match by name; other bots pass.
identity_is_ai() {
    grep -qiE -- "$ai_word|$ai_domain" <<<"$1"
}

mode=${1:-}
case "$mode" in
    commits)
        test $# -eq 2 || fail "usage: $0 commits RANGE"
        range="$2"
        # A push that rewrote history, or created the branch, may name a
        # "before" commit that is absent or all zeros. Check everything
        # reachable from the new tip instead; that is only stricter.
        if [[ "$range" == *..* ]]; then
            left=${range%%..*}
            right=${range##*..}
            if [[ "$left" =~ ^0+$ ]] || ! git cat-file -e "$left^{commit}" 2>/dev/null; then
                range="$right"
            fi
        fi
        commits=$(git rev-list "$range") || fail "cannot list commits in $range"
        bad=0
        checked=0
        for commit in $commits; do
            checked=$((checked + 1))
            short=$(git rev-parse --short "$commit")
            hits=$(git log -1 --format=%B "$commit" | scan_lines "$short message")
            for who in "$(git log -1 --format='%an <%ae>' "$commit")" "$(git log -1 --format='%cn <%ce>' "$commit")"; do
                if identity_is_ai "$who"; then
                    hits+=$'\n'"$short identity: $who"
                fi
            done
            hits=$(sed '/^$/d' <<<"$hits")
            if test -n "$hits"; then
                echo "$hits" >&2
                bad=$((bad + 1))
            fi
        done
        test "$bad" -eq 0 || fail "$bad of $checked commit(s) carry AI attribution"
        echo "attribution check passed: $checked commit(s) in $range"
        ;;
    text)
        test $# -eq 2 || fail "usage: $0 text FILE"
        hits=$(cat -- "$2" | scan_lines "text")
        if test -n "$hits"; then
            echo "$hits" >&2
            fail "the pull request title or description carries AI attribution"
        fi
        echo "attribution check passed: pull request title and description"
        ;;
    *)
        fail "usage: $0 commits RANGE | text FILE"
        ;;
esac
