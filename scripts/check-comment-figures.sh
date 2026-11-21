#!/usr/bin/env bash
# check-comment-figures.sh: refuse account figures in Go comments.
#
# The repository is public, so comments and tests describe the mechanism,
# never what an account held. This flags comment lines that carry a dollar
# amount with a thousands separator or of five digits and more ($1,818,
# $25000, $16k), or a space-grouped amount to the cent (22 077.42): the forms
# in which observed balances have slipped in before. A figure that is
# deliberately illustrative says so with the word "synthetic" on the same line.
#
# Usage: scripts/check-comment-figures.sh   (exit 1 and the offending lines on a hit)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

pattern='//.*(\$[0-9]{1,3}(,[0-9]{3})+|\$[0-9]{5,}|\$[0-9]+(\.[0-9]+)?k\b|\b[0-9]{1,3}[  ][0-9]{3}\.[0-9]{2}\b)'

if hits=$(grep -rnE --include='*.go' "$pattern" cmd internal pkg | grep -vi 'synthetic'); then
	echo "$hits"
	echo "account-like figures in comments: describe the mechanism, or mark an illustrative figure \"synthetic\"" >&2
	exit 1
fi
