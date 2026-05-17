#!/usr/bin/env bash
# metrics.sh — métriques structurelles ad hoc pour la revue de code.
#
# Ce qu'il fait :
#   1. Littéraux de chaîne dupliqués (proxy SonarQube S1192) — approximation
#      regex, hors *_test.go et *.pb.go.
#   2. Signatures de fonctions multi-lignes (proxy S107 "trop de paramètres" —
#      une signature qui déborde sur plusieurs lignes a presque toujours
#      beaucoup de paramètres).
#   3. Fonctions > 80 lignes (proxy taille).
#
# Approximations basées regex (pas d'AST). À recouper manuellement.
# Rejouer :  bash doc/codereview/scripts/metrics.sh
set -u
cd "$(dirname "$0")/../../.." || exit 1

mapfile -t FILES < <(find cmd internal pkg -name '*.go' -not -name '*_test.go' -not -name '*.pb.go' 2>/dev/null)

echo "### 1. Littéraux de chaîne dupliqués (>=6 occurrences, longueur >=8) ###"
grep -hoE '"[^"]{8,}"' "${FILES[@]}" | sort | uniq -c | sort -rn | awk '$1>=6' | head -40

echo
echo "### 2. Signatures de fonctions multi-lignes (proxy 'trop de paramètres') ###"
grep -rnE '^func .*[(,]$' "${FILES[@]}" 2>/dev/null

echo
echo "### 3. Fonctions de plus de 80 lignes ###"
for f in "${FILES[@]}"; do
  awk -v F="$f" '
    /^func / { name=$0; start=NR; depth=0; inb=0 }
    /^func / || inb {
      o=gsub(/{/,"{"); c=gsub(/}/,"}"); depth+=o-c
      if (o>0) inb=1
      if (inb && depth<=0) { if (NR-start>80) printf "%5d  %s:%d\n", NR-start, F, start; inb=0 }
    }
  ' "$f"
done | sort -rn | head -30
