#!/usr/bin/env bash
set -euo pipefail

# Arguments identify the PR base repository and branch; reported checks use stdin.
if [[ $# -ne 2 || -z ${1:-} || -z ${2:-} ]]; then
  printf '%s\n' 'Usage: required-checks-pass.sh OWNER/REPO BASE_BRANCH < checks.json' >&2
  exit 2
fi

repository=$1
encoded_base=$(jq -nr --arg base "$2" '$base | @uri')
protection=$(timeout --foreground 30s gh api "repos/$repository/branches/$encoded_base/protection/required_status_checks" </dev/null)
rule_pages=$(timeout --foreground 30s gh api --paginate --slurp "repos/$repository/rules/branches/$encoded_base?per_page=100" </dev/null)

# API failures stop above. Never infer the required set from reported runs.
exec jq -e -s --argjson protection "$protection" --argjson rule_pages "$rule_pages" '
  ($protection |
    type == "object" and
    (has("contexts") or has("checks")) and
    ((.contexts // []) | type == "array") and
    ((.checks // []) | type == "array")
  ) and
  ($rule_pages |
    type == "array" and length > 0 and
    all(.[]; type == "array") and
    all(.[][]; .type | type == "string" and length > 0) and
    (if any(.[][]; .type == "workflows") then
      error("Required workflow rules are unsupported by this status-check gate")
    else true end) and
    all(.[][] | select(.type == "required_status_checks");
      (.parameters | type == "object") and
      (.parameters.required_status_checks |
        type == "array" and
        all(.[];
          type == "object" and
          (.context | type == "string" and length > 0)
        )
      )
    )
  ) and
  length == 1 and
  ((
    ($protection.contexts // []) +
    [($protection.checks // [])[].context] +
    [$rule_pages[][] |
      select(.type == "required_status_checks") |
      .parameters.required_status_checks[].context]
  ) | unique) as $expected |
  ($expected |
    all(.[]; type == "string" and length > 0) and
    index("checks") != null
  ) and
  (.[0] as $reported |
    ($reported | type == "array") and
    all($expected[]; . as $name | any($reported[]; .name == $name)) and
    all($reported[];
        type == "object" and
        (.name | type == "string" and length > 0) and
        .bucket == "pass"
    )
  )
' > /dev/null
