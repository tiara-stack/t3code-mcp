#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_dir=$(mktemp -d /tmp/required-checks-test.XXXXXX)
trap 'rm -rf "$test_dir"' EXIT

cat >"$test_dir/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CI_GATE_TEST_CALLS"
case "$*" in
  *'/protection/required_status_checks')
    printf '%s' "$CI_GATE_TEST_PROTECTION"
    exit "${CI_GATE_TEST_PROTECTION_STATUS:-0}"
    ;;
  *'/rules/branches/'*)
    printf '%s' "$CI_GATE_TEST_RULES"
    exit "${CI_GATE_TEST_RULES_STATUS:-0}"
    ;;
  *) exit 2 ;;
esac
STUB
chmod +x "$test_dir/gh"
export PATH="$test_dir:$PATH"
export CI_GATE_TEST_CALLS="$test_dir/calls"
export CI_GATE_TEST_PROTECTION='{"contexts":["checks"],"checks":[{"context":"checks"}]}'
export CI_GATE_TEST_RULES='[[]]'
passed='[{"name":"checks","bucket":"pass"}]'
case_count=0

expect_gate() {
  local expected=$1 name=$2 payload=$3 actual=blocked
  if bash "$script_dir/required-checks-pass.sh" owner/repo 'topic/one' \
    <<<"$payload" >"$test_dir/output" 2>"$test_dir/error"; then
    actual=pass
  fi
  if [[ "$actual" != "$expected" ]]; then
    printf 'FAIL: %s: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    cat "$test_dir/error" >&2
    exit 1
  fi
  case_count=$((case_count + 1))
}

expect_gate pass 'configured job passes' "$passed"
expect_gate pass 'duplicate runs pass' '[{"name":"checks","bucket":"pass"},{"name":"checks","bucket":"pass"}]'
expect_gate pass 'additional reported job passes' '[{"name":"checks","bucket":"pass"},{"name":"security","bucket":"pass"}]'
expect_gate blocked 'missing checks' '[{"name":"security","bucket":"pass"}]'
expect_gate blocked 'no reported jobs' '[]'
expect_gate blocked 'empty output' ''
expect_gate blocked 'malformed JSON' '[invalid'
expect_gate blocked 'wrong JSON shape' '{}'
expect_gate blocked 'missing result name' '[{"name":"checks","bucket":"pass"},{"bucket":"pass"}]'
expect_gate blocked 'multiple JSON documents' "[] $passed"
for bucket in fail cancel pending skipping unknown; do
  expect_gate blocked "duplicate run is $bucket" "[{\"name\":\"checks\",\"bucket\":\"pass\"},{\"name\":\"checks\",\"bucket\":\"$bucket\"}]"
done
expect_gate blocked 'additional reported failure' '[{"name":"checks","bucket":"pass"},{"name":"security","bucket":"fail"}]'

CI_GATE_TEST_PROTECTION='{"contexts":["checks","security"]}'
expect_gate blocked 'unreported classic required job' "$passed"
expect_gate pass 'all classic required jobs pass' '[{"name":"checks","bucket":"pass"},{"name":"security","bucket":"pass"}]'
CI_GATE_TEST_PROTECTION='{"checks":[{"context":"checks"},{"context":"security"}]}'
expect_gate blocked 'unreported app-bound required job' "$passed"
CI_GATE_TEST_PROTECTION='{"contexts":["checks"]}'
CI_GATE_TEST_RULES='[[],[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"security"}]}}]]'
expect_gate blocked 'unreported ruleset job on later page' "$passed"
expect_gate pass 'all ruleset required jobs pass' '[{"name":"checks","bucket":"pass"},{"name":"security","bucket":"pass"}]'
CI_GATE_TEST_RULES='[[{"type":"workflows","parameters":{"workflows":[{"path":".github/workflows/audit.yml","repository_id":1}]}}]]'
expect_gate blocked 'required workflow cannot be validated by check names' "$passed"
CI_GATE_TEST_RULES='[[{"type":"deletion"}]]'
expect_gate pass 'non-CI branch policy does not change check requirements' "$passed"

CI_GATE_TEST_RULES='[[]]'
for protection in '{}' 'null' '{"contexts":[]}' '{"contexts":[null]}' '{"contexts":"checks"}' 'invalid'; do
  CI_GATE_TEST_PROTECTION=$protection
  expect_gate blocked 'invalid or missing configured names' "$passed"
done
CI_GATE_TEST_PROTECTION='{"contexts":["checks"]}'
for rules in \
  '[]' \
  '{}' \
  '[[{}]]' \
  '[[{"type":"required_status_checks","parameters":{}}]]' \
  '[[{"type":"required_status_checks","parameters":{"required_status_checks":{}}}]]' \
  '[[{"type":"required_status_checks","parameters":{"required_status_checks":{"unexpected":{"context":"checks"}}}}]]' \
  '[[{"type":"required_status_checks","parameters":[]}]]' \
  '[[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":""}]}}]]' \
  '[[{"type":"required_status_checks","parameters":{"required_status_checks":[null]}}]]' \
  'invalid'; do
  CI_GATE_TEST_RULES=$rules
  expect_gate blocked 'invalid rules response' "$passed"
done
CI_GATE_TEST_RULES='[[{"type":"required_status_checks","parameters":{"required_status_checks":[]}}]]'
expect_gate pass 'well-formed rule with no additional required names' "$passed"
CI_GATE_TEST_RULES='[[]]'
export CI_GATE_TEST_PROTECTION_STATUS=1
expect_gate blocked 'protection API error' "$passed"
unset CI_GATE_TEST_PROTECTION_STATUS
export CI_GATE_TEST_RULES_STATUS=124
expect_gate blocked 'rules API timeout' "$passed"
unset CI_GATE_TEST_RULES_STATUS

if bash "$script_dir/required-checks-pass.sh" <<<"$passed" >"$test_dir/output" 2>"$test_dir/error"; then
  printf '%s\n' 'FAIL: missing repository and base arguments accepted' >&2
  exit 1
fi
if ! rg -q 'branches/topic%2Fone/protection/required_status_checks' "$CI_GATE_TEST_CALLS" ||
  ! rg -q -- '--paginate --slurp .*rules/branches/topic%2Fone' "$CI_GATE_TEST_CALLS"; then
  printf '%s\n' 'FAIL: branch encoding or rule pagination missing' >&2
  exit 1
fi
printf 'Required-check gate: %s cases passed, plus argument and API request checks\n' "$case_count"
