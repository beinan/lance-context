#!/usr/bin/env bash
# Run the selected Cargo test executables without changing their feature graph.
set -euo pipefail

if (( $# < 3 )); then
  echo "usage: $0 ARTIFACTS PACKAGES all|lib|bin:NAME [TEST_ARGS...]" >&2
  exit 2
fi
artifacts=$1
packages=$2
selection=$3
shift 3

entries=$(jq -ers --arg packages "$packages" --arg selection "$selection" '
  ($packages | split(",")) as $wanted
  | [ .[]
      | select(.reason == "compiler-artifact" and .profile.test == true
               and .executable != null)
      | . + {package: (.manifest_path | split("/") | .[-2])}
      | select(.package as $package | $wanted | index($package))
      | select(if $selection == "all" then true
               elif $selection == "lib" then .target.kind == ["lib"]
               elif ($selection | startswith("bin:")) then
                 .target.kind == ["bin"] and .target.name == $selection[4:]
               else error("invalid target selection") end)
    ]
  | unique_by(.executable)
  | . as $tests
  | if any($wanted[]; . as $package | all($tests[]; .package != $package))
      then error("missing test executable for a requested package")
    elif $selection != "all" and length != ($wanted | length)
      then error("ambiguous test target")
    else sort_by(.package, .target.name)
      | .[] | [.manifest_path, .executable] | @tsv end
' "$artifacts")

while IFS=$'\t' read -r manifest executable; do
  test -f "$manifest"
  test -x "$executable"
  printf 'Running %s\n' "$executable"
  # Cargo starts each test target in its package directory.
  (
    cd "$(dirname "$manifest")"
    CARGO_MANIFEST_DIR="$PWD" "$executable" "$@"
  )
done <<< "$entries"
