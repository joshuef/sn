#!/usr/bin/env bash

if ! command -v rg &> /dev/null
then
    echo "ripgrep could not be found and is required"
    exit 1
fi

if ! command -v jq &> /dev/null
then
    echo "jq could not be found and is required"
    exit 1
fi

if ! command -v statemap &> /dev/null
then
    echo "Statemap could not be found and is required, install with"
    echo "cargo install --git https://github.com/TritonDataCenter/statemap.git"
    exit 1
fi

log_dir="$HOME/.safe/node/local-test-network"
out_file="safe_states.out"

# Extract statemap metadata
rg -IN ".*STATEMAP_METADATA: " "$log_dir" --replace "" | head -n1 > "$out_file"
rg -IN ".*STATEMAP_ENTRY: " "$log_dir" --replace "" | jq -s 'sort_by(.time|tonumber)' | jq -c '.[]' >> "$out_file"

begin_time=$(cat safe_states.out | rg 'time' | jq -sr 'min_by(.time | tonumber) | .time')
end_time=$(cat safe_states.out | rg 'time' | jq -sr 'max_by(.time | tonumber) | .time')
statemap_cmd="statemap --sortby=Idle -b $begin_time -e $end_time -c 300000 $out_file"

if [[ $* == *--run-statemap* ]]
then
    $statemap_cmd
else
    echo "Wrote statemap data to $out_file"

    echo "Render the statemap SVG with"
    echo ""
    echo "    $statemap_cmd > safe.svg"
fi
