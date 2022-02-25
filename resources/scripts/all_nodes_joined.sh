#!/usr/bin/env bash

if ! command -v rg &> /dev/null
then
    echo "ripgrep could not be found and is required"
    exit 1
fi

DEFAULT_LOG_DIR="$HOME/.safe/node/local-test-network"

DEFAULT_ELDER_COUNT=7
ELDER_COUNT="${SN_ELDER_COUNT:-$DEFAULT_ELDER_COUNT}"
DEFAULT_NODE_COUNT=30
NODE_COUNT="${NODE_COUNT:-$DEFAULT_NODE_COUNT}"

log_dir="${LOG_DIR:-$DEFAULT_LOG_DIR}"
echo "Checking nodes log files to verify all nodes have joined. Logs path: $log_dir"

# -u needed here to search log dirs
nodes_ips=$(rg "connection info:.*" "$log_dir" -g "*.log*" -u | rg "(127.0.0.1:\d{5})" -or '$1' | sort)
nodes_ips_count=$(echo "$nodes_ips" | wc -l)

echo "Number of nodes: $nodes_ips_count"

if [[ $nodes_ips_count -ne $NODE_COUNT ]]
    then
        echo "Unexpected number of joined nodes. Expected $NODE_COUNT, we have $nodes_ips_count:"
        echo "$nodes_ips"
        exit 100
    else
        echo "All nodes have joined. Nodes IPs:"
        echo "$nodes_ips"
fi

# We'll use the logs from the nodes that joined, to obtain the
# list of members in the network knowledge they share with AE messages.
members=$(rg ".* msg: AntiEntropy \{.* kind: Update \{ members: \{(SectionSigned \{.*\})+ \}.*" -or '$1' "$log_dir" -g "*.log*" | rg "(127.0.0.1:\d{5})" -or '$1' | sort -u)
members_count=$(echo "$members" | wc -l)

echo ""
if [[ $members_count -ne $NODE_COUNT ]]
then
  echo "Unexpected number of nodes in network knowledge. Expected $NODE_COUNT, we have $members_count:"
  echo "$members"
else
  echo "Number of nodes found in network knowledge: $members_count"
fi

echo ""
echo "Checking which nodes in network knowledge match the list of nodes IPs..."

invalid_member_found=false
for m in $members
do
    if grep -q "$m" <<< "$nodes_ips"
    then
      echo "Node $m is a valid member"
    else
      echo "Node $m in network knowledge was not found in the list of nodes IPs"
      invalid_member_found=true
    fi
done

echo ""
if $invalid_member_found
then
  echo "At least one member in the network knowledge was found invalid"
  exit 100
else
  echo "All good, members in the network knowledge found in the list of nodes IPs!"
fi
