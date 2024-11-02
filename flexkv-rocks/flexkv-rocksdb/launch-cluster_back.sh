#!/bin/bash


set -o errexit

#cargo build

kill_all() {
    SERVICE='flexkv-rocks'
    if [ "$(uname)" = "Darwin" ]; then
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
        rm -r 127.0.0.1:*.db || echo "no db to clean"
    else
        set +e # killall will error if finds no process to kill
        killall "${SERVICE}"
        set -e
    fi
}

rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"
    {
        if [ ".$body" = "." ]; then
            time curl --silent "127.0.0.1:$uri"
        else
            time curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }

    echo
    echo
}

export RUST_LOG=trace
export RUST_BACKTRACE=full
bin=./target/debug/flexkv-rocks

echo "Killing all running flexkv-rocks and cleaning up old data"

kill_all
sleep 1

if ls 127.0.0.1:*.db
then
    rm -r 127.0.0.1:*.db || echo "no db to clean"
fi


# Check if the config file path is provided as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <config_file_path>"
    exit 1
fi

config_file="$1"

echo "Start flexkv-rocks servers from config file: $config_file"


declare -A http_addrs
declare -A rpc_addrs
index=0
# Start servers and store their addresses
while IFS=',' read -r id http_addr rpc_addr; do

    http_addrs[$id]="$http_addr"
    rpc_addrs[$id]="$rpc_addr"

    if [ "$id" -eq 1 ]; then
        ${bin} --id "$id" --http-addr "$http_addr" --rpc-addr "$rpc_addr" 2>&1 > "n${id}.log" &
        PID1=$!
        sleep 1
        echo "Server $id started"
    else
        nohup ${bin} --id "$id" --http-addr "$http_addr" --rpc-addr "$rpc_addr" > "n${id}.log" &
        sleep 1
        echo "Server $id started"
    fi
        index=$((index + 1))

done < "$config_file"
echo $index


echo "Initialize server 1 as a single-node cluster"
sleep 2
echo
rpc "${http_addrs[1]}/cluster/init" '{}'

echo "Server 1 is a leader now"
sleep 2

echo "Get metrics from the leader"
echo rpc "${http_addrs[1]}/cluster/metrics" 
sleep 2


# Add each node (except for the leader, which is node 1) as a learner
for ((i=1; i<${#http_addrs[@]}; i++)); do
    id=$((i + 1))  # Adjust ID based on the index
    echo
    rpc "${http_addrs[1]}/cluster/add-learner" "[$id, \"${http_addrs[$i]}\", \"${rpc_addrs[$i]}\"]"
    echo "Node $id added as learner"
    sleep 1
done

echo "Get metrics from the leader, after adding learners"
sleep 1

# Change membership to include all nodes in the cluster in the order of [1, 2, 3]
membership_list=$(seq -s, 1 $index)
echo "Changing membership from [1] to multi-node cluster: [$membership_list]"
echo
rpc "${http_addrs[1]}/cluster/change-membership" "[$membership_list]"
sleep 1
echo "Membership changed"
sleep 1

echo "Cluster Launched!"
