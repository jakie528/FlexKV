#!/bin/sh

set -o errexit

# cargo build

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
# kill_all
# sleep 1

# if ls 127.0.0.1:*.db
# then
#     rm -r 127.0.0.1:*.db || echo "no db to clean"
# fi

kill_all

rm -r 127.0.0.1:*.db
