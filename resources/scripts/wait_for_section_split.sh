#!/usr/bin/env bash

echo "Waiting for all nodes to join..."
until ./resources/scripts/has_split.sh || $( sleep 15 && false ); do :; done
