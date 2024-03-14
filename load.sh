#!/bin/bash
set -e

BUILD=$1

if [[ "$BUILD" = 'prod' ]]
then
    echo 'INFO: loading siblings to prod'
    BUCKET="xai-cfg"
    SIBLINGS_FILE="siblings.json"
elif [[ "$BUILD" = 'dev' ]]
then
    echo 'INFO: loading siblings to dev'
    BUCKET="xai-cfg"
    SIBLINGS_FILE="siblings-dev.json"
else
    echo "Invalid `build`. Allowed [prod | dev]"
    exit
fi

# gcloud storage cp gs://$BUCKET/$SIBLINGS_FILE $SIBLINGS_FILE
X_ENV=$BUILD RUST_LOG=info cargo run --bin siblings-cli --release -- --nocapture