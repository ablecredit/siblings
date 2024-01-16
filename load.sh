#!/bin/bash
set -e

PROJECT=$1
BUCKET=$2
KEY=$3

gcloud storage cp gs://$BUCKET/$KEY siblings.json
X_PROJECT=$1 X_ENV=prod RUST_LOG=info cargo test load --release -- --nocapture