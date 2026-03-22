#!/bin/bash

export AWS_ACCESS_KEY_ID="tid_WAotOpFUJKEuxOtLKfWrwFLreutdY_xLvifqaIOUcHkxrbwDaP"
export AWS_SECRET_ACCESS_KEY="tsec_KvsiU-34raykBGv0lmbGI_QqXomvj8+iTiav6mQhdkqHeKv+aIsfP2dxIDstwcPbmnVpo+"
export AWS_ENDPOINT_URL_S3="https://fly.storage.tigris.dev"
export AWS_REGION="auto"
export WALRUST_TEST_BUCKET="walrust-test"
export RUST_LOG="walrust=debug"

# Run all tests including integration tests
cargo test --lib -- --include-ignored --nocapture "$@"
