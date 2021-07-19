#!/bin/sh

FUNCTION="update_hashtags"
BUCKET="gs://twitter-bucket"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
