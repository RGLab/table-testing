#!/bin/bash
set -exo pipefail

echo "Creating stack named $1"
echo "Uploading to bucket $2"

AWS="aws"

rm -f filter_merge_lambda.zip
# These are huge, like 65MB :(
pip3 install --system -t filter_merge_lambda pyarrow fastparquet s3fs
cd filter_merge_lambda && zip -X -r ../filter_merge_lambda.zip * && cd ..
$AWS s3 cp filter_merge_lambda.zip s3://"$2"/filter_merge_lambda.zip

$AWS cloudformation create-stack --stack-name "$1" \
    --template-body file://filter_merge_cfn.yaml \
    --parameters ParameterKey=LambdaCodeBucket,ParameterValue="$2" \
                 ParameterKey=LambdaCodeKey,ParameterValue=filter_merge_lambda.zip \
    --capabilities CAPABILITY_NAMED_IAM
