#!/bin/bash
cp state_tracker.py layers/state_tracker/
sam build -t dl_lambda_layers.yml --use-container --skip-pull-image
cd .aws-sam/build
rm -rf StateTrackerLayer/python/jars
sam package --template-file template.yaml --output-template-file ../../packaged.yaml --s3-bucket life360-102611674515-devops-artifacts --s3-prefix state-tracker-layer --profile $@
cd ../..
aws cloudformation deploy --template-file packaged.yaml --profile $@ --stack-name prod-state-tracker-layer --capabilities CAPABILITY_NAMED_IAM --role-arn arn:aws:iam::102611674515:role/AWS_LIFE360-DATASERVICES-PROD-01_DevopsBot_102611674515 --region us-east-1
