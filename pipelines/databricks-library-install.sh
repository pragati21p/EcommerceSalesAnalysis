#!/bin/bash
CLUSTER_NAME=$1

if [[ -z "$CLUSTER_NAME" ]]
then
    echo 'The cluster name must be passed as parameter'
    exit 1
fi

CLUSTER_ID=$(databricks clusters list --output json | jq -r --arg N "$CLUSTER_NAME" '.clusters[] | select(.cluster_name == $N) | .cluster_id')

databricks libraries install --cluster-id $CLUSTER_ID --maven-coordinates com.crealytics:spark-excel_2.12:0.13.5
