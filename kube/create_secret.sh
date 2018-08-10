#!/bin/bash -e
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-develop'}

kubectl create secret generic albuquery-config --from-file=./albuquery.yml --namespace $DAX_NAMESPACE
