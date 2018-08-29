#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-prod'}

kubectl create -f albuquery-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f albuquery-service.yaml --namespace $DAX_NAMESPACE
kubectl create -f albuquery-ingress.yaml --namespace $DAX_NAMESPACE
