#!/bin/bash -ex
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-dev'}

kubectl create -f albuquery-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f albuquery-service.yaml --namespace $DAX_NAMESPACE
kubectl create -f albuquery-ingress.yaml --namespace $DAX_NAMESPACE
