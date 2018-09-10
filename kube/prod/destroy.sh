#!/bin/bash -x
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-prod'}

kubectl delete ingress albuquery-ingress --namespace $DAX_NAMESPACE
kubectl delete service albuquery-service --namespace $DAX_NAMESPACE
kubectl delete deployment albuquery-deployment --namespace $DAX_NAMESPACE

