#!/bin/bash

## Script to pull images with a ISP workstation
podman pull --tls-verify=false postgres:17
podman pull --tls-verify=false confluentinc/cp-kafka:7.6.1
podman pull --tls-verify=false quay.io/debezium/connect:2.7
podman pull --tls-verify=false flink:1.19-scala_2.12-java17
podman pull --tls-verify=false confluentinc/cp-enterprise-control-center:7.6.1
podman pull --tls-verify=false provectuslabs/kafka-ui:latest
podman pull --tls-verify=false clickhouse/clickhouse-server:24.3
podman pull --tls-verify=false python:3.11-slim
