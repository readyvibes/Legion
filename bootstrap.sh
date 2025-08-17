#!/bin/bash

# Step 1: Create Certificate Authority (CA)
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -sha256 -subj "/C=US/ST=NJ/O=HPC/CN=HPC-CA" -days 3650 -out ca.crt

# Step 2: Generate Master Certificate
openssl genrsa -out master.key 4096
openssl req -new -key master.key -out master.csr -subj "/C=US/ST=NJ/O=HPC/CN=master.cluster.local"
openssl x509 -req -in master.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out master.crt -days 365 -sha256

# Step 3: Generate Worker Certificate (repeat for each worker)
openssl genrsa -out worker.key 4096
openssl req -new -key worker.key -out worker.csr -subj "/C=US/ST=NJ/O=HPC/CN=worker01.cluster.local"
openssl x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out worker.crt -days 365 -sha256