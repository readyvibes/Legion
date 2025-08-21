#!/bin/bash

# Usage: ./bootstrap.sh [master|worker]

if [ "$1" = "master" ]; then
    echo "Setting up certificates for MASTER node"
    
    # Create directories
    sudo mkdir -p /etc/ssl/certs
    sudo mkdir -p /etc/ssl/private

    # Step 1: Create Certificate Authority (CA)
    openssl genrsa -out ca.key 4096
    openssl req -new -x509 -key ca.key -sha256 -subj "/C=US/ST=NJ/O=HPC/CN=HPC-CA" -days 3650 -out ca.crt

    # Step 2: Generate Master Certificate
    openssl genrsa -out master.key 4096
    openssl req -new -key master.key -out master.csr -subj "/C=US/ST=NJ/O=HPC/CN=master.cluster.local"
    openssl x509 -req -in master.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out master.crt -days 365 -sha256

    # Install master certificates
    sudo cp ca.crt /etc/ssl/certs/
    sudo cp master.crt /etc/ssl/certs/
    sudo cp master.key /etc/ssl/private/

    # Set proper permissions
    sudo chmod 644 /etc/ssl/certs/ca.crt
    sudo chmod 644 /etc/ssl/certs/master.crt
    sudo chmod 600 /etc/ssl/private/master.key

    echo "Master certificates installed in /etc/ssl/certs/"
    echo "Copy ca.crt to worker nodes"

    mkdir -p /var/log/legion

    # Fix DNS first before installing packages
    echo "Fixing DNS configuration..."
    
    # Backup original resolv.conf
    sudo cp /etc/resolv.conf /etc/resolv.conf.backup
    
    # Use Google DNS temporarily for package installation
    sudo tee /etc/resolv.conf > /dev/null <<EOF
nameserver 8.8.8.8
nameserver 8.8.4.4
search cluster.local
EOF

    # Update package lists
    sudo apt update

    # Install required packages
    sudo apt install -y dnsmasq postgresql

    # Configure dnsmasq AFTER postgresql is installed
    sudo tee /etc/dnsmasq.d/cluster.conf > /dev/null <<EOF
# Listen on all interfaces
interface=ens4
bind-interfaces

# DNS settings
domain=cluster.local
expand-hosts

# Master node DNS record
address=/master.cluster.local/$(hostname -I | awk '{print $1}')

# DHCP range (optional)
dhcp-range=10.128.0.100,10.128.0.200,24h

# Log queries (for debugging)
log-queries

# Forward DNS queries to upstream servers
server=8.8.8.8
server=8.8.4.4
EOF

    # Configure resolv.conf to use both local and upstream DNS
    sudo tee /etc/resolv.conf > /dev/null <<EOF
nameserver 127.0.0.1
nameserver 8.8.8.8
nameserver 8.8.4.4
search cluster.local
EOF

    # Restart dnsmasq
    sudo systemctl restart dnsmasq
    sudo systemctl enable dnsmasq

    echo "DNS configured to use local dnsmasq with upstream fallback"

    # PostgreSQL setup
    echo "Setting up PostgreSQL..."
    
    # Check if postgres user exists first
    if id "postgres" &>/dev/null; then
        echo "postgres user exists, creating root user..."
        sudo -u postgres createuser --superuser root 2>/dev/null || echo "root user may already exist"
    else
        echo "postgres user doesn't exist, PostgreSQL may not be properly installed"
        exit 1
    fi

    # Create database
    if command -v createdb &> /dev/null; then
        createdb legiondb 2>/dev/null || echo "Database legiondb may already exist"
        echo "Database legiondb created successfully"
    else
        echo "createdb command not found, PostgreSQL installation may have failed"
        exit 1
    fi

elif [ "$1" = "worker" ]; then
    echo "Setting up certificates for WORKER node"
    
    # Create directories
    sudo mkdir -p /etc/ssl/certs
    sudo mkdir -p /etc/ssl/private

    # Check if ca.crt exists (should be copied from master)
    if [ ! -f "ca.crt" ]; then
        echo "ERROR: ca.crt not found. Copy it from master node first."
        exit 1
    fi

    # Step 3: Generate Worker Certificate
    openssl genrsa -out worker.key 4096
    openssl req -new -key worker.key -out worker.csr -subj "/C=US/ST=NJ/O=HPC/CN=worker01.cluster.local"
    
    # Need ca.key to sign worker cert - this should be done on master
    if [ ! -f "ca.key" ]; then
        echo "ERROR: ca.key not found. Generate worker cert on master node or copy ca.key"
        exit 1
    fi
    
    openssl x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out worker.crt -days 365 -sha256

    # Install worker certificates
    sudo cp ca.crt /etc/ssl/certs/
    sudo cp worker.crt /etc/ssl/certs/
    sudo cp worker.key /etc/ssl/private/

    # Set proper permissions
    sudo chmod 644 /etc/ssl/certs/ca.crt
    sudo chmod 644 /etc/ssl/certs/worker.crt
    sudo chmod 600 /etc/ssl/private/worker.key

    echo "Worker certificates installed in /etc/ssl/certs/"

else
    echo "Usage: $0 [master|worker]"
    echo "  master - Setup CA and master certificates"
    echo "  worker - Setup worker certificates (requires ca.crt from master)"
    exit 1
fi