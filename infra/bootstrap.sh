#!/bin/bash
set -euo pipefail

apt-get update
apt-get install -y git

sudo -u ubuntu git clone \
    --depth 1 -- \
    "https://github.com/mirandaricky9/qBachTorRick.git" \
    /home/ubuntu/qBachTorRick
