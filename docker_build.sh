#!/bin/bash
docker build --rm -t dlza-manager-checker:latest .
docker tag dlza-manager-checker:latest registry.localhost:5001/dlza-manager-checker
docker push registry.localhost:5001/dlza-manager-checker
