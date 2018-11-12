#!/bin/bash
#
#
#
launch-tool/launch.py shutdown
git pull "https://srinidhigoud:12Iittopper!@github.com/nyu-distributed-systems-fa18/lab-2-raft-srinidhigoud.git" master
cd server 
go build .
cd ../
./create-docker-image.sh
launch-tool/launch.py boot 5
