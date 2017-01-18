#!/bin/bash
ssh -n -f -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $1 nohup bash $2/bin/start-db-server.sh $3 $4 $5 $6 $7
