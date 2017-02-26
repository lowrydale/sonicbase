#!/bin/bash
rsync -rvlLt --delete --exclude 'logs' -e 'ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no' * $1
