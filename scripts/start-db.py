from subprocess import call
import sys
import shlex
from os.path import expanduser
import os
from time import sleep
import threading
import Queue
from os.path import expanduser
import json

home = expanduser("~")

hosts = [
"52.11.128.207"
]

with open('config/config.json') as data_file:
    data = json.load(data_file)

shards = data["database"]["shards"]


def start_db_server(home, dns, port, shard):
    command = "ssh -n -f -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i " + home + '/amazonkeys/dale-aws2.pem  ubuntu@' + dns + ' \"sh -c \'sudo mkdir -p /mnt/logs; sudo chown -R ubuntu:ubuntu /mnt; nohup bash /home/ubuntu/database/bin/remote-start-db-server ' + str(shard) + ' ' + str(port) + ' >> /mnt/logs/rest-stdout.log 2>&1 < /dev/null   &\'\"'
    print "Starting: " + command
    split_command = shlex.split(command)
    ret = call(split_command)
    if ret == 0:
        print "Started successfully to " + dns
    else:
        print "Start failed to " + dns + ", code=" + unicode(ret)

def stop_db(home, dns):
  command = "ssh -n -f -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i " + home + '/amazonkeys/dale-aws2.pem  ubuntu@' + dns + " killall -9 java"
  #/home/ubuntu/database/bin/kill-server NettyServer"
  print "Stopping: " + command
  split_command = shlex.split(command)
  ret = call(split_command)
  if ret == 0:
    print "Stopped successfully to " + dns
  else:
    print "Stop failed: command=" + command


for i in range(0, len(shards)):
    host = shards[i]["host"]
    stop_db(home, host)

for i in range(0, len(shards)):
    host = shards[i]["host"]
    port = shards[i]["port"]
    start_db_server(home, host, port + 10, i)


