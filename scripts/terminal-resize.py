#!/usr/bin/env python
import os
import shlex
import struct
import platform
import subprocess
import time
import signal

print "checking resize"

def handler(signum, frame):
  with open('bin/resize.txt', 'wb') as fh:
    fh.write("true")
    fh.flush()

signal.signal(signal.SIGWINCH, handler)


while True:
    time.sleep(60)

print "exiting"
