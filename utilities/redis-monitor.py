#!/usr/bin/python3

import sys, getopt
import argparse
import json
import string
import datetime
import math
import redis

class t_global(object):
    args=None;

def process_options ():
    parser = argparse.ArgumentParser(description="Monitor commands being processed by a redis server.");

    parser.add_argument('--redis-server',
                        dest = 'redis_server',
                        help = 'What is network name for the redis server (hostname or IP address).',
                        default = 'localhost',
                        type = str)

    parser.add_argument('--redis-password',
                        dest = 'redis_password',
                        help = 'What is password used to connect to the redis server.',
                        default = 'foobar',
                        type = str)

    t_global.args = parser.parse_args();

def main():
    process_options()

    redcon = redis.Redis(host = t_global.args.redis_server,
                         port = 6379,
                         password = t_global.args.redis_password)

    with redcon.monitor() as m:
        for command in m.listen():
            print(command)

    redcon.close()
    
if __name__ == "__main__":
    exit(main())
