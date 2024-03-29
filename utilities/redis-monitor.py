#!/usr/bin/python3

'''Utility to monitor redis activity'''

import sys
import argparse

import redis


def process_options ():
    '''Define the CLI argument parsing options'''

    parser = argparse.ArgumentParser(description="Monitor commands being processed by a redis server.")

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

    return parser.parse_args()

def main():
    '''Main control block'''

    args = process_options()

    try:
        redcon = redis.Redis(host = args.redis_server,
                             port = 6379,
                             password = args.redis_password,
                             health_check_interval = 0)
        redcon.ping()
    except redis.exceptions.ConnectionError as con_error:
        print(f"EXCEPTION: {con_error}")
        print("ERROR: Redis connection could not be opened!")
        return -1

    with redcon.monitor() as mon:
        for command in mon.listen():
            print(command)

    redcon.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
