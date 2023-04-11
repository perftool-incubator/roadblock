#!/usr/bin/python3

'''Roadblock is a synchronization and message passing utility which relies on redis for communication'''

import argparse
import logging
import sys
import socket
import shlex

from pathlib import Path
from dataclasses import dataclass

from roadblock import roadblock

# define some global variables
@dataclass
class global_vars:
    '''Global variables'''

    args = None
    log = None
    wait_for_cmd = None

    # log formatting variables
    log_debug_format =  '[%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'


def process_options ():
    '''Define the CLI argument parsing options'''

    parser = argparse.ArgumentParser(description = "Roadblock provides multi entity (system, vm, container, etc.) synchronization.",
                                     formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--uuid",
                        dest = "roadblock_uuid",
                        help = "UUID that maps to the specific roadblock being processed.",
                        required = True)

    parser.add_argument("--role",
                        dest = "roadblock_role",
                        help = "What is the roadblock role of this node.",
                        default = "follower",
                        choices = ["leader", "follower"])

    parser.add_argument("--timeout",
                        dest = "roadblock_timeout",
                        help = "How long should the roadblock wait before timing out.",
                        default = 30,
                        type = int)

    parser.add_argument("--follower-id",
                        dest = "roadblock_follower_id",
                        help = "What is follower ID for this node.",
                        default = socket.getfqdn(),
                        type = str)

    parser.add_argument("--leader-id",
                        dest = "roadblock_leader_id",
                        help = "What is leader ID for this specific roadblock.",
                        type = str,
                        required = True)

    parser.add_argument("--redis-server",
                        dest = "roadblock_redis_server",
                        help = "What is network name for the redis server (hostname or IP address).",
                        default = "localhost",
                        type = str)

    parser.add_argument("--redis-password",
                        dest = "roadblock_redis_password",
                        help = "What is password used to connect to the redis server.",
                        default = "foobar",
                        type = str)

    parser.add_argument("--followers",
                        dest = "roadblock_followers",
                        help = "Use one or more times on the leader to specify the followers by name.",
                        action = "append",
                        type = str)

    parser.add_argument("--abort",
                        dest = "abort",
                        help = "Use this option as a follower or leader to send an abort message as part of this synchronization",
                        action = "store_true")

    parser.add_argument("--message-log",
                        dest = "message_log",
                        help = "File to log all received messages to.",
                        default = None,
                        type = str)

    parser.add_argument("--user-messages",
                        dest = "user_messages",
                        help = "File to load user specified messages from.",
                        default = None,
                        type = str)

    parser.add_argument("--log-level",
                        dest = "log_level",
                        help = "Control how much logging output should be generated",
                        default = "normal",
                        choices = [ "normal", "debug" ])

    parser.add_argument("--message-validation",
                        dest = "message_validation",
                        help = "What type of message validation to do",
                        default = "all",
                        choices = [ "none", "checksum", "schema", "all" ])

    parser.add_argument("--wait-for",
                        dest = "wait_for",
                        help = "Launch this program/script and wait for it to complete before proceeding.  Requires --wait-for-log to be set.",
                        default = None,
                        type = str)

    parser.add_argument("--wait-for-log",
                        dest = "wait_for_log",
                        help = "Where to log the output from the --wait-for program/script",
                        default = None,
                        type = str)

    parser.add_argument("--simulate-heartbeat-timeout",
                        dest = "simulate_heartbeat_timeout",
                        help = argparse.SUPPRESS,
                        action = "store_true")

    t_global.args = parser.parse_args()

    if t_global.args.wait_for is not None and t_global.args.wait_for_log is None:
        parser.error("When --wait-for is defined then --wait-for-log must also be defined")

    if t_global.args.wait_for is not None:
        t_global.wait_for_cmd = shlex.split(t_global.args.wait_for)
        p = Path(t_global.wait_for_cmd[0])
        if not p.exists():
            parser.error(f"The specified --wait-for command does not exist [{t_global.wait_for_cmd[0]}]")
        if not p.is_file():
            parser.error(f"The specified --wait-for command is not a file [{t_global.wait_for_cmd[0]}]")

    if t_global.args.log_level == 'debug':
        logging.basicConfig(level = logging.DEBUG, format = t_global.log_debug_format, stream = sys.stdout)
    elif t_global.args.log_level == 'normal':
        logging.basicConfig(level = logging.INFO, format = t_global.log_normal_format, stream = sys.stdout)

    t_global.log = logging.getLogger(__file__)


def main():
    '''Main control block'''

    process_options()

    log_debug = False
    if t_global.args.log_level == "debug":
        log_debug = True

    rb = roadblock(t_global.log, log_debug)
    rb.set_uuid(t_global.args.roadblock_uuid)
    rb.set_role(t_global.args.roadblock_role)
    rb.set_follower_id(t_global.args.roadblock_follower_id)
    rb.set_leader_id(t_global.args.roadblock_leader_id)
    rb.set_timeout(t_global.args.roadblock_timeout)
    rb.set_redis_server(t_global.args.roadblock_redis_server)
    rb.set_redis_password(t_global.args.roadblock_redis_password)
    rb.set_followers(t_global.args.roadblock_followers)
    rb.set_abort(t_global.args.abort)
    rb.set_message_log(t_global.args.message_log)
    rb.set_user_messages(t_global.args.user_messages)
    rb.set_message_validation(t_global.args.message_validation)
    rb.set_wait_for_cmd(t_global.wait_for_cmd)
    rb.set_wait_for_log(t_global.args.wait_for_log)
    rb.set_simulate_heartbeat_timeout(t_global.args.simulate_heartbeat_timeout)

    return rb.run_it()

if __name__ == "__main__":
    t_global = global_vars()
    sys.exit(main())
