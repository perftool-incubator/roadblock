#!/usr/bin/python3

'''Roadblock is a synchronization and message passing utility which relies on redis for communication'''

import argparse
import logging
import signal
import socket
import shlex
import sys
import threading

from pathlib import Path

from roadblock import roadblock
from roadblock import VERBOSE_DEBUG_LEVEL
import roadblocker_config


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

    parser.add_argument("--followers-file",
                        dest = "roadblock_followers_file",
                        help = "File containing a list of followers to load",
                        default = None,
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
                        default = "verbose-debug",
                        choices = [ "normal", "debug", "verbose-debug" ])

    parser.add_argument("--message-validation",
                        dest = "message_validation",
                        help = "What type of message validation to do",
                        default = "none",
                        choices = [ "none", "checksum", "schema", "all" ])

    parser.add_argument("--connection-watchdog",
                        dest = "connection_watchdog",
                        help = "Should the connection watchdog be enabled or disabled",
                        default = "disabled",
                        choices = [ "enabled", "disabled" ])

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

    args = parser.parse_args()

    if args.wait_for is not None and args.wait_for_log is None:
        parser.error("When --wait-for is defined then --wait-for-log must also be defined")

    if args.wait_for is not None:
        cmd = shlex.split(args.wait_for)
        p = Path(cmd[0])
        if not p.exists():
            parser.error(f"The specified --wait-for command does not exist [{cmd[0]}]")
        if not p.is_file():
            parser.error(f"The specified --wait-for command is not a file [{cmd[0]}]")

    if args.roadblock_followers_file is not None:
        rb_f_file = shlex.split(args.roadblock_followers_file)
        p = Path(rb_f_file[0])
        if not p.exists():
            parser.error(f"The specified --followers-file does not exist [{rb_f_file[0]}]")
        if not p.is_file():
            parser.error(f"The specified --followers-file is not a file [{rb_f_file[0]}]")

    return args


def sigint_handler(signum, frame):
    '''Handle a SIGINT/CTRL-C'''

    if signum == signal.SIGINT:
        roadblocker_config.logger.warning("Caught a SIGINT signal")
        roadblocker_config.sigint_counter += 1

        if roadblocker_config.sigint_counter == 1:
            roadblocker_config.logger.warning("SIGINT handler is procesing a minor abort event")
            roadblocker_config.minor_abort_event.set()
        else:
            roadblocker_config.logger.warning("SIGINT handler is processing a major abort event [%d]", roadblocker_config.sigint_counter)
            roadblocker_config.major_abort_event.set()
    else:
        roadblocker_config.logger.warning("SIGINT handler called with signal %d", signum)

    return 0


def main():
    '''Main control block'''

    args = process_options()

    # log formatting variables
    log_debug_format =  '[CODE][%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s][%(threadName)s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'

    if args.log_level == 'debug':
        logging.basicConfig(level = logging.DEBUG, format = log_debug_format, stream = sys.stdout)
    elif args.log_level == 'normal':
        logging.basicConfig(level = logging.INFO, format = log_normal_format, stream = sys.stdout)
    elif args.log_level == 'verbose-debug':
        logging.basicConfig(level = VERBOSE_DEBUG_LEVEL, format = log_debug_format, stream = sys.stdout)

    roadblocker_config.logger = logging.getLogger(__file__)

    log_debug = False
    if args.log_level in [ "debug",  "verbose-debug" ]:
        log_debug = True

    followers = []
    if args.roadblock_followers is not None and len(args.roadblock_followers) > 0:
        followers.extend(args.roadblock_followers)
    if args.roadblock_followers_file is not None:
        try:
            with open(args.roadblock_followers_file, "r", encoding="ascii") as followers_file:
                for line in followers_file:
                    followers.append(line.rstrip('\n'))
        except IOError:
            roadblocker_config.logger.critical("Could not load the roadblock followers file '%s'!", args.roadblock_followers_file)
            return 2

    rb = roadblock(roadblocker_config.logger, log_debug)
    rb.set_uuid(args.roadblock_uuid)
    rb.set_role(args.roadblock_role)
    rb.set_follower_id(args.roadblock_follower_id)
    rb.set_leader_id(args.roadblock_leader_id)
    rb.set_timeout(args.roadblock_timeout)
    rb.set_redis_server(args.roadblock_redis_server)
    rb.set_redis_password(args.roadblock_redis_password)
    rb.set_followers(followers)
    rb.set_abort(args.abort)
    rb.set_message_log(args.message_log)
    rb.set_user_messages(args.user_messages)
    rb.set_message_validation(args.message_validation)
    rb.set_connection_watchdog(args.connection_watchdog)
    if args.wait_for is not None:
        rb.set_wait_for_cmd(shlex.split(args.wait_for))
    rb.set_wait_for_log(args.wait_for_log)
    rb.set_simulate_heartbeat_timeout(args.simulate_heartbeat_timeout)

    roadblocker_config.minor_abort_event = threading.Event()
    roadblocker_config.major_abort_event = threading.Event()
    rb.set_minor_abort_event(roadblocker_config.minor_abort_event)
    rb.set_major_abort_event(roadblocker_config.major_abort_event)

    #catch SIGINT/CTRL-C
    signal.signal(signal.SIGINT, sigint_handler)

    return rb.run_it()

if __name__ == "__main__":
    sys.exit(main())
