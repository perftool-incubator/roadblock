#!/usr/bin/python3

import argparse
import string
import datetime
import time
import calendar
import socket
import redis
import signal
import hashlib
import json

from jsonschema import validate

# define some global variables
class t_global(object):
    args = None
    redcon = None
    pubsubcon = None
    first = False
    schema = None
    my_id = None

def message_to_str (message):
    return(json.dumps(message, separators=(',', ':')))

def message_from_str (message):
    return(json.loads(message))

def message_build (recipient_type, recipient_id, command, value=None):
    message = {
        "payload": {
            "uuid": t_global.args.roadblock_uuid,
            "sender": {
                "type": t_global.args.roadblock_role,
                "id": t_global.my_id
            },
            "recipient": {
                "type": recipient_type
            },
            "message": {
                "command": command
            }
        },
        "checksum": None
    }

    if recipient_type != "all":
        message["payload"]["recipient"]["id"] = recipient_id

    if value is not None:
        message["payload"]["message"]["value"] = str(value)

    message["checksum"] = hashlib.sha256(str(message_to_str(message["payload"])).encode('utf-8')).hexdigest()

    return(message)

def message_validate (message):
    try:
        validate(instance=message, schema=t_global.schema)

        checksum = hashlib.sha256(str(message_to_str(message["payload"])).encode('utf-8')).hexdigest()

        if message["checksum"] == checksum:
            return(True)
        else:
            return(False)
    except:
        return(False)

def message_for_me (message):
    if message["payload"]["recipient"]["type"] == "all":
        return(True)
    elif message["payload"]["recipient"]["type"] == t_global.args.roadblock_role and message["payload"]["recipient"]["id"] == t_global.my_id:
        return(True)
    else:
        return(False)

def message_get_command(message):
    return(message["payload"]["message"]["command"])

def message_get_value(message):
    return(message["payload"]["message"]["value"])

def message_get_sender(message):
    return(message["payload"]["sender"]["id"])

def define_msg_schema ():
    t_global.schema = {
        "type": "object",
        "properties": {
            "payload": {
                "type": "object",
                "properties": {
                    "uuid": {
                        "type": "string",
                        "enum": [
                            t_global.args.roadblock_uuid
                        ]
                    },
                    "sender": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [
                                    "leader",
                                    "follower"
                                ]
                            },
                            "id": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "type",
                            "id"
                        ],
                        "additionalProperties": False
                    },
                    "recipient": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [
                                    "leader",
                                    "follower",
                                    "all"
                                ]
                            },
                            "id": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "type"
                        ],
                        "additionalProperties": False,
                        "if": {
                            "properties": {
                                "type": {
                                    "enum": [
                                        "leader",
                                        "follower"
                                    ]
                                }
                            }
                        },
                        "then": {
                            "required": [
                                "id"
                            ]
                        }
                    },
                    "message": {
                        "type": "object",
                        "properties": {
                            "command": {
                                "type": "string",
                                "enum": [
                                    "timeout-ts",
                                    "initialized",
                                    "leader-online",
                                    "abort",
                                    "ready",
                                    "go-abort",
                                    "go",
                                    "gone"
                                ]
                            },
                            "value": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "command"
                        ],
                        "additionalProperties": False,
                        "if": {
                            "properties": {
                                "command": {
                                    "enum": [
                                        "timeout-ts"
                                    ]
                                }
                            }
                        },
                        "then": {
                            "required": [
                                "value"
                            ]
                        }
                    }
                },
                "required": [
                    "uuid",
                    "sender",
                    "recipient",
                    "message"
                ],
                "additionalProperties": False
            },
            "checksum": {
                "type": "string",
                "minLength": 64,
                "maxLength": 64
            }
        },
        "required": [
            "payload",
            "checksum"
        ],
        "additionalProperties": False
    }

def process_options ():
    parser = argparse.ArgumentParser(description="Roadblock provides multi entity (system, vm, container, etc.) synchronization.");

    parser.add_argument('--uuid',
                        dest = 'roadblock_uuid',
                        help = 'UUID that maps to the specific roadblock being processed.',
                        required = True)

    parser.add_argument('--role',
                        dest = 'roadblock_role',
                        help = 'What is the roadblock role of this node.',
                        default = 'follower',
                        choices = ['leader', 'follower'])

    parser.add_argument('--timeout',
                        dest = 'roadblock_timeout',
                        help = 'How long should the roadblock wait before timing out.',
                        default = 30,
                        type = int)

    parser.add_argument('--follower-id',
                        dest = 'roadblock_follower_id',
                        help = 'What is follower ID for this node.',
                        default = socket.getfqdn(),
                        type = str)

    parser.add_argument('--leader-id',
                        dest = 'roadblock_leader_id',
                        help = 'What is leader ID for this specific roadblock.',
                        type = str)

    parser.add_argument('--redis-server',
                        dest = 'roadblock_redis_server',
                        help = 'What is network name for the redis server (hostname or IP address).',
                        default = 'localhost',
                        type = str)

    parser.add_argument('--redis-password',
                        dest = 'roadblock_redis_password',
                        help = 'What is password used to connect to the redis server.',
                        default = 'foobar',
                        type = str)

    parser.add_argument('--followers',
                        dest = 'roadblock_followers',
                        help = 'Use one or more times on the leader to specify the followers by name.',
                        action = 'append',
                        type = str)

    parser.add_argument('--abort',
                        dest = 'abort',
                        help = 'Use this option as a follower to send an abort message as part of this synchronization',
                        action = 'append',
                        type = bool)

    parser.add_argument('--debug',
                        dest = 'debug',
                        help = 'Turn on debug output',
                        action = 'store_true')

    t_global.args = parser.parse_args();


def cleanup():
    print("Disabling timeout alarm")
    signal.alarm(0)

    if t_global.args.roadblock_role == 'leader':
        print("Removing db objects specific to this roadblock")
        t_global.redcon.delete(t_global.args.roadblock_uuid)
        t_global.redcon.delete(t_global.args.roadblock_uuid + '__timeout-timestamp')
        t_global.redcon.delete(t_global.args.roadblock_uuid + '__initialized')
        t_global.redcon.delete(t_global.args.roadblock_uuid + '__online-status')

    print("Closing connections")
    t_global.pubsubcon.close()
    t_global.redcon.close()

    return(0)


def do_timeout():
    print("The roadblock has timed out")
    if t_global.first:
        # set a persistent flag that the roadblock timed out so that
        # any late arriving members know that the roadblock has
        # already failed.  done by the first member since that is the
        # only member that is guaranteed to have actually reached the
        # roadblock and be capable of setting this.
        t_global.redcon.msetnx({t_global.args.roadblock_uuid + '__timedout': int(True)})
    cleanup()
    exit(-3)


def sighandler(signum, frame):
    if signum == 14: # SIGALRM
        do_timeout()
    else:
        print('Signal handler called with signal', signum)

    return(0)

def main():
    process_options()

    leader_abort = False
    followers = { 'ready': {},
                  'gone': {} }

    if len(t_global.args.roadblock_leader_id) == 0:
        print("ERROR: You must specify the leader's ID using --leader-id")
        return(-1)

    if t_global.args.roadblock_role == 'leader':
        if len(t_global.args.roadblock_followers) == 0:
            print("ERROR: There must be at least one follower")
            return(-1)

        # build some hashes for easy tracking of follower status
        for follower in t_global.args.roadblock_followers:
            followers['ready'][follower] = True
            followers['gone'][follower] = True

    if t_global.args.roadblock_role == "follower":
        t_global.my_id = t_global.args.roadblock_follower_id
    elif t_global.args.roadblock_role == "leader":
        t_global.my_id = t_global.args.roadblock_leader_id

    define_msg_schema()

    # define a signal handler that will respond to SIGALRM when a
    # timeout even occurs
    signal.signal(signal.SIGALRM, sighandler)

    # create the redis connections
    t_global.redcon = redis.Redis(host = t_global.args.roadblock_redis_server,
                                  port = 6379,
                                  password = t_global.args.roadblock_redis_password)
    t_global.pubsubcon = t_global.redcon.pubsub(ignore_subscribe_messages = True)

    print("Roadblock UUID: %s" % (t_global.args.roadblock_uuid))
    print("Role: %s" % (t_global.args.roadblock_role))
    if t_global.args.roadblock_role == 'follower':
        print("Follower ID: %s" % (t_global.args.roadblock_follower_id))
        print("Leader ID:   %s" % (t_global.args.roadblock_leader_id))
    elif t_global.args.roadblock_role == 'leader':
        print("Leader ID: %s" % (t_global.args.roadblock_leader_id))
        print("Followers: %s" % (t_global.args.roadblock_followers))

    # check if the roadblock was previously created and already timed
    # out -- ie. I am very late
    if t_global.redcon.exists(t_global.args.roadblock_uuid + '__timedout'):
        do_timeout()

    # set the default timeout alarm
    signal.alarm(t_global.args.roadblock_timeout)
    mytime = calendar.timegm(time.gmtime())
    print("Current Time: %s" % (datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC")))
    cluster_timeout = mytime + t_global.args.roadblock_timeout

    # check if the roadblock has been initialized yet
    if t_global.redcon.msetnx({t_global.args.roadblock_uuid: mytime}):
        # i am creating the roadblock
        print("Initializing roadblock as first arriving member")
        t_global.redcon.msetnx({t_global.args.roadblock_uuid + '__timeout-timestamp': message_to_str(message_build("all", "all", "timeout-ts", cluster_timeout))})
        t_global.redcon.rpush(t_global.args.roadblock_uuid + '__online-status', message_to_str(message_build("all", "all", "initialized")))
        t_global.redcon.rpush(t_global.args.roadblock_uuid + '__initialized', int(True))
        t_global.first = True
    else:
        # the roadblock already exists, make sure it is initialized
        # completely before proceeding
        print("I am not the first arriving member, waiting for roadblock initialization to complete")

        # wait until the initialized flag has been set for the roadblock
        while not t_global.redcon.exists(t_global.args.roadblock_uuid + '__initialized'):
            time.sleep(1)
            print(".")

        print("Roadblock is initialized")

        # retrieve the posted timeout so that the same timestamp is
        # shared across all members of the roadblock for timing out --
        # this allows even late arrivals to share the same timeout
        # timestamp
        cluster_timeout_message = t_global.redcon.get(t_global.args.roadblock_uuid + '__timeout-timestamp')

        cluster_timeout_message = message_from_str(cluster_timeout_message)
        if message_validate(cluster_timeout_message) and message_for_me(cluster_timeout_message):
            message_command = message_get_command(cluster_timeout_message)
            if message_command == "timeout-ts":
                message_value = int(message_get_value(cluster_timeout_message))

                cluster_timeout = message_value

                mytime = calendar.timegm(time.gmtime())
                timeout = mytime - cluster_timeout
                if timeout < 0:
                    # the timeout is still in the future, update the alarm
                    signal.alarm(abs(timeout))
                    print("The new timeout value is in %d seconds" % (abs(timeout)))
                else:
                    signal.alarm(0)
                    # the timeout has already passed
                    print("The timeout has already occurred")
                    cleanup()
                    return(-2)
            else:
                print("Received non 'timeout-ts' command for cluster timeout message")
        else:
            print("Invalid timeout message received")

    print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))

    status_index = -1
    get_out = False
    while True:
        # retrieve unprocessed status messages
        status_list = t_global.redcon.lrange(t_global.args.roadblock_uuid + '__online-status', status_index+1, -1)

        # process any retrieved status messages
        if len(status_list):
            for msg_str in status_list:
                status_index += 1
                if t_global.args.debug:
                    print("received msg=[%s] status_index=[%d]" % (msg_str, status_index))

                msg = message_from_str(msg_str)

                if not message_validate(msg):
                    print("Received a message which did not validate! [%s]" % (msg_str))
                    continue
                else:
                    if not message_for_me(msg):
                        print("Received a message which is not for me! [%s]" % (msg_str))
                        continue

                    msg_command = message_get_command(msg)

                    if msg_command == "initialized":
                        if t_global.args.roadblock_role == 'leader':
                            # listen for messages published from the followers
                            t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + '__followers')

                            print("Signaling online")
                            t_global.redcon.rpush(t_global.args.roadblock_uuid + '__online-status', message_to_str(message_build("all", "all", "leader-online")))

                            get_out = True
                    elif msg_command == "leader-online":
                        if t_global.args.roadblock_role == 'follower':
                            print("Received online status from leader")

                            # listen for messages published from the leader
                            t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + '__leader')

                            if t_global.args.abort:
                                print("Publishing abort message")
                                t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', message_to_str(message_build("leader", t_global.args.roadblock_leader_id, "abort")))
                            else:
                                print("Publishing ready message")
                                t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', message_to_str(message_build("leader", t_global.args.roadblock_leader_id, "ready")))

                            get_out = True

        if get_out:
            break
        else:
            time.sleep(1)

    if t_global.args.roadblock_role == 'leader':
        for msg in t_global.pubsubcon.listen():
            msg_str = msg['data'].decode()
            if t_global.args.debug:
                print("received msg=[%s]" % (msg_str))

            msg = message_from_str(msg_str)

            if not message_validate(msg):
                print("Received a message which did not validate! [%s]" % (msg_str))
                continue
            else:
                if not message_for_me(msg):
                    print("Received a message which is not for me! [%s]" % (msg_str))
                    continue

            msg_command = message_get_command(msg)

            if msg_command == 'ready':
                msg_sender = message_get_sender(msg)

                if msg_sender in followers['ready']:
                    print("Received ready message from '%s'" % (msg_sender))
                    del followers['ready'][msg_sender]
                elif msg_sender in t_global.args.roadblock_followers:
                    print("Did I already process this ready message from follower '%s'?" % (msg_sender))
                else:
                    print("Received ready message from unknown follower '%s'" % (msg_sender))

            elif msg_command == 'abort':
                msg_sender = message_get_sender(msg)

                leader_abort = True
                if msg_sender in followers['ready']:
                    print("Received abort message from '%s'" % (msg_sender))
                    del followers['ready'][msg_sender]
                elif msg_sender in t_global.args.roadblock_followers:
                    print("Did I already process this abort message from follower '%s'?" % (msg_sender))
                else:
                    print("Received abort message from unknown follower '%s'" % (msg_sender))

            if len(followers['ready']) == 0:
                print("All followers ready")
                if leader_abort:
                    print("Publishing go-abort message")
                    t_global.redcon.publish(t_global.args.roadblock_uuid + '__leader', message_to_str(message_build("all", "all", "go-abort")))
                else:
                    print("Publishing go message")
                    t_global.redcon.publish(t_global.args.roadblock_uuid + '__leader', message_to_str(message_build("all", "all", "go")))
                break
    elif t_global.args.roadblock_role == 'follower':
        for msg in t_global.pubsubcon.listen():
            msg_str = msg['data'].decode()
            if t_global.args.debug:
                print("received msg=[%s]" % (msg_str))

            msg = message_from_str(msg_str)

            if not message_validate(msg):
                print("Received a message which did not validate! [%s]" % (msg_str))
                continue
            else:
                if not message_for_me(msg):
                    print("Received a message which is not for me! [%s]" % (msg_str))
                    continue

            msg_command = message_get_command(msg)

            if msg_command == 'go':
                print("Received go message from leader")
                # stop listening for messages published from the leader
                t_global.pubsubcon.unsubscribe(t_global.args.roadblock_uuid + '__leader')
                print("Publishing gone message")
                t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', message_to_str(message_build("leader", t_global.args.roadblock_leader_id, "gone")))
                print("Cleaning up")
                cleanup()
                print("Exiting")
                return(0)
            elif msg_command == 'go-abort':
                print("Received abort message from leader")
                # stop listening for messages published from the leader
                t_global.pubsubcon.unsubscribe(t_global.args.roadblock_uuid + '__leader')
                print("Publishing gone message")
                t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', message_to_str(message_build("leader", t_global.args.roadblock_leader_id, "gone")))
                print("Cleaning up")
                cleanup()
                print("Exiting")
                return(-3)

    if t_global.args.roadblock_role == 'leader':
        for msg in t_global.pubsubcon.listen():
            msg_str = msg['data'].decode()
            if t_global.args.debug:
                print("received msg=[%s]" % (msg_str))

            msg = message_from_str(msg_str)

            if not message_validate(msg):
                print("Received a message which did not validate! [%s]" % (msg_str))
                continue
            else:
                if not message_for_me(msg):
                    print("Received a message which is not for me! [%s]" % (msg_str))
                    continue

            msg_command = message_get_command(msg)

            if msg_command == 'gone':
                msg_sender = message_get_sender(msg)

                if msg_sender in followers['gone']:
                    print("Received gone message from '%s'" % (msg_sender))
                    del followers['gone'][msg_sender]
                elif msg_sender in t_global.args.roadblock_followers:
                    print("Did I already process this gone message from follower '%s'?" % (msg_sender))
                else:
                    print("Received gone message from unknown follower '%s'" % (msg_sender))

            if len(followers['gone']) == 0:
                print("All followers gone")
                # stop listening for messages published from the followers
                t_global.pubsubcon.unsubscribe(t_global.args.roadblock_uuid + '__followers')
                print("Cleaning up")
                cleanup()
                if leader_abort:
                    print("Exiting with abort")
                    return(-3)
                else:
                    print("Exiting")
                    return(0)

if __name__ == "__main__":
    exit(main())
