#!/usr/bin/python3

import argparse
import datetime
import time
import calendar
import socket
import signal
import hashlib
import json
import uuid
import threading
import logging
import sys

import redis
import jsonschema

# define some global variables
class t_global(object):
    alarm_active = False
    args = None
    con_pool = None
    con_pool_state = False
    con_watchdog_exit = None
    con_watchdog = None
    redcon = None
    pubsubcon = None
    initiator = False
    mirror_busB = False
    schema = None
    user_schema = None
    my_id = None
    watch_busA = True
    watch_busB = False
    leader_abort = False
    follower_abort = False
    initiator_type = None
    initiator_id = None
    followers = { "online": {},
                  "ready": {},
                  "gone": {} }
    processed_messages = {}
    messages = { "sent": [],
                 "received": [] }
    message_log = None
    user_messages = None
    log_debug_format =  '[%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'
    log = None

def message_to_str(message):
    '''Converts a message into a JSON string'''

    return json.dumps(message, separators=(",", ":"))

def message_from_str(message):
    '''Convert a JSON string into a message'''

    return json.loads(message)

def message_build(recipient_type, recipient_id, command, value=None):
    '''Create a generic message using the ID and role of the sender'''

    return message_build_custom(t_global.args.roadblock_role, t_global.my_id, recipient_type, recipient_id, command, value)

def message_build_custom(sender_type, sender_id, recipient_type, recipient_id, command, value=None):
    '''Create a custom message with any user specified values'''

    message = {
        "payload": {
            "uuid": str(uuid.uuid4()),
            "roadblock": t_global.args.roadblock_uuid,
            "sender": {
                "timestamp": calendar.timegm(time.gmtime()),
                "type": sender_type,
                "id": sender_id,
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
        if command == "user-string":
            message["payload"]["message"]["user-string"] = value
        elif command == "user-object":
            message["payload"]["message"]["user-object"] = value
        else:
            message["payload"]["message"]["value"] = str(value)

    message["checksum"] = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

    return message

def message_validate(message):
    '''Validate that a received message matches the message schema and that it is not corrupted'''

    try:
        jsonschema.validate(instance=message, schema=t_global.schema)

        checksum = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

        return bool(message["checksum"] == checksum)
    except jsonschema.exceptions.SchemaError:
        return False

def message_for_me(message):
    '''Determine if a received message was intended for me'''

    message["payload"]["recipient"]["timestamp"] = calendar.timegm(time.gmtime())

    if message["payload"]["sender"]["id"] == t_global.my_id and message["payload"]["sender"]["type"] == t_global.args.roadblock_role:
        # I'm the sender so ignore it
        return False
    elif message["payload"]["recipient"]["type"] == "all":
        return True
    elif message["payload"]["recipient"]["type"] == t_global.args.roadblock_role and message["payload"]["recipient"]["id"] == t_global.my_id:
        return True
    else:
        return False

def message_get_command(message):
    '''Extract the command from a message'''

    return message["payload"]["message"]["command"]

def message_get_value(message):
    '''Extract a value from the a message'''

    return message["payload"]["message"]["value"]

def message_get_sender(message):
    '''Extract the sender ID from a message'''

    return message["payload"]["sender"]["id"]

def message_get_sender_type(message):
    '''Extract the sender type from a message'''

    return message["payload"]["sender"]["type"]

def message_get_uuid(message):
    '''Extract a message's UUID'''

    return message["payload"]["uuid"]

def define_usr_msg_schema():
    '''Define the schema used to validate user messages'''

    t_global.user_schema = {
        "type": "array",
        "minItems": 1,
        "uniqueItems": True,
        "items": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "recipient": {
                            "$ref": "#/definitions/recipient"
                        },
                        "user-string": {
                            "type": "string",
                            "minLength": 1
                        }
                    },
                    "required": [
                        "recipient",
                        "user-string"
                    ],
                    "additionalProperties": False
                },
                {
                    "type": "object",
                    "properties": {
                        "recipient": {
                            "$ref": "#/definitions/recipient"
                        },
                        "user-object": {
                            "type": "object"
                        }
                    },
                    "required": [
                        "recipient",
                        "user-object"
                    ],
                    "additionalProperties": False
                }
            ]
        },
        "definitions": {
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
                    "type",
                    "id"
                ],
                "additionalProperties": False
            }
        }
    }

def define_msg_schema():
    '''Define the schema used to validate roadblock protocol messages'''

    t_global.schema = {
        "type": "object",
        "properties": {
            "payload": {
                "type": "object",
                "properties": {
                    "uuid": {
                        "type": "string",
                        "minLength": 36,
                        "maxLength": 36
                    },
                    "roadblock": {
                        "type": "string",
                        "enum": [
                            t_global.args.roadblock_uuid
                        ]
                    },
                    "sender": {
                        "type": "object",
                        "properties": {
                            "timestamp": {
                                "type": "integer"
                            },
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
                            "timestamp",
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
                                    "switch-buses",
                                    "leader-online",
                                    "follower-online",
                                    "all-online",
                                    "initiator-info",
                                    "follower-ready",
                                    "follower-ready-abort",
                                    "all-ready",
                                    "all-go",
                                    "all-abort",
                                    "follower-gone",
                                    "all-gone",
                                    "user-string",
                                    "user-object"
                                ]
                            },
                            "value": {
                                "type": "string",
                                "minLength": 1
                            },
                            "user-string": {
                                "type": "string",
                                "minLength": 1
                            },
                            "user-object": {}
                        },
                        "required": [
                            "command"
                        ],
                        "additionalProperties": False,
                        "allOf": [
                            {
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
                            },
                            {
                                "if": {
                                    "properties": {
                                        "command": {
                                            "enum": [
                                                "user-string"
                                            ]
                                        }
                                    }
                                },
                                "then": {
                                    "required": [
                                        "user-string"
                                    ]
                                }
                            },
                            {
                                "if": {
                                    "properties": {
                                        "command": {
                                            "enum": [
                                                "user-object"
                                            ]
                                        }
                                    }
                                },
                                "then": {
                                    "required": [
                                        "user-object"
                                    ]
                                }
                            }
                        ]
                    }
                },
                "required": [
                    "uuid",
                    "roadblock",
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

def send_user_messages():
    '''Send user defined messages'''

    if t_global.user_messages is not None:
        t_global.log.info("Sending user requested messages")
        user_msg_counter = 1
        for user_msg in t_global.user_messages:
            if "user-string" in user_msg:
                t_global.log.info("Sending user message %d: 'user-string'" % (user_msg_counter))
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-string", user_msg["user-string"]))
            elif "user-object" in user_msg:
                t_global.log.info("Sending user message %d: 'user-object'" % (user_msg_counter))
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-object", user_msg["user-object"]))

            user_msg_counter += 1

def message_handle (message):
    '''Roadblock protocol message handler'''

    msg_uuid = message_get_uuid(message)
    if msg_uuid in t_global.processed_messages:
        t_global.log.debug("I have already processed this message! [%s]" % (msg_uuid))
        return 0
    else:
        t_global.log.debug("adding uuid='%s' to the processed messages list" % (msg_uuid))
        t_global.processed_messages[msg_uuid] = True

        if t_global.message_log is not None:
            # if the message log is open then append messages to the queue
            # for later dumping
            t_global.messages["received"].append(message)

    msg_command = message_get_command(message)

    if msg_command == "timeout-ts":
        t_global.log.info("Received 'timeout-ts' message")

        cluster_timeout = int(message_get_value(message))

        mytime = calendar.timegm(time.gmtime())
        timeout = mytime - cluster_timeout

        if timeout < 0:
            signal.alarm(abs(timeout))
            t_global.alarm_active = True
            t_global.log.info("The new timeout value is in %d seconds" % (abs(timeout)))
            t_global.log.info("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))
        else:
            signal.alarm(0)
            t_global.alarm_active = False
            t_global.log.critical("The timeout has already occurred")
            return -2
    elif msg_command == "switch-buses":
        t_global.log.debug("switching busses")

        t_global.watch_busA = False
        t_global.watch_busB = True
    elif msg_command == "leader-online":
        if t_global.args.roadblock_role == "follower":
            t_global.log.debug("I see that the leader is online")
    elif msg_command == "follower-online":
        if t_global.args.roadblock_role == "leader":
            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["online"]:
                t_global.log.info("Received 'follower-online' message from '%s'" % (msg_sender))
                del t_global.followers["online"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Did I already process this 'follower-online' message from follower '%s'?" % (msg_sender))
            else:
                t_global.log.info("Received 'follower-online' message from unknown follower '%s'" % (msg_sender))

            if len(t_global.followers["online"]) == 0:
                t_global.log.info("Sending 'all-online' message")
                message_publish(message_build("all", "all", "all-online"))
                if t_global.initiator:
                    t_global.mirror_busB = False
                send_user_messages()
    elif msg_command == "all-online":
        if t_global.initiator:
            t_global.log.info("Initiator received 'all-online' message")
            t_global.mirror_busB = False
        else:
            t_global.log.info("Received 'all-online' message")

        send_user_messages()

        if t_global.args.roadblock_role == "follower":
            if t_global.args.abort:
                t_global.log.info("Sending 'follower-ready-abort' message")
                message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready-abort"))
            else:
                t_global.log.info("Sending 'follower-ready' message")
                message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready"))
    elif msg_command in ("follower-ready", "follower-ready-abort"):
        if t_global.args.roadblock_role == "leader":
            t_global.log.debug("leader got a 'follower-ready'")

            if msg_command == "follower-ready-abort":
                t_global.leader_abort = True

            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["ready"]:
                t_global.log.info("Received '%s' message from '%s'" % (msg_command, msg_sender))
                del t_global.followers["ready"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Received a redundant '%s' message from follower '%s'?" % (msg_command, msg_sender))
            else:
                t_global.log.info("Received '%s' message from unknown follower '%s'" % (msg_command, msg_sender))

            if len(t_global.followers["ready"]) == 0:
                t_global.log.info("Sending 'all-ready' message")
                message_publish(message_build("all", "all", "all-ready"))

                if t_global.leader_abort:
                    t_global.log.info("Sending 'all-abort' command")
                    message_publish(message_build("all", "all", "all-abort"))
                else:
                    t_global.log.info("Sending 'all-go' command")
                    message_publish(message_build("all", "all", "all-go"))
    elif msg_command == "all-ready":
        t_global.log.info("Received 'all-ready' message")
    elif msg_command in ("all-go", "all-abort"):
        if t_global.args.roadblock_role == "follower":
            if msg_command == "all-go":
                t_global.log.info("Received 'all-go' from leader")
            else:
                t_global.log.info("Received 'all-abort' from leader")
                t_global.follower_abort = True

            # tell the leader that I'm gone
            t_global.log.info("Sending 'follower-gone' message")
            message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-gone"))

            # signal myself to exit
            t_global.watch_busB = False
    elif msg_command == "follower-gone":
        if t_global.args.roadblock_role == "leader":
            t_global.log.debug("leader got a 'follower-gone' message")

            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["gone"]:
                t_global.log.info("Received 'follower-gone' message from '%s'" % (msg_sender))
                del t_global.followers["gone"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Received a redundant 'follower-gone' message from follower '%s'?" % (msg_sender))
            else:
                t_global.log.info("Received 'follower-gone' message from unknown follower '%s'" % (msg_sender))

            if len(t_global.followers["gone"]) == 0:
                # send a message that will probably not be observed by
                # anyone...but just in case...
                t_global.log.info("Sending 'all-gone' message")
                message_publish(message_build("all", "all", "all-gone"))

                # signal myself to exit
                t_global.watch_busB = False
    elif msg_command == "initiator-info":
        t_global.initiator_type = message_get_sender_type(message)
        t_global.initiator_id = message_get_sender(message)
        t_global.log.debug("Received an 'initiator-info' message with type='%s' and id='%s'" % (t_global.initiator_type, t_global.initiator_id))

    return 0

def message_publish(message):
    '''Publish messages for subscribers to receive'''

    message_str = message_to_str(message)

    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return the number of clients that receive the message
        # we expect it to be greater than zero, if not we retry
        ret_val = t_global.redcon.publish(t_global.args.roadblock_uuid + "__busB", message_str)

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to publish message '%s'" % (counter, message))

            backoff(counter)

    if t_global.message_log is not None:
        # if the message log is open then append messages to the queue
        # for later dumping
        t_global.messages["sent"].append(message)

    return 0

def key_delete(key):
    '''Delete a key from redis'''

    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return the number of keys deleted which is
        # expected to be one, if not we retry
        ret_val = t_global.redcon.delete(key)

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to delete key '%s'" % (counter, key))

            backoff(counter)

    return 0

def key_set_once(key, value):
    '''Set a key once in redis'''

    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return one on success, if not we retry
        ret_val = t_global.redcon.msetnx( { key: value } )

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to set key '%s' with value '%s' once" % (counter, key, value))

            backoff(counter)

    return 0

def key_set(key, value):
    '''Set a key in redis if it does not already exist'''

    # in this case we want to return the true/false behavior so the
    # caller knows if they set the key or it already existed
    return t_global.redcon.msetnx( { key: value } )

def key_check(key):
    '''Check if a key already exists in redis'''

    # inform the caller whether the key already existed or not
    return t_global.redcon.exists(key)

def list_append(key, value):
    '''Append a value to a list in redis'''

    ret_val = 0
    counter = 0
    while ret_val == 0:
        # if this call returns 0 then it failed somehow since it
        # should be the size of the list after we have added to it, so
        # we retry
        ret_val = t_global.redcon.rpush(key, value)

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to append value '%s' to key '%s'" % (counter, value, key))

            backoff(counter)

    return ret_val

def list_fetch(key, offset):
    '''Fetch a list from redis'''

    # return the elements in the specified range (offset to end), this
    # could be empty so we can't really verify it
    return t_global.redcon.lrange(key, offset, -1)

def backoff(attempts):
    '''Control the rate of retries depending on how many have been attempted'''

    if attempts <= 10:
        # no back off, try really hard (spin)
        pass
    elif 10 < attempts <= 50:
        # back off a bit, don't spin as quickly
        time.sleep(0.1)
    else:
        # back off more, spin even slower
        time.sleep(0.5)

    return 0

def process_options ():
    '''Define the CLI argument parsing options'''

    parser = argparse.ArgumentParser(description="Roadblock provides multi entity (system, vm, container, etc.) synchronization.")

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
                        type = str)

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

    t_global.args = parser.parse_args()

    if t_global.args.log_level == 'debug':
        logging.basicConfig(level = logging.DEBUG, format = t_global.log_debug_format, stream = sys.stdout)
    elif t_global.args.log_level == 'normal':
        logging.basicConfig(level = logging.INFO, format = t_global.log_normal_format, stream = sys.stdout)

    t_global.log = logging.getLogger(__file__)


def cleanup():
    '''Cleanup the roadblock before exiting'''

    if t_global.alarm_active:
        t_global.log.info("Disabling timeout alarm")
        signal.alarm(0)

    if t_global.con_pool_state:
        if t_global.args.roadblock_role == "leader":
            t_global.log.info("Removing db objects specific to this roadblock")
            key_delete(t_global.args.roadblock_uuid)
            key_delete(t_global.args.roadblock_uuid + "__initialized")
            key_delete(t_global.args.roadblock_uuid + "__busA")

        t_global.log.info("Closing connection pool watchdog")
        t_global.con_watchdog_exit.set()
        t_global.con_watchdog.join()

        t_global.log.info("Closing connection pool")
        t_global.con_pool.disconnect()
        t_global.con_pool_state = False

    if t_global.message_log is not None:
        # if the message log is open then dump the message queue and
        # close the file handle
        print("%s\n" % (json.dumps(t_global.messages, indent = 4, separators=(',', ': '), sort_keys = False)), file=t_global.message_log)
        t_global.message_log.close()

    t_global.log.debug("Processed Messages:")
    for msg in t_global.processed_messages:
        t_global.log.debug("\t%s" % (msg))

    return 0

def get_followers_list(followers):
    '''Generate a list of the followers'''

    followers_list = ""

    for follower in followers:
        followers_list += follower + " "

    return followers_list

def do_timeout():
    '''Handle a roadblock timeout event'''

    t_global.log.critical("Roadblock failed with timeout")

    if t_global.con_pool_state and t_global.initiator:
        # set a persistent flag that the roadblock timed out so that
        # any late arriving members know that the roadblock has
        # already failed.  done by the first member since that is the
        # only member that is guaranteed to have actually reached the
        # roadblock and be capable of setting this.
        key_set_once(t_global.args.roadblock_uuid + "__timedout", int(True))

    cleanup()

    if t_global.args.roadblock_role == "leader":
        if len(t_global.followers["online"]) != 0:
            t_global.log.critical("These followers never reached 'online': %s" % (get_followers_list(t_global.followers["online"])))
        elif len(t_global.followers["ready"]) != 0:
            t_global.log.critical("These followers never reached 'ready': %s" % (get_followers_list(t_global.followers["ready"])))
        elif len(t_global.followers["gone"]) != 0:
            t_global.log.critical("These followers never reach 'gone': %s" % (get_followers_list(t_global.followers["gone"])))

    sys.exit(-3)


def sighandler(signum, frame):
    '''Handle signals delivered to the process'''

    if signum == 14: # SIGALRM
        t_global.alarm_active = False
        do_timeout()
    else:
        t_global.log.info("Signal handler called with signal", signum)

    return 0

def connection_watchdog():
    '''Check if the redis connection is still open'''

    while not t_global.con_watchdog_exit.is_set():
        time.sleep(1)
        try:
            if t_global.con_pool_state:
                t_global.redcon.ping()
            else:
                t_global.log.error("con_pool_state=False")
        except redis.exceptions.ConnectionError as con_error:
            t_global.con_pool_state = False
            t_global.log.error("%s" % (con_error))
            t_global.log.error("Redis connection failed")

    return 0

def main():
    '''Main control block'''

    process_options()

    if len(t_global.args.roadblock_leader_id) == 0:
        t_global.log.critical("You must specify the leader's ID using --leader-id")
        return -1

    if t_global.args.roadblock_role == "leader":
        if len(t_global.args.roadblock_followers) == 0:
            t_global.log.critical("There must be at least one follower")
            return -1
        if t_global.args.abort:
            t_global.leader_abort = True

        # build some hashes for easy tracking of follower status
        for follower in t_global.args.roadblock_followers:
            t_global.followers["online"][follower] = True
            t_global.followers["ready"][follower] = True
            t_global.followers["gone"][follower] = True

    if t_global.args.roadblock_role == "follower":
        t_global.my_id = t_global.args.roadblock_follower_id
    elif t_global.args.roadblock_role == "leader":
        t_global.my_id = t_global.args.roadblock_leader_id

    if t_global.args.message_log is not None:
        # open the message log, if specified
        try:
            t_global.message_log = open(t_global.args.message_log, "w")
        except IOError:
            t_global.log.critical("Could not open message log '%s' for writing!" % (t_global.args.message_log))
            return -1

    define_msg_schema()
    define_usr_msg_schema()

    if t_global.args.user_messages is not None:
        # load the user messages, if specified
        try:
            with open(t_global.args.user_messages, "r") as user_messages:
                t_global.user_messages = json.load(user_messages)
        except IOError:
            t_global.log.critical("Could not load the user messages '%s'!" % (t_global.args.user_messages))
            return -1

        try:
            jsonschema.validate(instance=t_global.user_messages, schema=t_global.user_schema)
        except jsonschema.exceptions.SchemaError as e:
            t_global.log.critical(e)
            t_global.log.critical("Could not JSON validate the user messages!")
            return -1

    # define a signal handler that will respond to SIGALRM when a
    # timeout even occurs
    signal.signal(signal.SIGALRM, sighandler)

    # set the default timeout alarm
    signal.alarm(t_global.args.roadblock_timeout)
    t_global.alarm_active = True
    mytime = calendar.timegm(time.gmtime())
    t_global.log.info("Current Time: %s" % (datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC")))
    cluster_timeout = mytime + t_global.args.roadblock_timeout
    t_global.log.info("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))

    # create the redis connections
    while not t_global.con_pool_state:
        try:
            t_global.con_pool = redis.ConnectionPool(host = t_global.args.roadblock_redis_server,
                                                     password = t_global.args.roadblock_redis_password,
                                                     port = 6379,
                                                     db = 0,
                                                     health_check_interval = 0)
            t_global.redcon = redis.Redis(connection_pool = t_global.con_pool)
            t_global.redcon.ping()
            t_global.con_pool_state = True
        except redis.exceptions.ConnectionError as con_error:
            t_global.log.error("%s" % (con_error))
            t_global.log.error("Redis connection could not be opened!")
            time.sleep(3)

    t_global.pubsubcon = t_global.redcon.pubsub(ignore_subscribe_messages = True)

    t_global.con_watchdog_exit = threading.Event()
    t_global.con_watchdog = threading.Thread(target = connection_watchdog, args = ())
    t_global.con_watchdog.start()

    t_global.log.info("Roadblock UUID: %s" % (t_global.args.roadblock_uuid))
    t_global.log.info("Role: %s" % (t_global.args.roadblock_role))
    if t_global.args.roadblock_role == "follower":
        t_global.log.info("Follower ID: %s" % (t_global.args.roadblock_follower_id))
        t_global.log.info("Leader ID: %s" % (t_global.args.roadblock_leader_id))
    elif t_global.args.roadblock_role == "leader":
        t_global.log.info("Leader ID: %s" % (t_global.args.roadblock_leader_id))
        t_global.log.info("Total followers: %d" % (len(t_global.args.roadblock_followers)))
        t_global.log.info("Followers: %s" % (t_global.args.roadblock_followers))
    if t_global.args.abort:
        t_global.log.info("Abort: True")
    else:
        t_global.log.info("Abort: False")

    # check if the roadblock was previously created and already timed
    # out -- ie. I am very late
    if key_check(t_global.args.roadblock_uuid + "__timedout"):
        t_global.log.critical("Detected previous timeout for this roadblock")
        do_timeout()

    # check if the roadblock has been initialized yet
    if key_set(t_global.args.roadblock_uuid, mytime):
        # i am creating the roadblock
        t_global.initiator = True
        t_global.log.info("Initiator: True")

        # set bus monitoring options
        t_global.watch_busA = False
        t_global.watch_busB = True
        t_global.mirror_busB = True

        # create busA
        list_append(t_global.args.roadblock_uuid + "__busA", message_to_str(message_build("all", "all", "initialized")))

        # create/subscribe to busB
        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + "__busB")

        # publish the cluster timeout to busB
        t_global.log.info("Sending 'timeout-ts' message")
        message_publish(message_build("all", "all", "timeout-ts", cluster_timeout))

        # publish the initiator information to busB
        t_global.log.info("Sending 'initiator-info' message")
        message_publish(message_build("all", "all", "initiator-info"))
        t_global.initiator_type = t_global.args.roadblock_role
        t_global.initiator_id = t_global.my_id

        list_append(t_global.args.roadblock_uuid + "__initialized", int(True))
    else:
        t_global.log.info("Initiator: False")

        # the roadblock already exists, make sure it is initialized
        # completely before proceeding
        t_global.log.info("Waiting for roadblock initialization to complete")

        # wait until the initialized flag has been set for the roadblock
        while not key_check(t_global.args.roadblock_uuid + "__initialized"):
            time.sleep(1)
            t_global.log.info(".")

        t_global.log.info("Roadblock is initialized")

        # subscribe to busB
        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + "__busB")

        # message myself on busB, once I receive this message on busA I will know I have processed all outstanding busA message and can move to monitoring busB
        t_global.log.debug("Sending 'switch-buses' message")
        message_publish(message_build_custom(t_global.args.roadblock_role, "switch-buses", t_global.args.roadblock_role, t_global.my_id, "switch-buses"))

    if t_global.args.roadblock_role == "follower":
        # tell the leader that I am online
        t_global.log.info("Sending 'follower-online' message")
        message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-online"))
    elif t_global.args.roadblock_role == "leader":
        # tell everyone that the leader is online
        t_global.log.info("Sending 'leader-online' message")
        message_publish(message_build("all", "all", "leader-online"))

    if t_global.initiator:
        # the initiator (first member to get to the roadblock) is
        # responsible for consuming messages from busB and copying
        # them onto busA so that they are preserved for other members
        # to receive once they arrive at the roadblock

        while t_global.mirror_busB:
            msg = t_global.pubsubcon.get_message()

            if not msg:
                time.sleep(0.001)
            else:
                msg_str = msg["data"].decode()
                t_global.log.debug("initiator received msg=[%s] on busB" % (msg_str))

                msg = message_from_str(msg_str)

                if not message_validate(msg):
                    t_global.log.error("initiator received a message which did not validate! [%s]" % (msg_str))
                else:
                    # copy the message over to busA
                    t_global.log.debug("initiator mirroring msg=[%s] to busA" % (msg_str))
                    list_append(t_global.args.roadblock_uuid + "__busA", msg_str)

                    if not message_for_me(msg):
                        t_global.log.debug("initiator received a message which is not for me! [%s]" % (msg_str))
                    else:
                        t_global.log.debug("initiator received a message for me! [%s]" % (msg_str))
                        ret_val = message_handle(msg)
                        if ret_val:
                            return ret_val

            if not t_global.mirror_busB:
                t_global.log.debug("initiator stopping busB mirroring to busA")
    else:
        msg_list_index = -1
        while t_global.watch_busA:
            # retrieve unprocessed messages from busA
            msg_list = list_fetch(t_global.args.roadblock_uuid + "__busA", msg_list_index+1)

            # process any retrieved messages
            if len(msg_list):
                for msg_str in msg_list:
                    msg_list_index += 1
                    t_global.log.debug("received msg=[%s] on busA with status_index=[%d]" % (msg_str, msg_list_index))

                    msg = message_from_str(msg_str)

                    if not message_validate(msg):
                        t_global.log.error("received a message which did not validate! [%s]" % (msg_str))
                    else:
                        if not message_for_me(msg):
                            t_global.log.debug("received a message which is not for me!")
                        else:
                            t_global.log.debug("received a message which is for me!")
                            ret_val = message_handle(msg)
                            if ret_val:
                                return ret_val

            if t_global.watch_busA:
                time.sleep(1)

    t_global.log.debug("moving to common busB watch loop")

    while t_global.watch_busB:
        msg = t_global.pubsubcon.get_message()

        if not msg:
            time.sleep(0.001)
        else:
            msg_str = msg["data"].decode()
            t_global.log.debug("received msg=[%s] on busB" % (msg_str))

            msg = message_from_str(msg_str)

            if not message_validate(msg):
                t_global.log.error("received a message which did not validate! [%s]" % (msg_str))
            else:
                if not message_for_me(msg):
                    t_global.log.debug("received a message which is not for me!")
                else:
                    t_global.log.debug("received a message for me!")
                    ret_val = message_handle(msg)
                    if ret_val:
                        return ret_val

    t_global.log.info("Cleaning up")
    cleanup()

    t_global.log.info("Exiting")
    if t_global.leader_abort is True or t_global.follower_abort is True:
        t_global.log.info("Roadblock Completed with an Abort")
        return -3
    else:
        t_global.log.info("Roadblock Completed Successfully")
        return 0

if __name__ == "__main__":
    sys.exit(main())
