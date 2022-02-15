#!/usr/bin/python3

'''Roadblock is a synchronization and message passing utility which relies on redis for communication'''

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
import re
import subprocess
import select
import shlex
import copy
import base64
import lzma

from dataclasses import dataclass
from pathlib import Path

import redis
import jsonschema

RC_SUCCESS=0
RC_ERROR=1
RC_INVALID_INPUT=2
RC_TIMEOUT=3
RC_ABORT=4
RC_HEARTBEAT_TIMEOUT=5
RC_ABORT_WAITING=6

# define some global variables
@dataclass
class global_vars:
    '''Global variables'''

    alarm_active = False
    args = None
    con_pool = None
    con_pool_state = False
    con_watchdog_exit = None
    con_watchdog = None
    wait_for_cmd = None
    wait_for_io_handler_exited = None
    wait_for_launcher_thread = None
    wait_for_process = None
    wait_for_monitor_thread = None
    wait_for_monitor_exit = None
    wait_for_monitor_start = None
    wait_for_waiting = False
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
    leader_abort_waiting = False
    roadblock_waiting = False
    follower_abort = False
    initiator_type = None
    initiator_id = None
    followers = { "online": {},
                  "ready": {},
                  "gone": {},
                  "waiting": {},
                  "waiting_backup": {},
                  "busy_waiting": {} }
    processed_messages = {}
    messages = { "sent": [],
                 "received": [] }
    message_log = None
    user_messages = []
    log_debug_format =  '[%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'
    log = None
    heartbeat_timeout = 30
    waiting_failed = False

def set_alarm(seconds):
    '''Set a SIGALRM to fire in `seconds` seconds'''

    signal.alarm(seconds)
    t_global.alarm_active = True

    return RC_SUCCESS

def disable_alarm():
    '''Disable an existing SIGALRM'''

    signal.alarm(0)
    t_global.alarm_active = True

    return RC_SUCCESS

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
        "tx_checksum": None
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

    message["tx_checksum"] = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

    return message

def message_validate(message):
    '''Validate that a received message matches the message schema and that it is not corrupted'''

    if t_global.args.message_validation == "none":
        return True

    if t_global.args.message_validation in [ "checksum", "all" ]:
        # the checksum comparison appears to be fairly light weight so do
        # that first -- no reason to continue with schema validation if
        # this fails
        if not bool(message["tx_checksum"] == message["rx_checksum"]):
            t_global.log.error("message failed checksum validation [%s]", message_to_str(message))
            return False
        else:
            t_global.log.debug("message passed checksum validation [%s]", message_to_str(message))

    if t_global.args.message_validation in [ "schema", "all" ]:
        try:
            jsonschema.validate(instance=message, schema=t_global.schema)
            t_global.log.debug("message passed schema validation [%s]", message_to_str(message))

        except jsonschema.exceptions.SchemaError:
            t_global.log.error("message failed schema validation [%s]", message_to_str(message))
            return False

    return True

def message_for_me(message):
    '''Determine if a received message was intended for me'''

    # grab the rx_timestamp ASAP after beginning to process the message
    rx_timestamp = calendar.timegm(time.gmtime())

    incomplete_message = False

    if not "payload" in message:
        incomplete_message = True
    elif not "recipient" in message["payload"]:
        incomplete_message = True
    elif not "sender" in message["payload"]:
        incomplete_message = True
    elif not "id" in message["payload"]["sender"]:
        incomplete_message = True
    elif not "recipient" in message["payload"]:
        incomplete_message = True
    elif not "type" in message["payload"]["recipient"]:
        incomplete_message = True

    if incomplete_message:
        t_global.log.error("incomplete message received [%s]", message_to_str(message))
        return False

    if message["payload"]["sender"]["id"] == t_global.my_id and message["payload"]["sender"]["type"] == t_global.args.roadblock_role:
        # I'm the sender so ignore it
        return False
    elif message["payload"]["recipient"]["type"] == "all":
        pass
    elif message["payload"]["recipient"]["type"] == t_global.args.roadblock_role and message["payload"]["recipient"]["id"] == t_global.my_id:
        pass
    else:
        return False

    if t_global.args.message_validation in [ "checksum", "all" ]:
        # get the rx_checksum before we modify the message by adding the rx_timestamp to it
        message["rx_checksum"] = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

    # embed the rx_timestamp into the message
    message["payload"]["recipient"]["timestamp"] = rx_timestamp

    return True

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
                    "timestamp": {
                        "type": "integer"
                    },
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
                            "timestamp": {
                                "type": "integer"
                            },
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
                                    "follower-ready-waiting",
                                    "leader-heartbeat",
                                    "follower-heartbeat",
                                    "follower-heartbeat-complete",
                                    "follower-heartbeat-complete-failed",
                                    "follower-waiting-complete",
                                    "follower-waiting-complete-failed",
                                    "heartbeat-timeout",
                                    "all-ready",
                                    "all-go",
                                    "all-abort",
                                    "all-wait",
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
            "tx_checksum": {
                "type": "string",
                "minLength": 64,
                "maxLength": 64
            },
            "rx_checksum": {
                "type": "string",
                "minLength": 64,
                "maxLength": 64
            }
        },
        "required": [
            "payload",
            "tx_checksum"
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
                t_global.log.info("Sending user message %d: 'user-string'", user_msg_counter)
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-string", user_msg["user-string"]))
            elif "user-object" in user_msg:
                t_global.log.info("Sending user message %d: 'user-object'", user_msg_counter)
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-object", user_msg["user-object"]))

            user_msg_counter += 1

def message_handle (message):
    '''Roadblock protocol message handler'''

    msg_uuid = message_get_uuid(message)
    if msg_uuid in t_global.processed_messages:
        t_global.log.debug("I have already processed this message! [%s]", msg_uuid)
        return RC_SUCCESS
    else:
        t_global.log.debug("adding uuid='%s' to the processed messages list", msg_uuid)
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
            set_alarm(abs(timeout))
            t_global.log.info("The new timeout value is in %d seconds", abs(timeout))
            t_global.log.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))
        else:
            disable_alarm()
            t_global.log.critical("The timeout has already occurred")
            return RC_TIMEOUT
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
                t_global.log.info("Received 'follower-online' message from '%s'", msg_sender)
                del t_global.followers["online"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Did I already process this 'follower-online' message from follower '%s'?", msg_sender)
            else:
                t_global.log.info("Received 'follower-online' message from unknown follower '%s'", msg_sender)

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
                if t_global.args.wait_for is not None and t_global.wait_for_process is not None and t_global.wait_for_process.poll() is None:
                    t_global.log.info("Sending 'follower-ready-waiting' message")
                    message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready-waiting"))
                else:
                    t_global.log.info("Sending 'follower-ready' message")
                    message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready"))
    elif msg_command in ("follower-ready", "follower-ready-abort", "follower-ready-waiting"):
        if t_global.args.roadblock_role == "leader":
            t_global.log.debug("leader got a '%s' message", msg_command)

            msg_sender = message_get_sender(message)

            if msg_command == "follower-ready-abort":
                t_global.leader_abort = True
            elif msg_command == "follower-ready-waiting":
                t_global.roadblock_waiting = True

                t_global.log.info("Adding follower '%s' to the waiting list", msg_sender)
                t_global.followers["busy_waiting"][msg_sender] = True

            if msg_sender in t_global.followers["ready"]:
                t_global.log.info("Received '%s' message from '%s'", msg_command, msg_sender)
                del t_global.followers["ready"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Received a redundant '%s' message from follower '%s'?", msg_command, msg_sender)
            else:
                t_global.log.info("Received '%s' message from unknown follower '%s'", msg_command, msg_sender)

            if len(t_global.followers["ready"]) == 0:
                t_global.log.info("Sending 'all-ready' message")
                message_publish(message_build("all", "all", "all-ready"))

                if t_global.leader_abort:
                    t_global.log.info("Sending 'all-abort' command")
                    message_publish(message_build("all", "all", "all-abort"))
                elif t_global.roadblock_waiting:
                    t_global.log.info("Sending 'all-wait' command")
                    message_publish(message_build("all", "all", "all-wait"))

                    t_global.log.info("Disabling original timeout")
                    disable_alarm()

                    t_global.log.info("Disabling original timeout signal handler")
                    t_global.log.info("Enabling heartbeat timeout signal handler")
                    signal.signal(signal.SIGALRM, heartbeat_signal_handler)

                    t_global.log.info("Sending 'leader-heartbeat' message")
                    message_publish(message_build("all", "all", "leader-heartbeat"))

                    t_global.log.info("Staring heartbeat monitoring period")
                    set_alarm(t_global.heartbeat_timeout)
                else:
                    t_global.log.info("Sending 'all-go' command")
                    message_publish(message_build("all", "all", "all-go"))
    elif msg_command == "all-wait":
        if t_global.args.roadblock_role == "follower":
            t_global.log.info("Received 'all-wait' message")

            t_global.wait_for_waiting = True

            t_global.log.info("Disabling original timeout")
            disable_alarm()
    elif msg_command == "leader-heartbeat":
        if t_global.args.roadblock_role == "follower":
            t_global.log.info("Received 'leader-heartbeat' message")

            if t_global.args.simulate_heartbeat_timeout:
                t_global.log.critical("Not sending 'follower-heartbeat' because of heartbeat timeout simulation request")
            else:
                t_global.log.info("Sending 'follower-heartbeat' message")
                message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-heartbeat"))
    elif msg_command in ("follower-heartbeat", "follower-heartbeat-complete", "follower-heartbeat-complete-failed"):
        if t_global.args.roadblock_role == "leader":
            t_global.log.info("Received '%s' message", msg_command)

            msg_sender = message_get_sender(message)

            if msg_command in ("follower-heartbeat-complete", "follower-heartbeat-complete-failed"):
                if msg_sender in t_global.followers["busy_waiting"]:
                    t_global.log.info("Received heartbeat from follower '%s' and removing from the busy waiting list", msg_sender)
                    del t_global.followers["busy_waiting"][msg_sender]
                    del t_global.followers["waiting"][msg_sender]

                    if msg_command == "follower-heartbeat-complete-failed":
                        t_global.waiting_failed = True
                elif msg_sender in t_global.args.roadblock_followers:
                    t_global.log.warning("Received '%s' from a follower '%s' that is not busy waiting?", msg_command, msg_sender)
                else:
                    t_global.log.info("Received '%s' message from an unknown follower '%s'", msg_command, msg_sender)
            elif msg_command == "follower-heartbeat":
                if msg_sender in t_global.followers["waiting"]:
                    t_global.log.info("Received heartbeat from follower '%s'", msg_sender)
                    del t_global.followers["waiting"][msg_sender]
                elif msg_sender in t_global.args.roadblock_followers:
                    t_global.log.warning("Received a redundant heartbeat message from follower '%s'?", msg_sender)
                else:
                    t_global.log.warning("Received a heartbeat message from an unknown follower '%s'", msg_sender)

            if len(t_global.followers["busy_waiting"]) == 0 and len(t_global.followers["waiting"]) == 0:
                t_global.log.info("Disabling heartbeat timeout")
                disable_alarm()

                if t_global.waiting_failed:
                    t_global.leader_abort = True
                    t_global.log.info("Sending 'all-abort' command")
                    message_publish(message_build("all", "all", "all-abort"))
                else:
                    t_global.log.info("Sending 'all-go' command")
                    message_publish(message_build("all", "all", "all-go"))
    elif msg_command in ("follower-waiting-complete", "follower-waiting-complete-failed"):
        if t_global.args.roadblock_role == "leader":
            t_global.log.info("Received '%s' message", msg_command)

            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["busy_waiting"]:
                t_global.log.info("Follower '%s' is no longer busy waiting", msg_sender)
                del t_global.followers["busy_waiting"][msg_sender]

                if msg_command == "follower-waiting-complete-failed":
                    t_global.waiting_failed = True
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Received '%s' from a follower '%s' that is not busy waiting?", msg_command, msg_sender)
            else:
                t_global.log.info("Received '%s' message from an unknown follower '%s'", msg_command, msg_sender)

            if len(t_global.followers["busy_waiting"]) == 0 or t_global.waiting_failed:
                t_global.log.info("Disabling heartbeat timeout")
                disable_alarm()

                if t_global.waiting_failed:
                    t_global.leader_abort_waiting = True
                    t_global.log.info("Sending 'all-abort' command")
                    message_publish(message_build("all", "all", "all-abort"))
                else:
                    t_global.log.info("Sending 'all-go' command")
                    message_publish(message_build("all", "all", "all-go"))
    elif msg_command == "heartbeat-timeout":
        if t_global.args.roadblock_role == "follower":
            t_global.log.info("Received '%s' message", msg_command)

            t_global.log.critical("Roadblock failed due to a heartbeat timeout")

            timeout_internals()

            sys.exit(RC_HEARTBEAT_TIMEOUT)
    elif msg_command == "all-ready":
        t_global.log.info("Received 'all-ready' message")
    elif msg_command in ("all-go", "all-abort"):
        if t_global.args.roadblock_role == "follower":
            if msg_command == "all-go":
                t_global.log.info("Received 'all-go' from leader")
            else:
                t_global.log.info("Received 'all-abort' from leader")
                t_global.follower_abort = True

                if t_global.args.wait_for is not None and t_global.wait_for_process is not None:
                    t_global.log.critical("Killing wait_for process")
                    t_global.wait_for_process.kill()

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
                t_global.log.info("Received 'follower-gone' message from '%s'", msg_sender)
                del t_global.followers["gone"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                t_global.log.warning("Received a redundant 'follower-gone' message from follower '%s'?", msg_sender)
            else:
                t_global.log.info("Received 'follower-gone' message from unknown follower '%s'", msg_sender)

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
        t_global.log.debug("Received an 'initiator-info' message with type='%s' and id='%s'", t_global.initiator_type, t_global.initiator_id)

    return RC_SUCCESS

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
            t_global.log.warning("Failed attempt %d to publish message '%s'", counter, message)

            backoff(counter)

    if t_global.message_log is not None:
        # if the message log is open then append messages to the queue
        # for later dumping
        t_global.messages["sent"].append(message)

    return RC_SUCCESS

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
            t_global.log.warning("Failed attempt %d to delete key '%s'", counter, key)

            backoff(counter)

    return RC_SUCCESS

def key_set_once(key, value):
    '''Set a key once in redis'''

    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return one on success, if not we retry
        ret_val = t_global.redcon.msetnx( { key: value } )

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to set key '%s' with value '%s' once", counter, key, value)

            backoff(counter)

    return RC_SUCCESS

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
        counter += 1
        # if this call returns 0 then it failed somehow since it
        # should be the size of the list after we have added to it, so
        # we retry
        ret_val = t_global.redcon.rpush(key, value)

        if ret_val == 0:
            t_global.log.warning("Failed attempt %d to append value '%s' to key '%s'", counter, value, key)

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

    return RC_SUCCESS

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


def cleanup():
    '''Cleanup the roadblock before exiting'''

    if t_global.alarm_active:
        t_global.log.info("Disabling timeout alarm")
        disable_alarm()

    if t_global.args.wait_for is not None:
        t_global.log.info("Closing wait_for monitor thread")
        t_global.wait_for_monitor_exit.set()
        t_global.wait_for_monitor_thread.join()

        t_global.log.info("Closing wait_for launcher thread")
        t_global.wait_for_launcher_thread.join()

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
        print(f"{json.dumps(t_global.messages, indent = 4, separators=(',', ': '), sort_keys = False)}\n", file=t_global.message_log)
        t_global.message_log.close()

    t_global.log.debug("Processed Messages:")
    for msg in t_global.processed_messages:
        t_global.log.debug("\t%s", msg)

    return RC_SUCCESS

def get_followers_list(followers):
    '''Generate a list of the followers'''

    followers_list = ""

    for follower in followers:
        followers_list += follower + " "

    return followers_list


def timeout_internals():
    '''Steps common to both types of timeout events'''

    if t_global.args.wait_for is not None:
        if t_global.wait_for_process is not None:
            if t_global.wait_for_process.poll() is None:
                t_global.log.critical("Killing wait_for process")
                t_global.wait_for_process.kill()
            else:
                t_global.log.info("wait_for process has already exited")
        else:
            t_global.log.critical("The wait-for process object is missing")

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
            t_global.log.critical("These followers never reached 'online': %s", get_followers_list(t_global.followers["online"]))
        elif len(t_global.followers["ready"]) != 0:
            t_global.log.critical("These followers never reached 'ready': %s", get_followers_list(t_global.followers["ready"]))
        elif len(t_global.followers["busy_waiting"]) != 0 or len(t_global.followers["waiting"]) != 0:
            if len(t_global.followers["busy_waiting"]) != 0:
                t_global.log.critical("These followers were still 'busy waiting': %s", get_followers_list(t_global.followers["busy_waiting"]))

            if len(t_global.followers["waiting"]) != 0:
                t_global.log.critical("These followers did not respond to the heartbeat and were still 'waiting': %s", get_followers_list(t_global.followers["waiting"]))
        elif len(t_global.followers["gone"]) != 0:
            t_global.log.critical("These followers never reach 'gone': %s", get_followers_list(t_global.followers["gone"]))

    return RC_SUCCESS


def do_heartbeat_signal():
    '''Handle a heartbeat timeout event'''

    if len(t_global.followers["waiting"]) != 0:
        t_global.log.critical("Failed ending current heartbeat monitoring period -> heartbeat timeout")

        t_global.log.info("Sending 'heartbeat-timeout' message")
        message_publish(message_build("all", "all", "heartbeat-timeout"))

        timeout_internals()

        sys.exit(RC_HEARTBEAT_TIMEOUT)
    else:
        t_global.log.info("Successfully ending current heartbeat monitoring period")
        disable_alarm()

        # rebuild the tracking list by copying from a backup
        t_global.followers["waiting"] = copy.deepcopy(t_global.followers["waiting_backup"])

        t_global.log.info("Sending 'leader-heartbeat' message")
        message_publish(message_build("all", "all", "leader-heartbeat"))

        t_global.log.info("Starting new heartbeat monitoring period")
        set_alarm(t_global.heartbeat_timeout)

    return RC_SUCCESS

def do_timeout():
    '''Handle a roadblock timeout event'''

    t_global.log.critical("Roadblock failed with timeout")

    timeout_internals()

    sys.exit(RC_TIMEOUT)


def timeout_signal_handler(signum, frame):
    '''Handle roadblock timeout signals delivered to the process'''

    if signum == 14: # SIGALRM
        t_global.alarm_active = False
        do_timeout()
    else:
        t_global.log.info("Timeout Signal handler called with signal %d", signum)

    return RC_SUCCESS

def heartbeat_signal_handler(signum, frame):
    '''Handle heartbeat timeout signals delivered to the process'''

    if signum == 14: # SIGALRM
        t_global.alarm_active = False
        do_heartbeat_signal()
    else:
        t_global.log.info("Heartbeat Signal handler called with signal %d", signum)

    return RC_SUCCESS

def sigint_handler(signum, frame):
    '''Handle a SIGINT/CTRL-C'''

    if signum == 2: # SIGINT
        t_global.log.critical("Exiting due to SIGINT")
        cleanup()
        sys.exit(RC_ERROR)
    else:
        t_global.log.info("SIGINT Signal handler called with signal %d", signum)

    return RC_SUCCESS

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
            t_global.log.error("%s", con_error)
            t_global.log.error("Redis connection failed")

    return RC_SUCCESS

def wait_for_process_io_handler():
    '''Handle the output logging of a --wait-for program/script process'''

    t_global.wait_for_io_handler_exited = threading.Event()

    with open(t_global.args.wait_for_log, "w", encoding = "ascii") as wait_for_log_fh:
        # process lines while the process is running in a non-blocking fashion
        while t_global.wait_for_process.poll() is None:
            t_global.wait_for_process.stdout.flush()
            ready_streams = select.select([t_global.wait_for_process.stdout], [], [], 1)
            if t_global.wait_for_process.stdout in ready_streams[0]:
                for line in t_global.wait_for_process.stdout:
                    wait_for_log_fh.write(line)

        # process any remaining lines that haven't been handled yet
        # (this will not block now so it is simpler than above)
        for line in t_global.wait_for_process.stdout:
            wait_for_log_fh.write(line)

    t_global.wait_for_io_handler_exited.set()

    return RC_SUCCESS

def wait_for_process_monitor():
    '''Monitor the status of a --wait-for program/script process'''

    t_global.log.info("wait_for monitor is waiting")

    t_global.wait_for_monitor_start.wait()

    t_global.log.info("wait_for monitor is starting")

    while not t_global.wait_for_monitor_exit.is_set():
        if t_global.wait_for_process is None:
            t_global.log.critical("There is no wait_process to monitor")
            t_global.wait_for_monitor_exit.set()
        else:
            if t_global.wait_for_process.poll() is None:
                #t_global.log.info("wait_for process is still running")
                time.sleep(1)
            else:
                t_global.log.info("wait_for process is complete")
                t_global.wait_for_monitor_exit.set()

                if t_global.wait_for_waiting:
                    if t_global.wait_for_process.returncode != 0:
                        t_global.wait_for_io_handler_exited.wait()

                        log_contents = ""
                        with open(t_global.args.wait_for_log, "r", encoding = "ascii") as wait_for_log_fh:
                            log_contents = str(base64.b64encode(lzma.compress(wait_for_log_fh.read().encode("ascii"))), "ascii")

                        t_global.log.critical("Sending 'follower-waiting-complete-failed' message")
                        message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-waiting-complete-failed", value = log_contents))
                    else:
                        t_global.log.info("Sending 'follower-waiting-complete' message")
                        message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-waiting-complete"))

    t_global.log.info("wait_for monitor is exiting")

    return RC_SUCCESS

def wait_for_process_launcher():
    '''Handle the execution of a --wait-for program/script'''

    ret_val = -1

    try:
        t_global.wait_for_process = subprocess.Popen(t_global.wait_for_cmd,
                                                     bufsize = 0,
                                                     encoding = 'ascii',
                                                     stdout = subprocess.PIPE,
                                                     stderr = subprocess.STDOUT)

        if t_global.wait_for_process is None:
            t_global.log.critical("The wait for process failed to launch")
            ret_val = 1
            t_global.wait_for_monitor_start.set()
        else:
            t_global.wait_for_monitor_start.set()
            wait_for_io_thread = threading.Thread(target = wait_for_process_io_handler, args = ())
            wait_for_io_thread.start()

            ret_val = t_global.wait_for_process.wait()
            wait_for_io_thread.join()

    except PermissionError:
        t_global.log.critical("Received a permission error when attempting to execute --wait-for command")
        ret_val = 1
        t_global.wait_for_monitor_start.set()

    t_global.log.info("wait_for process exited with return code %d", ret_val)

    return RC_SUCCESS

def main():
    '''Main control block'''

    #catch SIGINT/CTRL-C
    signal.signal(signal.SIGINT, sigint_handler)

    process_options()

    if len(t_global.args.roadblock_leader_id) == 0:
        t_global.log.critical("You must specify the leader's ID using --leader-id")
        return RC_INVALID_INPUT

    if t_global.args.roadblock_role == "leader":
        if len(t_global.args.roadblock_followers) == 0:
            t_global.log.critical("There must be at least one follower")
            return RC_INVALID_INPUT
        if t_global.args.abort:
            t_global.leader_abort = True

        # build some hashes for easy tracking of follower status
        for follower in t_global.args.roadblock_followers:
            t_global.followers["online"][follower] = True
            t_global.followers["ready"][follower] = True
            t_global.followers["gone"][follower] = True
            t_global.followers["waiting"][follower] = True
            t_global.followers["waiting_backup"][follower] = True

    if t_global.args.roadblock_role == "follower":
        t_global.my_id = t_global.args.roadblock_follower_id
    elif t_global.args.roadblock_role == "leader":
        t_global.my_id = t_global.args.roadblock_leader_id

    if t_global.args.message_log is not None:
        # open the message log, if specified
        try:
            t_global.message_log = open(t_global.args.message_log, "w", encoding="ascii")
        except IOError:
            t_global.log.critical("Could not open message log '%s' for writing!", t_global.args.message_log)
            return RC_INVALID_INPUT

    define_msg_schema()
    define_usr_msg_schema()

    if t_global.args.user_messages is not None:
        # load the user messages, if specified
        try:
            with open(t_global.args.user_messages, "r", encoding="ascii") as user_messages:
                t_global.user_messages = json.load(user_messages)
        except IOError:
            t_global.log.critical("Could not load the user messages '%s'!", t_global.args.user_messages)
            return RC_INVALID_INPUT

        try:
            jsonschema.validate(instance=t_global.user_messages, schema=t_global.user_schema)
        except jsonschema.exceptions.SchemaError as exception:
            t_global.log.critical(exception)
            t_global.log.critical("Could not JSON validate the user messages!")
            return RC_INVALID_INPUT

    # define a signal handler that will respond to SIGALRM when a
    # timeout even occurs
    signal.signal(signal.SIGALRM, timeout_signal_handler)

    mytime = calendar.timegm(time.gmtime())
    t_global.log.info("Current Time: %s", datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC"))

    if t_global.args.wait_for is not None:
        t_global.wait_for_monitor_start = threading.Event()
        t_global.wait_for_launcher_thread = threading.Thread(target = wait_for_process_launcher, args = ())
        t_global.wait_for_monitor_thread = threading.Thread(target = wait_for_process_monitor, args = ())
        t_global.wait_for_monitor_exit = threading.Event()
        t_global.wait_for_launcher_thread.start()
        t_global.wait_for_monitor_thread.start()

    # set the default timeout alarm
    set_alarm(t_global.args.roadblock_timeout)
    cluster_timeout = mytime + t_global.args.roadblock_timeout
    t_global.log.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))

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
        except redis.exceptions.ResponseError as con_error:
            match = re.search(r"WRONGPASS", str(con_error))
            if match:
                t_global.log.error("Invalid username/password pair")
                return RC_INVALID_INPUT
            else:
                t_global.log.error("%s", con_error)
                t_global.log.error("Redis connection could not be opened due to response error!")
                time.sleep(3)
        except redis.exceptions.ConnectionError as con_error:
            t_global.log.error("%s", con_error)
            t_global.log.error("Redis connection could not be opened due to connection error!")
            time.sleep(3)

    t_global.pubsubcon = t_global.redcon.pubsub(ignore_subscribe_messages = True)

    t_global.con_watchdog_exit = threading.Event()
    t_global.con_watchdog = threading.Thread(target = connection_watchdog, args = ())
    t_global.con_watchdog.start()

    t_global.log.info("Roadblock UUID: %s", t_global.args.roadblock_uuid)
    t_global.log.info("Role: %s", t_global.args.roadblock_role)
    if t_global.args.roadblock_role == "follower":
        t_global.log.info("Follower ID: %s", t_global.args.roadblock_follower_id)
        t_global.log.info("Leader ID: %s", t_global.args.roadblock_leader_id)
    elif t_global.args.roadblock_role == "leader":
        t_global.log.info("Leader ID: %s", t_global.args.roadblock_leader_id)
        t_global.log.info("Total followers: %d", len(t_global.args.roadblock_followers))
        t_global.log.info("Followers: %s", t_global.args.roadblock_followers)
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
                t_global.log.debug("initiator received msg=[%s] on busB", msg_str)

                msg = message_from_str(msg_str)

                # copy the message over to busA
                t_global.log.debug("initiator mirroring msg=[%s] to busA", msg_str)
                list_append(t_global.args.roadblock_uuid + "__busA", msg_str)

                if not message_for_me(msg):
                    t_global.log.debug("received a message which is not for me!")
                else:
                    if not message_validate(msg):
                        t_global.log.error("initiator received a message for me which did not validate! [%s]", msg_str)
                    else:
                        t_global.log.debug("initiator received a validated message for me! [%s]", msg_str)
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
                    t_global.log.debug("received msg=[%s] on busA with status_index=[%d]", msg_str, msg_list_index)

                    msg = message_from_str(msg_str)

                    if not message_for_me(msg):
                        t_global.log.debug("received a message which is not for me!")
                    else:
                        if not message_validate(msg):
                            t_global.log.error("received a message for me which did not validate! [%s]", msg_str)
                        else:
                            t_global.log.debug("received a validated message for me!")
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
            t_global.log.debug("received msg=[%s] on busB", msg_str)

            msg = message_from_str(msg_str)

            if not message_for_me(msg):
                t_global.log.debug("received a message which is not for me!")
            else:
                if not message_validate(msg):
                    t_global.log.error("received a message for me which did not validate! [%s]", msg_str)
                else:
                    t_global.log.debug("received a validated message for me!")
                    ret_val = message_handle(msg)
                    if ret_val:
                        return ret_val

    t_global.log.info("Cleaning up")
    cleanup()

    t_global.log.info("Exiting")
    if t_global.leader_abort_waiting:
        t_global.log.critical("Roadblock Completed with a Waiting Abort")
        return RC_ABORT_WAITING
    if t_global.leader_abort or t_global.follower_abort:
        t_global.log.critical("Roadblock Completed with an Abort")
        return RC_ABORT
    else:
        t_global.log.info("Roadblock Completed Successfully")
        return RC_SUCCESS

if __name__ == "__main__":
    t_global = global_vars()
    sys.exit(main())
