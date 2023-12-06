'''Roadblock is a synchronization and message passing utility which relies on redis for communication'''

import datetime
import time
import calendar
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
import copy
import base64
import lzma
import redis
import jsonschema


class roadblock:
    '''roadblock object class'''

    # log formatting variables
    log_debug_format =  '[CODE][%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'

    # return code variables
    RC_SUCCESS=0
    RC_ERROR=1
    RC_INVALID_INPUT=2
    RC_TIMEOUT=3
    RC_ABORT=4
    RC_HEARTBEAT_TIMEOUT=5
    RC_ABORT_WAITING=6

    # object variables
    logger = None
    debug = False

    # return code status
    rc = 0

    # parameters
    roadblock_role = None
    my_id = None
    roadblock_uuid = None
    message_validation = "all"
    connection_watchdog_state = "disabled"
    roadblock_followers = None
    abort = None
    roadblock_leader_id = None
    wait_for_cmd = None
    wait_for = None
    wait_for_log = None
    simulate_heartbeat_timeout = None
    roadblock_follower_id = None
    roadblock_timeout = 30
    roadblock_redis_server = "localhost"
    roadblock_redis_password = None

    # runtime variables
    alarm_active = False
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
    initiator = False
    schema = None
    user_schema = None
    my_id = None
    watch_bus = True
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
    log = None
    heartbeat_timeout = 30
    waiting_failed = False
    sigint_counter = 0

    def __init__(self, logger, debug):
        '''roadblock object initiator function'''

        if not debug:
            self.debug = True

        if not logger is None:
            self.logger = logger
        else:
            if self.debug:
                logging.basicConfig(level = logging.DEBUG, format = self.log_debug_format, stream = sys.stdout)
            else:
                logging.basicConfig(level = logging.INFO, format = self.log_normal_format, stream = sys.stdout)

            self.logger = logging.getLogger(__file__)

    def get_rc(self):
        '''get the roadblock return code'''

        return self.rc

    def get_logger(self):
        '''return the logging object'''

        return self.logger

    def set_role(self, role):
        '''set the participant's role'''

        self.roadblock_role = role

        return 0

    def set_follower_id(self, follower_id):
        '''set the follower's id'''

        self.roadblock_follower_id = follower_id

        return 0

    def set_leader_id(self, leader_id):
        '''set the leader's id'''

        self.roadblock_leader_id = leader_id

        return 0

    def set_uuid(self, rb_uuid):
        '''set the roadblock's uuid'''

        self.roadblock_uuid = rb_uuid

        return 0

    def set_timeout(self, timeout):
        '''set the roadblock's timeout'''

        self.roadblock_timeout = timeout

        return 0

    def set_redis_server(self, server):
        '''set the redis server'''

        self.roadblock_redis_server = server

        return 0

    def set_redis_password(self, password):
        '''set the redis password'''

        self.roadblock_redis_password = password

        return 0

    def set_followers(self, followers):
        '''set the roadblock followers'''

        self.roadblock_followers = copy.deepcopy(followers)

        return 0

    def set_abort(self, abort):
        '''set the abort status'''

        self.abort = abort

        return 0

    def set_message_log(self, msg_log):
        '''set the message log'''

        self.message_log = msg_log

        return 0

    def set_user_messages(self, usr_msgs):
        '''set the user messages'''

        self.user_messages = usr_msgs

        return 0

    def set_message_validation(self, msg_val):
        '''set the message validation level'''

        self.message_validation = msg_val

        return 0

    def set_connection_watchdog(self, con_watchdog_state):
        '''Enable/disable the connection watchdog'''

        self.connection_watchdog_state = con_watchdog_state

        return 0

    def set_wait_for_cmd(self, wait_for_cmd):
        '''set the wait-for program'''

        self.wait_for_cmd = wait_for_cmd
        self.wait_for = self.wait_for_cmd

        return 0

    def set_wait_for_log(self, wait_for_log):
        '''set the wait-for log'''

        self.wait_for_log = wait_for_log

        return 0

    def set_simulate_heartbeat_timeout(self, value):
        '''set the simulate heartbeat timeout value'''

        self.simulate_heartbeat_timeout = value

        return 0

    def set_alarm(self, seconds):
        '''Set a SIGALRM to fire in `seconds` seconds'''

        signal.alarm(seconds)
        self.alarm_active = True

        return self.RC_SUCCESS

    def disable_alarm(self):
        '''Disable an existing SIGALRM'''

        signal.alarm(0)
        self.alarm_active = True

        return self.RC_SUCCESS

    def message_to_str(self, message):
        '''Converts a message into a JSON string'''

        return json.dumps(message, separators=(",", ":"))

    def message_from_str(self, message):
        '''Convert a JSON string into a message'''

        return json.loads(message)

    def message_build(self, recipient_type, recipient_id, command, value=None):
        '''Create a generic message using the ID and role of the sender'''

        return self.message_build_custom(self.roadblock_role, self.my_id, recipient_type, recipient_id, command, value)

    def message_build_custom(self, sender_type, sender_id, recipient_type, recipient_id, command, value=None):
        '''Create a custom message with any user specified values'''

        message = {
            "payload": {
                "uuid": str(uuid.uuid4()),
                "roadblock": self.roadblock_uuid,
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

        message["tx_checksum"] = hashlib.sha256(str(self.message_to_str(message["payload"])).encode("utf-8")).hexdigest()

        return message

    def message_validate(self, message):
        '''Validate that a received message matches the message schema and that it is not corrupted'''

        if self.message_validation == "none":
            return True

        if self.message_validation in [ "checksum", "all" ]:
            # the checksum comparison appears to be fairly light weight so do
            # that first -- no reason to continue with schema validation if
            # this fails
            if not bool(message["tx_checksum"] == message["rx_checksum"]):
                self.logger.error("message failed checksum validation [%s]", self.message_to_str(message))
                return False
            else:
                self.logger.debug("message passed checksum validation [%s]", self.message_to_str(message))

        if self.message_validation in [ "schema", "all" ]:
            try:
                jsonschema.validate(instance=message, schema=self.schema)
                self.logger.debug("message passed schema validation [%s]", self.message_to_str(message))

            except jsonschema.exceptions.SchemaError:
                self.logger.error("message failed schema validation [%s]", self.message_to_str(message))
                return False

        return True

    def message_for_me(self, message):
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
            self.logger.error("incomplete message received [%s]", self.message_to_str(message))
            return False

        if message["payload"]["sender"]["id"] == self.my_id and message["payload"]["sender"]["type"] == self.roadblock_role:
            # I'm the sender so ignore it
            return False
        elif message["payload"]["recipient"]["type"] == "all":
            pass
        elif message["payload"]["recipient"]["type"] == self.roadblock_role and message["payload"]["recipient"]["id"] == self.my_id:
            pass
        else:
            return False

        if self.message_validation in [ "checksum", "all" ]:
            # get the rx_checksum before we modify the message by adding the rx_timestamp to it
            message["rx_checksum"] = hashlib.sha256(str(self.message_to_str(message["payload"])).encode("utf-8")).hexdigest()

        # embed the rx_timestamp into the message
        message["payload"]["recipient"]["timestamp"] = rx_timestamp

        return True

    def message_get_command(self, message):
        '''Extract the command from a message'''

        return message["payload"]["message"]["command"]

    def message_get_value(self, message):
        '''Extract a value from the a message'''

        return message["payload"]["message"]["value"]

    def message_get_sender(self, message):
        '''Extract the sender ID from a message'''

        return message["payload"]["sender"]["id"]

    def message_get_sender_type(self, message):
        '''Extract the sender type from a message'''

        return message["payload"]["sender"]["type"]

    def message_get_uuid(self, message):
        '''Extract a message's UUID'''

        return message["payload"]["uuid"]

    def define_usr_msg_schema(self):
        '''Define the schema used to validate user messages'''

        self.user_schema = {
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

        return 0

    def define_msg_schema(self):
        '''Define the schema used to validate roadblock protocol messages'''

        self.schema = {
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
                                self.roadblock_uuid
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
                                        "global-bus-created",
                                        "leader-bus-created",
                                        "followers-bus-created",
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

        return 0

    def send_user_messages(self):
        '''Send user defined messages'''

        if self.user_messages is not None:
            self.logger.info("Sending user requested messages")
            user_msg_counter = 1
            for user_msg in self.user_messages:
                if "user-string" in user_msg:
                    self.logger.info("Sending user message %d: 'user-string'", user_msg_counter)
                    self.message_publish("global", self.message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-string", user_msg["user-string"]))
                elif "user-object" in user_msg:
                    self.logger.info("Sending user message %d: 'user-object'", user_msg_counter)
                    self.message_publish("global", self.message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-object", user_msg["user-object"]))

                user_msg_counter += 1

        return 0

    def message_handle (self, message):
        '''Roadblock protocol message handler'''

        msg_uuid = self.message_get_uuid(message)
        if msg_uuid in self.processed_messages:
            self.logger.debug("I have already processed this message! [%s]", msg_uuid)
            return self.RC_SUCCESS
        else:
            self.logger.debug("adding uuid='%s' to the processed messages list", msg_uuid)
            self.processed_messages[msg_uuid] = True

            if self.message_log is not None:
                # if the message log is open then append messages to the queue
                # for later dumping
                self.messages["received"].append(message)

        msg_command = self.message_get_command(message)

        if msg_command in ("global-bus-created", "leader-bus-created", "followers-bus-created"):
            self.logger.info("Received '%s' message", msg_command)
        elif msg_command == "timeout-ts":
            self.logger.info("Received 'timeout-ts' message")

            cluster_timeout = int(self.message_get_value(message))

            mytime = calendar.timegm(time.gmtime())
            timeout = mytime - cluster_timeout

            if timeout < 0:
                self.set_alarm(abs(timeout))
                self.logger.info("The new timeout value is in %d seconds", abs(timeout))
                self.logger.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))
            else:
                self.disable_alarm()
                self.logger.critical("The timeout has already occurred")
                return self.RC_TIMEOUT
        elif msg_command == "leader-online":
            if self.roadblock_role == "follower":
                self.logger.debug("I see that the leader is online")
        elif msg_command == "follower-online":
            if self.roadblock_role == "leader":
                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["online"]:
                    self.logger.info("Received 'follower-online' message from '%s'", msg_sender)
                    del self.followers["online"][msg_sender]
                elif msg_sender in self.roadblock_followers:
                    self.logger.warning("Did I already process this 'follower-online' message from follower '%s'?", msg_sender)
                else:
                    self.logger.info("Received 'follower-online' message from unknown follower '%s'", msg_sender)

                if len(self.followers["online"]) == 0:
                    self.logger.info("Sending 'all-online' message")
                    self.message_publish("followers", self.message_build("all", "all", "all-online"))
                    if self.initiator:
                        self.send_user_messages()
        elif msg_command == "all-online":
            if self.initiator:
                self.logger.info("Initiator received 'all-online' message")
            else:
                self.logger.info("Received 'all-online' message")

            self.send_user_messages()

            if self.roadblock_role == "follower":
                if self.abort:
                    self.logger.info("Sending 'follower-ready-abort' message")
                    self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready-abort"))
                else:
                    if self.wait_for is not None and self.wait_for_process is not None and self.wait_for_process.poll() is None:
                        self.wait_for_waiting = True
                        self.logger.info("Sending 'follower-ready-waiting' message")
                        self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready-waiting"))
                    else:
                        self.logger.info("Sending 'follower-ready' message")
                        self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready"))
        elif msg_command in ("follower-ready", "follower-ready-abort", "follower-ready-waiting"):
            if self.roadblock_role == "leader":
                self.logger.debug("leader got a '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_command == "follower-ready-abort":
                    self.leader_abort = True
                elif msg_command == "follower-ready-waiting":
                    self.roadblock_waiting = True

                    self.logger.info("Adding follower '%s' to the waiting list", msg_sender)
                    self.followers["busy_waiting"][msg_sender] = True

                if msg_sender in self.followers["ready"]:
                    self.logger.info("Received '%s' message from '%s'", msg_command, msg_sender)
                    del self.followers["ready"][msg_sender]
                elif msg_sender in self.roadblock_followers:
                    self.logger.warning("Received a redundant '%s' message from follower '%s'?", msg_command, msg_sender)
                else:
                    self.logger.info("Received '%s' message from unknown follower '%s'", msg_command, msg_sender)

                if len(self.followers["ready"]) == 0:
                    self.logger.info("Sending 'all-ready' message")
                    self.message_publish("followers", self.message_build("all", "all", "all-ready"))

                    if self.leader_abort:
                        self.logger.info("Sending 'all-abort' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-abort"))
                    elif self.roadblock_waiting:
                        self.logger.info("Sending 'all-wait' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-wait"))

                        self.logger.info("Disabling original timeout")
                        self.disable_alarm()

                        self.logger.info("Disabling original timeout signal handler")
                        self.logger.info("Enabling heartbeat timeout signal handler")
                        signal.signal(signal.SIGALRM, self.heartbeat_signal_handler)

                        self.logger.info("Sending 'leader-heartbeat' message")
                        self.message_publish("followers", self.message_build("all", "all", "leader-heartbeat"))

                        self.logger.info("Starting heartbeat monitoring period")
                        self.set_alarm(self.heartbeat_timeout)
                    else:
                        self.logger.info("Sending 'all-go' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-go"))
        elif msg_command == "all-wait":
            if self.roadblock_role == "follower":
                self.logger.info("Received 'all-wait' message")

                self.logger.info("Disabling original timeout")
                self.disable_alarm()
        elif msg_command == "leader-heartbeat":
            if self.roadblock_role == "follower":
                self.logger.info("Received 'leader-heartbeat' message")

                if self.simulate_heartbeat_timeout:
                    self.logger.critical("Not sending 'follower-heartbeat' because of heartbeat timeout simulation request")
                else:
                    self.logger.info("Sending 'follower-heartbeat' message")
                    self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-heartbeat"))
        elif msg_command  == "follower-heartbeat":
            if self.roadblock_role == "leader":
                self.logger.info("Received '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["waiting"]:
                    self.logger.info("Received heartbeat from follower '%s'", msg_sender)
                    del self.followers["waiting"][msg_sender]
                elif msg_sender in self.roadblock_followers:
                    self.logger.warning("Received a redundant heartbeat message from follower '%s'?", msg_sender)
                else:
                    self.logger.warning("Received a heartbeat message from an unknown follower '%s'", msg_sender)

                if len(self.followers["busy_waiting"]) == 0 and len(self.followers["waiting"]) == 0:
                    self.logger.info("Disabling heartbeat timeout")
                    self.disable_alarm()

                    if self.waiting_failed:
                        self.leader_abort = True
                        self.logger.info("Sending 'all-abort' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-abort"))
                    else:
                        self.logger.info("Sending 'all-go' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-go"))
        elif msg_command in ("follower-waiting-complete", "follower-waiting-complete-failed"):
            if self.roadblock_role == "leader":
                self.logger.info("Received '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["busy_waiting"]:
                    self.logger.info("Follower '%s' is no longer busy waiting", msg_sender)
                    del self.followers["busy_waiting"][msg_sender]

                    if msg_command == "follower-waiting-complete-failed":
                        self.waiting_failed = True
                elif msg_sender in self.roadblock_followers:
                    self.logger.warning("Received '%s' from a follower '%s' that is not busy waiting?", msg_command, msg_sender)
                else:
                    self.logger.info("Received '%s' message from an unknown follower '%s'", msg_command, msg_sender)

                if len(self.followers["busy_waiting"]) == 0 or self.waiting_failed:
                    self.logger.info("Disabling heartbeat timeout")
                    self.disable_alarm()

                    if self.waiting_failed:
                        self.leader_abort_waiting = True
                        self.logger.info("Sending 'all-abort' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-abort"))
                    else:
                        self.logger.info("Sending 'all-go' command")
                        self.message_publish("followers", self.message_build("all", "all", "all-go"))
        elif msg_command == "heartbeat-timeout":
            if self.roadblock_role == "follower":
                self.logger.info("Received '%s' message", msg_command)

                self.logger.critical("Roadblock failed due to a heartbeat timeout")

                self.timeout_internals()

                # signal myself to exit
                self.watch_bus = False

                self.rc = self.RC_HEARTBEAT_TIMEOUT
                return self.rc
        elif msg_command == "all-ready":
            self.logger.info("Received 'all-ready' message")
        elif msg_command in ("all-go", "all-abort"):
            if self.roadblock_role == "follower":
                if msg_command == "all-go":
                    self.logger.info("Received 'all-go' from leader")
                else:
                    self.logger.info("Received 'all-abort' from leader")
                    self.follower_abort = True

                    if self.wait_for is not None and self.wait_for_process is not None:
                        self.logger.critical("Killing wait_for process due to 'all-abort' from leader")
                        self.wait_for_process.kill()

                # tell the leader that I'm gone
                self.logger.info("Sending 'follower-gone' message")
                self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-gone"))

                # signal myself to exit
                self.watch_bus = False
        elif msg_command == "follower-gone":
            if self.roadblock_role == "leader":
                self.logger.debug("leader got a 'follower-gone' message")

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["gone"]:
                    self.logger.info("Received 'follower-gone' message from '%s'", msg_sender)
                    del self.followers["gone"][msg_sender]
                elif msg_sender in self.roadblock_followers:
                    self.logger.warning("Received a redundant 'follower-gone' message from follower '%s'?", msg_sender)
                else:
                    self.logger.info("Received 'follower-gone' message from unknown follower '%s'", msg_sender)

                if len(self.followers["gone"]) == 0:
                    # send a message that will probably not be observed by
                    # anyone...but just in case...
                    self.logger.info("Sending 'all-gone' message")
                    self.message_publish("followers", self.message_build("all", "all", "all-gone"))

                    # signal myself to exit
                    self.watch_bus = False
        elif msg_command == "initiator-info":
            self.initiator_type = self.message_get_sender_type(message)
            self.initiator_id = self.message_get_sender(message)
            self.logger.debug("Received an 'initiator-info' message with type='%s' and id='%s'", self.initiator_type, self.initiator_id)

        return self.RC_SUCCESS

    def message_publish(self, message_bus, message):
        '''Publish messages for subscribers to receive'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                self.logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1

            try:
                ret_val = self.redcon.xadd(self.roadblock_uuid + "__bus__" + message_bus, { 'msg': self.message_to_str(message) })
            except redis.exceptions.ConnectionError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Bus add to '%s' failed due to connection error!", message_bus)
            except redis.exceptions.TimeoutError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Bus add to '%s' failed due to a timeout error!", message_bus)

            if ret_val is None:
                self.logger.warning("Failed attempt %d to publish message '%s' to bus '%s'", counter, message, message_bus)

                self.backoff(counter)
            else:
                self.logger.debug("Message '%s' was sent on the %d attempt with message ID '%s' on bus '%s'", message, counter, ret_val, message_bus)

        if self.message_log is not None:
            # if the message log is open then append messages to the queue
            # for later dumping
            # do something bus specific here
            self.messages["sent"].append(message)

        return self.RC_SUCCESS

    def key_delete(self, key):
        '''Delete a key from redis'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                self.logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # this call should return the number of keys deleted which is
            # expected to be one, if not we retry
            ret_val = self.redcon.delete(key)

            if ret_val == 0:
                self.logger.warning("Failed attempt %d to delete key '%s'", counter, key)

                self.backoff(counter)

        return self.RC_SUCCESS

    def key_set_once(self, key, value):
        '''Set a key once in redis'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                self.logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # this call should return one on success, if not we retry
            ret_val = self.redcon.msetnx( { key: value } )

            if ret_val == 0:
                self.logger.warning("Failed attempt %d to set key '%s' with value '%s' once", counter, key, value)

                self.backoff(counter)

        return self.RC_SUCCESS

    def key_set(self, key, value):
        '''Set a key in redis if it does not already exist'''

        # in this case we want to return the true/false behavior so the
        # caller knows if they set the key or it already existed
        return self.redcon.msetnx( { key: value } )

    def key_check(self, key):
        '''Check if a key already exists in redis'''

        # inform the caller whether the key already existed or not
        return self.redcon.exists(key)

    def list_append(self, key, value):
        '''Append a value to a list in redis'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                self.logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # if this call returns 0 then it failed somehow since it
            # should be the size of the list after we have added to it, so
            # we retry
            ret_val = self.redcon.rpush(key, value)

            if ret_val == 0:
                self.logger.warning("Failed attempt %d to append value '%s' to key '%s'", counter, value, key)

                self.backoff(counter)

        return ret_val

    def list_fetch(self, key, offset):
        '''Fetch a list from redis'''

        # return the elements in the specified range (offset to end), this
        # could be empty so we can't really verify it
        return self.redcon.lrange(key, offset, -1)

    def backoff(self, attempts):
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

        return self.RC_SUCCESS

    def cleanup(self):
        '''Cleanup the roadblock before exiting'''

        self.logger.info("Cleaning up")

        if self.alarm_active:
            self.logger.info("Disabling timeout alarm")
            self.disable_alarm()

        if self.wait_for is not None:
            self.logger.info("Closing wait_for monitor thread")
            self.wait_for_monitor_exit.set()
            self.wait_for_monitor_thread.join()

            self.logger.info("Closing wait_for launcher thread")
            self.wait_for_launcher_thread.join()

        if self.con_pool_state:
            if self.roadblock_role == "leader":
                self.logger.info("Removing db objects specific to this roadblock")
                self.key_delete(self.roadblock_uuid)
                self.key_delete(self.roadblock_uuid + "__initialized")

                for bus_name in ( "global", "leader", "followers" ):
                    msg_count = self.redcon.xlen(self.roadblock_uuid + "__bus__" + bus_name)
                    self.logger.debug("total messages on bus '%s': %d", bus_name, msg_count)

                    msgs_trimmed = self.redcon.xtrim(self.roadblock_uuid + "__bus__" + bus_name, maxlen = 0, approximate = False)
                    self.logger.debug("total messages deleted from bus '%s': %d", bus_name, msgs_trimmed)

                    msg_count = self.redcon.xlen(self.roadblock_uuid + "__bus__" + bus_name)
                    self.logger.debug("total messages on bus '%s': %d", bus_name, msg_count)

            if self.connection_watchdog_state == "enabled":
                self.logger.info("Closing connection pool watchdog")
                self.con_watchdog_exit.set()
                self.con_watchdog.join()

            self.logger.info("Closing connection pool")
            self.con_pool.disconnect()
            self.con_pool_state = False

        if self.message_log is not None:
            # if the message log is open then dump the message queue and
            # close the file handle
            print(f"{json.dumps(self.messages, indent = 4, separators=(',', ': '), sort_keys = False)}\n", file=self.message_log)
            self.message_log.close()

        self.logger.debug("Processed Messages:")
        for msg in self.processed_messages:
            self.logger.debug("\t%s", msg)

        return self.RC_SUCCESS

    def get_followers_list(self, followers):
        '''Generate a list of the followers'''

        followers_list = ""

        for follower in followers:
            followers_list += follower + " "

        return followers_list

    def timeout_internals(self):
        '''Steps common to both types of timeout events'''

        if self.wait_for is not None:
            if self.wait_for_process is not None:
                if self.wait_for_process.poll() is None:
                    self.logger.critical("Killing wait_for process due to timeout")
                    self.wait_for_process.kill()
                else:
                    self.logger.debug("wait_for process has already exited")
            else:
                self.logger.critical("The wait-for process object is missing")

        if self.con_pool_state and self.initiator:
            # set a persistent flag that the roadblock timed out so that
            # any late arriving members know that the roadblock has
            # already failed.  done by the first member since that is the
            # only member that is guaranteed to have actually reached the
            # roadblock and be capable of setting this.
            self.key_set_once(self.roadblock_uuid + "__timedout", int(True))

        self.cleanup()

        if self.roadblock_role == "leader":
            if len(self.followers["online"]) != 0:
                self.logger.critical("These followers never reached 'online': %s", self.get_followers_list(self.followers["online"]))
            elif len(self.followers["ready"]) != 0:
                self.logger.critical("These followers never reached 'ready': %s", self.get_followers_list(self.followers["ready"]))
            elif self.roadblock_waiting:
                if len(self.followers["busy_waiting"]) != 0:
                    self.logger.critical("These followers were still 'busy waiting': %s", self.get_followers_list(self.followers["busy_waiting"]))

                if len(self.followers["waiting"]) != 0:
                    self.logger.critical("These followers did not respond to the heartbeat and were still 'waiting': %s", self.get_followers_list(self.followers["waiting"]))
            elif len(self.followers["gone"]) != 0:
                self.logger.critical("These followers never reached 'gone': %s", self.get_followers_list(self.followers["gone"]))

        return self.RC_SUCCESS

    def do_heartbeat_signal(self):
        '''Handle a heartbeat timeout event'''

        if len(self.followers["waiting"]) != 0:
            self.logger.critical("Failed ending current heartbeat monitoring period -> heartbeat timeout")

            self.logger.info("Sending 'heartbeat-timeout' message")
            self.message_publish("followers", self.message_build("all", "all", "heartbeat-timeout"))

            self.timeout_internals()

            self.rc = self.RC_HEARTBEAT_TIMEOUT
            return self.rc
        else:
            self.logger.info("Successfully ending current heartbeat monitoring period")
            self.disable_alarm()

            # rebuild the tracking list by copying from a backup
            self.followers["waiting"] = copy.deepcopy(self.followers["waiting_backup"])

            self.logger.info("Sending 'leader-heartbeat' message")
            self.message_publish("followers", self.message_build("all", "all", "leader-heartbeat"))

            self.logger.info("Starting new heartbeat monitoring period")
            self.set_alarm(self.heartbeat_timeout)

        return self.RC_SUCCESS

    def do_timeout(self):
        '''Handle a roadblock timeout event'''

        self.logger.critical("Roadblock failed with timeout")

        self.timeout_internals()

        self.rc = self.RC_TIMEOUT
        return self.rc

    def timeout_signal_handler(self, signum, frame):
        '''Handle roadblock timeout signals delivered to the process'''

        if signum == 14: # SIGALRM
            self.alarm_active = False
            self.do_timeout()
            return self.rc
        else:
            self.logger.info("Timeout Signal handler called with signal %d", signum)

        return self.RC_SUCCESS

    def heartbeat_signal_handler(self, signum, frame):
        '''Handle heartbeat timeout signals delivered to the process'''

        if signum == 14: # SIGALRM
            self.alarm_active = False
            self.do_heartbeat_signal()
        else:
            self.logger.info("Heartbeat Signal handler called with signal %d", signum)

        return self.RC_SUCCESS

    def sigint_handler(self, signum, frame):
        '''Handle a SIGINT/CTRL-C'''

        if signum == 2: # SIGINT
            self.logger.warning("Caught SIGINT signal")
            self.sigint_counter += 1

            if self.sigint_counter > 1:
                self.logger.critical("Exiting due to SIGINT being received more than 1 time")
                self.cleanup()

                # signal myself to exit
                self.watch_bus = False

                self.rc = self.RC_ERROR
            else:
                self.logger.warning("SIGINT has been received once -> attempting to abort")
                self.leader_abort = True
        else:
            self.logger.info("SIGINT Signal handler called with signal %d", signum)

        return self.RC_SUCCESS

    def connection_watchdog(self):
        '''Check if the redis connection is still open'''

        while not self.con_watchdog_exit.is_set():
            time.sleep(1)
            try:
                if self.con_pool_state:
                    ping_begin = time.time_ns()
                    self.redcon.ping()
                    ping_end = time.time_ns()
                    self.logger.debug("Connection watchdog ping succeeded in %f milliseconds", ((ping_end - ping_begin) / (10 ** 6)))
                else:
                    self.logger.error("Connection watchdog ping skipped due to disconnected state")
            except redis.exceptions.ConnectionError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Connection watchdog ping failed")

        return self.RC_SUCCESS

    def wait_for_process_io_handler(self):
        '''Handle the output logging of a --wait-for program/script process'''

        self.wait_for_io_handler_exited = threading.Event()

        with open(self.wait_for_log, "w", encoding = "ascii") as wait_for_log_fh:
            # process lines while the process is running in a non-blocking fashion
            while self.wait_for_process.poll() is None:
                self.wait_for_process.stdout.flush()
                ready_streams = select.select([self.wait_for_process.stdout], [], [], 1)
                if self.wait_for_process.stdout in ready_streams[0]:
                    for line in self.wait_for_process.stdout:
                        wait_for_log_fh.write(line)

            # process any remaining lines that haven't been handled yet
            # (this will not block now so it is simpler than above)
            for line in self.wait_for_process.stdout:
                wait_for_log_fh.write(line)

        self.wait_for_io_handler_exited.set()

        return self.RC_SUCCESS

    def wait_for_process_monitor(self):
        '''Monitor the status of a --wait-for program/script process'''

        self.logger.debug("The wait_for monitor is waiting to start")

        self.wait_for_monitor_start.wait()

        self.logger.debug("The wait_for monitor is starting")

        while not self.wait_for_monitor_exit.is_set():
            if self.wait_for_process is None:
                self.logger.critical("There is no wait_for_process to monitor")
                self.wait_for_monitor_exit.set()
            else:
                if self.wait_for_process.poll() is None:
                    self.logger.debug("The wait_for_process is still running")
                    time.sleep(1)
                else:
                    self.logger.info("The wait_for process has finished")
                    self.wait_for_monitor_exit.set()

                    if self.wait_for_waiting:
                        if self.wait_for_process.returncode != 0:
                            self.wait_for_io_handler_exited.wait()

                            log_contents = ""
                            with open(self.wait_for_log, "r", encoding = "ascii") as wait_for_log_fh:
                                log_contents = str(base64.b64encode(lzma.compress(wait_for_log_fh.read().encode("ascii"))), "ascii")

                            self.logger.critical("Sending 'follower-waiting-complete-failed' message")
                            self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-waiting-complete-failed", value = log_contents))
                        else:
                            self.logger.info("Sending 'follower-waiting-complete' message")
                            self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-waiting-complete"))

        self.logger.debug("The wait_for monitor is exiting")

        return self.RC_SUCCESS

    def wait_for_process_launcher(self):
        '''Handle the execution of a --wait-for program/script'''

        ret_val = -1

        try:
            self.wait_for_process = subprocess.Popen(self.wait_for_cmd,
                                                     bufsize = 0,
                                                     encoding = 'ascii',
                                                     stdout = subprocess.PIPE,
                                                     stderr = subprocess.STDOUT)

            if self.wait_for_process is None:
                self.logger.critical("The wait_for process failed to launch")
                ret_val = 1
                self.wait_for_monitor_start.set()
            else:
                self.logger.info("The wait-for process is now running")

                self.wait_for_monitor_start.set()
                wait_for_io_thread = threading.Thread(target = self.wait_for_process_io_handler, args = ())
                wait_for_io_thread.start()

                ret_val = self.wait_for_process.wait()
                wait_for_io_thread.join()

        except PermissionError:
            self.logger.critical("Received a permission error when attempting to execute --wait-for command")
            ret_val = 1
            self.wait_for_monitor_start.set()

        self.logger.info("The wait_for process exited with return code %d", ret_val)

        return self.RC_SUCCESS

    def run_it(self):
        '''execute the roadblock'''

        #catch SIGINT/CTRL-C
        signal.signal(signal.SIGINT, self.sigint_handler)

        if len(self.roadblock_leader_id) == 0:
            self.logger.critical("You must specify the leader's ID using --leader-id")
            return self.RC_INVALID_INPUT

        if self.roadblock_role == "leader":
            if len(self.roadblock_followers) == 0:
                self.logger.critical("There must be at least one follower")
                return self.RC_INVALID_INPUT
            if self.abort:
                self.leader_abort = True

            # build some hashes for easy tracking of follower status
            for follower in self.roadblock_followers:
                self.followers["online"][follower] = True
                self.followers["ready"][follower] = True
                self.followers["gone"][follower] = True
                self.followers["waiting"][follower] = True
                self.followers["waiting_backup"][follower] = True

        if self.roadblock_role == "follower":
            self.my_id = self.roadblock_follower_id
        elif self.roadblock_role == "leader":
            self.my_id = self.roadblock_leader_id

        if self.message_log is not None:
            # open the message log, if specified
            try:
                self.message_log = open(self.message_log, "w", encoding="ascii")
            except IOError:
                self.logger.critical("Could not open message log '%s' for writing!", self.message_log)
                return self.RC_INVALID_INPUT

        self.define_msg_schema()
        self.define_usr_msg_schema()

        if self.user_messages is not None:
            # load the user messages, if specified
            try:
                with open(self.user_messages, "r", encoding="ascii") as user_messages:
                    self.user_messages = json.load(user_messages)
            except IOError:
                self.logger.critical("Could not load the user messages '%s'!", self.user_messages)
                return self.RC_INVALID_INPUT

            try:
                jsonschema.validate(instance=self.user_messages, schema=self.user_schema)
            except jsonschema.exceptions.SchemaError as exception:
                self.logger.critical(exception)
                self.logger.critical("Could not JSON validate the user messages!")
                return self.RC_INVALID_INPUT

        # define a signal handler that will respond to SIGALRM when a
        # timeout even occurs
        signal.signal(signal.SIGALRM, self.timeout_signal_handler)

        mytime = calendar.timegm(time.gmtime())
        self.logger.info("Current Time: %s", datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC"))

        if self.wait_for is not None:
            self.logger.info("Wait-For: True")
            self.logger.info("Wait-For Task: %s", self.wait_for)
            self.logger.info("Wait-For Log: %s", self.wait_for_log)
            self.wait_for_monitor_start = threading.Event()
            self.wait_for_launcher_thread = threading.Thread(target = self.wait_for_process_launcher, args = ())
            self.wait_for_monitor_thread = threading.Thread(target = self.wait_for_process_monitor, args = ())
            self.wait_for_monitor_exit = threading.Event()
            self.wait_for_launcher_thread.start()
            self.wait_for_monitor_thread.start()
        else:
            self.logger.info("Wait-For: False")

        # set the default timeout alarm
        self.set_alarm(self.roadblock_timeout)
        cluster_timeout = mytime + self.roadblock_timeout
        self.logger.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))

        # create the redis connections
        while not self.con_pool_state:
            try:
                self.con_pool = redis.ConnectionPool(host = self.roadblock_redis_server,
                                                     password = self.roadblock_redis_password,
                                                     port = 6379,
                                                     db = 0,
                                                     socket_connect_timeout = 5,
                                                     health_check_interval = 0)
                self.redcon = redis.Redis(connection_pool = self.con_pool)
                self.redcon.ping()
                self.con_pool_state = True
            except redis.exceptions.ResponseError as con_error:
                match = re.search(r"WRONGPASS", str(con_error))
                if match:
                    self.logger.error("Invalid username/password pair")
                    return self.RC_INVALID_INPUT
                else:
                    self.logger.error("%s", con_error)
                    self.logger.error("Redis connection could not be opened due to response error!")
                    time.sleep(3)
            except redis.exceptions.ConnectionError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Redis connection could not be opened due to connection error!")
                time.sleep(3)
            except redis.exceptions.TimeoutError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Redis connection could not be opened due to a timeout error!")
                time.sleep(3)

        if self.connection_watchdog_state == "enabled":
            self.con_watchdog_exit = threading.Event()
            self.con_watchdog = threading.Thread(target = self.connection_watchdog, args = ())
            self.con_watchdog.start()

        self.logger.info("Roadblock UUID: %s", self.roadblock_uuid)
        self.logger.info("Role: %s", self.roadblock_role)
        if self.roadblock_role == "follower":
            self.logger.info("Follower ID: %s", self.roadblock_follower_id)
            self.logger.info("Leader ID: %s", self.roadblock_leader_id)
        elif self.roadblock_role == "leader":
            self.logger.info("Leader ID: %s", self.roadblock_leader_id)
            self.logger.info("Total followers: %d", len(self.roadblock_followers))
            self.logger.info("Followers: %s", self.roadblock_followers)
        if self.abort:
            self.logger.info("Abort: True")
        else:
            self.logger.info("Abort: False")

        # check if the roadblock was previously created and already timed
        # out -- ie. I am very late
        if self.key_check(self.roadblock_uuid + "__timedout"):
            self.logger.critical("Detected previous timeout for this roadblock")
            self.do_timeout()
            return self.rc

        # check if the roadblock has been initialized yet
        if self.key_set(self.roadblock_uuid, mytime):
            # i am creating the roadblock
            self.initiator = True
            self.logger.info("Initiator: True")

            # create the streams/buses
            self.logger.info("Creating buses")
            self.message_publish("global", self.message_build("all", "all", "global-bus-created"))
            self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "leader-bus-created"))
            self.message_publish("followers", self.message_build("all", "all", "followers-bus-created"))

            # publish the cluster timeout
            self.logger.info("Sending 'timeout-ts' message")
            self.message_publish("global", self.message_build("all", "all", "timeout-ts", cluster_timeout))

            # publish the initiator information
            self.logger.info("Sending 'initiator-info' message")
            self.message_publish("global", self.message_build("all", "all", "initiator-info"))
            self.initiator_type = self.roadblock_role
            self.initiator_id = self.my_id

            self.list_append(self.roadblock_uuid + "__initialized", int(True))
        else:
            self.logger.info("Initiator: False")

            # the roadblock already exists, make sure it is initialized
            # completely before proceeding
            self.logger.info("Waiting for roadblock initialization to complete")

            # wait until the initialized flag has been set for the roadblock
            while not self.key_check(self.roadblock_uuid + "__initialized"):
                if self.rc != 0:
                    self.logger.debug("self.rc != 0 --> breaking")
                    break

                time.sleep(1)
                self.logger.info(".")

            self.logger.info("Roadblock is initialized")
        if self.roadblock_role == "follower":
            # tell the leader that I am online
            self.logger.info("Sending 'follower-online' message")
            self.message_publish("leader", self.message_build("leader", self.roadblock_leader_id, "follower-online"))
        elif self.roadblock_role == "leader":
            # tell everyone that the leader is online
            self.logger.info("Sending 'leader-online' message")
            self.message_publish("followers", self.message_build("all", "all", "leader-online"))

        followers_last_msg_id = 0
        leader_last_msg_id = 0
        global_last_msg_id = 0
        while self.watch_bus:
            if self.rc != 0:
                self.logger.debug("self.rc != 0 --> breaking")
                break

            try:
                if self.roadblock_role == "follower":
                    msgs = self.redcon.xread(streams = {
                        self.roadblock_uuid + "__bus__global": global_last_msg_id,
                        self.roadblock_uuid + "__bus__followers": followers_last_msg_id
                    }, block = 0)
                elif self.roadblock_role == "leader":
                    msgs = self.redcon.xread(streams = {
                        self.roadblock_uuid + "__bus__global": global_last_msg_id,
                        self.roadblock_uuid + "__bus__leader": leader_last_msg_id
                    }, block = 0)
            except redis.exceptions.ConnectionError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Bus read failed due to connection error!")
            except redis.exceptions.TimeoutError as con_error:
                self.logger.error("%s", con_error)
                self.logger.error("Bus read failed due to a timeout error!")

            if len(msgs) == 0:
                time.sleep(0.001)
            else:
                for bus in msgs:
                    bus_name = bus[0].decode()

                    self.logger.debug("retrieved %d messages from bus '%s' for processing", len(bus[1]), bus_name)

                    for msg_id, msg in bus[1]:
                        if bus_name == self.roadblock_uuid + "__bus__global":
                            global_last_msg_id = msg_id
                        elif bus_name == self.roadblock_uuid + "__bus__leader":
                            leader_last_msg_id = msg_id
                        elif bus_name == self.roadblock_uuid + "__bus__followers":
                            followers_last_msg_id = msg_id

                        self.logger.debug("received msg=[%s] with msg_id=[%s] from bus '%s'", msg, msg_id, bus_name)

                        msg = self.message_from_str(msg[b"msg"].decode())

                        if not self.message_for_me(msg):
                            self.logger.debug("received a message which is not for me!")
                        else:
                            if not self.message_validate(msg):
                                self.logger.error("received a message for me which did not validate! [%s]", msg)
                            else:
                                self.logger.debug("received a validated message for me!")
                                ret_val = self.message_handle(msg)
                                if ret_val:
                                    return ret_val

        if self.rc == 0:
            self.cleanup()

        self.logger.info("Exiting")

        if self.sigint_counter > 1 and self.rc != 0:
            self.logger.critical("Roadblock Completed with a double SIGINT")
            return self.RC_ERROR

        if self.rc == self.RC_HEARTBEAT_TIMEOUT:
            self.logger.critical("Roadblock Completed with a Heartbeat Timeout")
            return self.RC_HEARTBEAT_TIMEOUT

        if self.leader_abort_waiting:
            self.logger.critical("Roadblock Completed with a Waiting Abort")
            return self.RC_ABORT_WAITING

        if self.leader_abort or self.follower_abort:
            self.logger.critical("Roadblock Completed with an Abort")
            return self.RC_ABORT

        if self.rc != self.RC_SUCCESS:
            self.logger.info("Roadblock Completed with an Error")
            return self.rc
        else:
            self.logger.info("Roadblock Completed Successfully")
            return self.RC_SUCCESS
