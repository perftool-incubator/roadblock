'''Roadblock is a synchronization and message passing utility which relies on redis for communication'''

import datetime
import time
import calendar
import hashlib
import json
import uuid
import threading
import logging
import re
import subprocess
import select
import copy
import base64
import lzma
import queue

import redis
import jsonschema

VERBOSE_DEBUG_LEVEL = 5

logging.addLevelName(VERBOSE_DEBUG_LEVEL, "VDEBUG")

def verbose_debug(self, message, *args, **kws):
    '''a new log handler for VERBOSE_DEBUG_LEVEL'''
    if self.isEnabledFor(VERBOSE_DEBUG_LEVEL):
        self._log(VERBOSE_DEBUG_LEVEL, message, args, **kws)

logging.Logger.verbose_debug = verbose_debug

logger = logging.getLogger(__file__)

class DebuggingConnection(redis.Connection):
    '''a custom connection class which adds important connection information useful for debugging'''

    # this is a wrapper around the original send_command so that we
    # can add some extra bits
    def send_command(self, *args, **options):
        # get the port number and IP address from the socket that will
        # be used to send the command; since there are often multiple
        # roadblocks running on the same system the port number is the
        # most obvious way to connect client side activity to server
        # side activity
        socket_info = [ "N/A", "N/A" ]
        if self._sock:
            try:
                # getsockname() returns ('ip', port)
                socket_info = self._sock.getsockname()
            except OSError:
                # for if the socket is closed
                socket_info = [ "Socket Error", "Socket Error" ]

        logger.verbose_debug("IP: %s | Port: %s | Command: %s", socket_info[0], socket_info[1], args)

        # call the original send_command to continue normally
        # this preserves all original logic.
        return super().send_command(*args, **options)

class roadblock_json_encoder(json.JSONEncoder):
    '''a custom json encoder to handle the custom classes'''

    def default(self, o):
        '''override the default encoder from the parent class'''

        if isinstance(o, roadblock_list):
            return o.list()

        return json.JSONEncoder.default(self, o)

class roadblock_list:
    '''a list object class that is thread safe'''

    def __init__(self, __list = None):
        '''roadblock_list object initiator function'''

        self._lock = threading.Lock()
        with self._lock:
            if __list is None:
                self._list = []
            else:
                self._list = __list

    def __contains__(self, item):
        '''check if the item is in the list'''

        with self._lock:
            return item in self._list

    def __len__(self):
        '''return the length of the list'''

        with self._lock:
            return len(self._list)

    def __iter__(self):
        '''iterate through the list'''

        with self._lock:
            yield from self._list

    def __copy__(self):
        '''return a copy of the object'''

        with self._lock:
            return roadblock_list(self._list.copy())

    def __deepcopy__(self, memo):
        '''return a deep copy of the object'''

        with self._lock:
            return roadblock_list(copy.deepcopy(self._list, memo))

    def append(self, item):
        '''add an item to the end of the list'''

        with self._lock:
            return self._list.append(item)

    def get(self, index):
        '''return the item at a specific index'''

        with self._lock:
            return self._list[index]

    def extend(self, __list):
        '''extend the list by adding another list'''

        with self._lock:
            return self._list.extend(__list)

    def list(self):
        '''get the list for external usage'''

        with self._lock:
            return copy.deepcopy(self._list)

class roadblock_dictionary:
    '''a dictionary object class that is thread safe'''

    def __init__(self, __dict = None):
        '''roadblock_dictionary object initiator function'''

        self._lock = threading.Lock()
        with self._lock:
            if __dict is None:
                self._dict = {}
            else:
                self._dict = __dict

    def __contains__(self, key):
        '''check if key exists in the dictionary'''

        with self._lock:
            return key in self._dict

    def __len__(self):
        '''return the length of the dictionary'''

        with self._lock:
            return len(self._dict)

    def __iter__(self):
        '''iterate through the dictionary'''

        with self._lock:
            yield from self._dict

    def __copy__(self):
        '''return a copy of the object'''

        with self._lock:
            return roadblock_dictionary(self._dict.copy())

    def __deepcopy__(self, memo):
        '''return a deep copy of the object'''

        with self._lock:
            return roadblock_dictionary(copy.deepcopy(self._dict, memo))

    def add(self, key, value):
        '''add a key to the dictionary if it does not already exist'''

        with self._lock:
            if key in self._dict:
                return False
            else:
                self._dict[key] = value
                return True

    def remove(self, key):
        '''remove a key from teh dictionary if it exists'''

        with self._lock:
            if key in self._dict:
                del self._dict[key]
                return True
            else:
                return False

    def get(self, key):
        '''return a value for a key in the dictionary'''

        with self._lock:
            return self._dict[key]

    def modify(self, key, value):
        '''modify a key's value in the dictionary if it exists'''

        with self._lock:
            if key in self._dict:
                self._dict[key] = value
                return True
            else:
                return False

class roadblock:
    '''roadblock object class'''

    # log formatting variables
    log_debug_format =  '[CODE][%(module)s %(funcName)s:%(lineno)d]\n[%(asctime)s][%(levelname) 8s][%(threadName)s] %(message)s'
    log_normal_format = '[%(asctime)s][%(levelname) 8s] %(message)s'

    # return code variables
    RC_SUCCESS=0
    RC_ERROR=1
    RC_INVALID_INPUT=2
    RC_TIMEOUT=3
    RC_ABORT=4
    RC_HEARTBEAT_TIMEOUT=5
    RC_ABORT_WAITING=6

    def __init__(self, noop1 = None, noop2 = None):
        '''roadblock object initiator function'''

        # noop1 and noop2 are used to provide backwards compatibility
        # with a two argument version of this constructor

        # return code status
        self.rc = 0

        # parameters
        self.roadblock_role = None
        self.roadblock_uuid = None
        self.message_validation = "all"
        self.connection_watchdog_state = "disabled"
        self.roadblock_followers = None
        self.abort = None
        self.roadblock_leader_id = None
        self.wait_for_cmd = None
        self.wait_for = None
        self.wait_for_log = None
        self.simulate_heartbeat_timeout = None
        self.roadblock_follower_id = None
        self.roadblock_timeout = 30
        self.roadblock_redis_server = "localhost"
        self.roadblock_redis_password = None
        self.minor_abort_event = None
        self.major_abort_event = None

        # runtime variables
        self.abort_event_loop = threading.Event()
        self.abort_event_thread = None
        self.minor_abort_event_processed = threading.Event()
        self.major_abort_event_processed = threading.Event()
        self.timeout_active = threading.Event()
        self.timeout_thread = None
        self.con_pool = None
        self.con_pool_active = threading.Event()
        self.con_watchdog_exit = None
        self.con_watchdog = None
        self.wait_for_cmd = None
        self.wait_for_io_handler_exited = None
        self.wait_for_launcher_thread = None
        self.wait_for_process = None
        self.wait_for_monitor_thread = None
        self.wait_for_monitor_exit = None
        self.wait_for_monitor_start = None
        self.wait_for_waiting = threading.Event()
        self.redcon = None
        self.initiator = threading.Event()
        self.schema = None
        self.user_schema = None
        self.my_id = None
        self.watch_stream = threading.Event()
        self.watch_stream.set()
        self.leader_abort = threading.Event()
        self.leader_abort_waiting = False
        self.roadblock_waiting = threading.Event()
        self.follower_abort = False
        self.initiator_type = None
        self.initiator_id = None
        self.followers = { "online": roadblock_dictionary(),
                           "ready": roadblock_dictionary(),
                           "gone": roadblock_dictionary(),
                           "waiting": roadblock_dictionary(),
                           "waiting_backup": roadblock_dictionary(),
                           "busy_waiting": roadblock_dictionary() }
        self.processed_messages = roadblock_dictionary()
        self.messages = { "sent": roadblock_list(),
                          "received": roadblock_list() }
        self.message_log = None
        self.user_messages = []
        self.log = None
        self.heartbeat_timeout = 30
        self.waiting_failed = False

        self.message_processing_thread = None
        self.message_processing_queue = queue.Queue()
        self.message_processing_force_exit = threading.Event()

        self.followers_last_msg_id = 0
        self.followers_prev_last_msg_id = None
        self.leader_last_msg_id = 0
        self.leader_prev_last_msg_id = None
        self.global_last_msg_id = 0
        self.global_prev_last_msg_id = None
        self.personal_last_msg_id = 0
        self.personal_prev_last_msg_id = None

    def get_rc(self):
        '''get the roadblock return code'''

        return self.rc

    def set_minor_abort_event(self, event):
        '''set the minor abort event'''

        self.minor_abort_event = event

        return 0

    def set_major_abort_event(self, event):
        '''set the major abort event'''

        self.major_abort_event = event

        return 0

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

    def enable_timeout(self, seconds, timeout_function, thread_name):
        '''Enable a timeout thread to fire  in `seconds` seconds'''

        self.disable_timeout()

        logger.debug("Creating new timeout")
        self.timeout_thread = threading.Timer(seconds, timeout_function)
        self.timeout_thread.name = thread_name
        self.timeout_thread.start()
        self.timeout_active.set()

        return self.RC_SUCCESS

    def disable_timeout(self):
        '''Disable an existing timeout thread'''

        if self.timeout_thread is not None and self.timeout_active.is_set():
            logger.info("Disabling existing timeout")
            self.timeout_thread.cancel()
            self.timeout_active.clear()
        else:
            logger.debug("No existing timeout to disable")

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
                logger.error("message failed checksum validation [%s]", self.message_to_str(message))
                return False
            else:
                logger.debug("message passed checksum validation [%s]", self.message_to_str(message))

        if self.message_validation in [ "schema", "all" ]:
            try:
                jsonschema.validate(instance=message, schema=self.schema)
                logger.debug("message passed schema validation [%s]", self.message_to_str(message))

            except jsonschema.exceptions.SchemaError:
                logger.error("message failed schema validation [%s]", self.message_to_str(message))
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
            logger.error("incomplete message received [%s]", self.message_to_str(message))
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
                                        "global-stream-created",
                                        "leader-stream-created",
                                        "followers-stream-created",
                                        "personal-stream-created",
                                        "timeout-ts",
                                        "initialized",
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
            logger.info("Sending user requested messages")
            user_msg_counter = 1
            for user_msg in self.user_messages:
                stream_name = "global"
                if user_msg["recipient"]["id"] != "all":
                    stream_name = user_msg["recipient"]["id"]

                if "user-string" in user_msg:
                    logger.info("Sending user message %d: 'user-string'", user_msg_counter)
                    self.stream_add(stream_name, self.message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-string", user_msg["user-string"]))
                elif "user-object" in user_msg:
                    logger.info("Sending user message %d: 'user-object'", user_msg_counter)
                    self.stream_add(stream_name, self.message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-object", user_msg["user-object"]))

                user_msg_counter += 1

        return 0

    def message_handle (self, message):
        '''Roadblock protocol message handler'''

        msg_uuid = self.message_get_uuid(message)
        if msg_uuid in self.processed_messages:
            logger.debug("I have already processed this message! [%s]", msg_uuid)
            return self.RC_SUCCESS
        else:
            logger.debug("adding uuid='%s' to the processed messages list", msg_uuid)
            self.processed_messages.add(msg_uuid, True)

            if self.message_log is not None:
                # if the message log is open then append messages to the queue
                # for later dumping
                self.messages["received"].append(message)

        msg_command = self.message_get_command(message)

        if msg_command in ("global-stream-created", "leader-stream-created", "followers-stream-created", "personal-stream-created"):
            logger.info("Received '%s' message", msg_command)
        elif msg_command == "timeout-ts":
            logger.info("Received 'timeout-ts' message")

            cluster_timeout = int(self.message_get_value(message))

            mytime = calendar.timegm(time.gmtime())
            timeout = mytime - cluster_timeout

            if timeout < 0:
                self.enable_timeout(abs(timeout), self.timeout_handler, "timeout_handler_2")
                logger.info("The new timeout value is in %d seconds", abs(timeout))
                logger.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))
            else:
                self.disable_timeout()
                logger.critical("The timeout has already occurred")
                return self.RC_TIMEOUT
        elif msg_command == "leader-online":
            if self.roadblock_role == "follower":
                logger.debug("I see that the leader is online")
        elif msg_command == "follower-online":
            if self.roadblock_role == "leader":
                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["online"]:
                    logger.info("Received 'follower-online' message from '%s'", msg_sender)
                    self.followers["online"].remove(msg_sender)
                elif msg_sender in self.roadblock_followers:
                    logger.warning("Did I already process this 'follower-online' message from follower '%s'?", msg_sender)
                else:
                    logger.info("Received 'follower-online' message from unknown follower '%s'", msg_sender)

                if len(self.followers["online"]) == 0:
                    logger.info("Sending 'all-online' message")
                    self.stream_add("followers", self.message_build("all", "all", "all-online"))
                    if self.initiator.is_set():
                        self.send_user_messages()
        elif msg_command == "all-online":
            if self.initiator.is_set():
                logger.info("Initiator received 'all-online' message")
            else:
                logger.info("Received 'all-online' message")

            self.send_user_messages()

            if self.roadblock_role == "follower":
                if self.abort:
                    logger.info("Sending 'follower-ready-abort' message")
                    self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready-abort"))
                else:
                    if self.wait_for is not None and self.wait_for_process is not None and self.wait_for_process.poll() is None:
                        self.wait_for_waiting.set()
                        logger.info("Sending 'follower-ready-waiting' message")
                        self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready-waiting"))
                    else:
                        logger.info("Sending 'follower-ready' message")
                        self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-ready"))
        elif msg_command in ("follower-ready", "follower-ready-abort", "follower-ready-waiting"):
            if self.roadblock_role == "leader":
                logger.debug("leader got a '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_command == "follower-ready-abort":
                    self.leader_abort.set()
                elif msg_command == "follower-ready-waiting":
                    self.roadblock_waiting.set()

                    logger.info("Adding follower '%s' to the waiting list", msg_sender)
                    self.followers["busy_waiting"].add(msg_sender, True)

                if msg_sender in self.followers["ready"]:
                    logger.info("Received '%s' message from '%s'", msg_command, msg_sender)
                    self.followers["ready"].remove(msg_sender)
                elif msg_sender in self.roadblock_followers:
                    logger.warning("Received a redundant '%s' message from follower '%s'?", msg_command, msg_sender)
                else:
                    logger.info("Received '%s' message from unknown follower '%s'", msg_command, msg_sender)

                if len(self.followers["ready"]) == 0:
                    logger.info("Sending 'all-ready' message")
                    self.stream_add("followers", self.message_build("all", "all", "all-ready"))

                    if self.leader_abort.is_set():
                        logger.info("Sending 'all-abort' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-abort"))
                    elif self.roadblock_waiting.is_set():
                        logger.info("Sending 'all-wait' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-wait"))

                        logger.info("Disabling original timeout handler")
                        self.disable_timeout()

                        logger.info("Enabling heartbeat timeout handler")
                        self.enable_timeout(self.heartbeat_timeout, self.heartbeat_handler, "heartbeat_handler_1")

                        logger.info("Sending 'leader-heartbeat' message")
                        self.stream_add("followers", self.message_build("all", "all", "leader-heartbeat"))
                    else:
                        logger.info("Sending 'all-go' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-go"))
        elif msg_command == "all-wait":
            if self.roadblock_role == "follower":
                logger.info("Received 'all-wait' message")

                logger.info("Disabling original timeout")
                self.disable_timeout()
        elif msg_command == "leader-heartbeat":
            if self.roadblock_role == "follower":
                logger.info("Received 'leader-heartbeat' message")

                if self.simulate_heartbeat_timeout:
                    logger.critical("Not sending 'follower-heartbeat' because of heartbeat timeout simulation request")
                else:
                    logger.info("Sending 'follower-heartbeat' message")
                    self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-heartbeat"))
        elif msg_command  == "follower-heartbeat":
            if self.roadblock_role == "leader":
                logger.info("Received '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["waiting"]:
                    logger.info("Received heartbeat from follower '%s'", msg_sender)
                    self.followers["waiting"].remove(msg_sender)
                elif msg_sender in self.roadblock_followers:
                    logger.warning("Received a redundant heartbeat message from follower '%s'?", msg_sender)
                else:
                    logger.warning("Received a heartbeat message from an unknown follower '%s'", msg_sender)

                if len(self.followers["busy_waiting"]) == 0 and len(self.followers["waiting"]) == 0:
                    logger.info("Disabling heartbeat timeout")
                    self.disable_timeout()

                    if self.waiting_failed:
                        self.leader_abort.set()
                        logger.info("Sending 'all-abort' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-abort"))
                    else:
                        logger.info("Sending 'all-go' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-go"))
        elif msg_command in ("follower-waiting-complete", "follower-waiting-complete-failed"):
            if self.roadblock_role == "leader":
                logger.info("Received '%s' message", msg_command)

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["busy_waiting"]:
                    logger.info("Follower '%s' is no longer busy waiting", msg_sender)
                    self.followers["busy_waiting"].remove(msg_sender)

                    if msg_command == "follower-waiting-complete-failed":
                        logger.warning("Follower '%s' completed waiting but failed", msg_sender)
                        self.waiting_failed = True
                elif msg_sender in self.roadblock_followers:
                    logger.warning("Received '%s' from a follower '%s' that is not busy waiting?", msg_command, msg_sender)
                else:
                    logger.info("Received '%s' message from an unknown follower '%s'", msg_command, msg_sender)

                if len(self.followers["busy_waiting"]) == 0 or self.waiting_failed:
                    logger.info("Disabling heartbeat timeout")
                    self.disable_timeout()

                    if self.waiting_failed:
                        self.leader_abort_waiting = True
                        logger.info("Sending 'all-abort' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-abort"))
                    else:
                        logger.info("Sending 'all-go' command")
                        self.stream_add("followers", self.message_build("all", "all", "all-go"))
        elif msg_command == "heartbeat-timeout":
            if self.roadblock_role == "follower":
                logger.info("Received '%s' message", msg_command)

                logger.critical("Roadblock failed due to a heartbeat timeout")

                self.timeout_internals()

                # signal myself to exit
                self.watch_stream.clear()

                self.rc = self.RC_HEARTBEAT_TIMEOUT
                return self.rc
        elif msg_command == "all-ready":
            logger.info("Received 'all-ready' message")
        elif msg_command in ("all-go", "all-abort"):
            if self.roadblock_role == "follower":
                if msg_command == "all-go":
                    logger.info("Received 'all-go' from leader")
                else:
                    logger.info("Received 'all-abort' from leader")
                    self.follower_abort = True

                    if self.wait_for is not None and self.wait_for_process is not None:
                        logger.critical("Killing wait_for process due to 'all-abort' from leader")
                        self.wait_for_process.kill()

                # tell the leader that I'm gone
                logger.info("Sending 'follower-gone' message")
                self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-gone"))

                # signal myself to exit
                self.watch_stream.clear()
        elif msg_command == "follower-gone":
            if self.roadblock_role == "leader":
                logger.debug("leader got a 'follower-gone' message")

                msg_sender = self.message_get_sender(message)

                if msg_sender in self.followers["gone"]:
                    logger.info("Received 'follower-gone' message from '%s'", msg_sender)
                    self.followers["gone"].remove(msg_sender)
                elif msg_sender in self.roadblock_followers:
                    logger.warning("Received a redundant 'follower-gone' message from follower '%s'?", msg_sender)
                else:
                    logger.info("Received 'follower-gone' message from unknown follower '%s'", msg_sender)

                if len(self.followers["gone"]) == 0:
                    # send a message that will probably not be observed by
                    # anyone...but just in case...
                    logger.info("Sending 'all-gone' message")
                    self.stream_add("followers", self.message_build("all", "all", "all-gone"))

                    # signal myself to exit
                    self.watch_stream.clear()
        elif msg_command == "initiator-info":
            self.initiator_type = self.message_get_sender_type(message)
            self.initiator_id = self.message_get_sender(message)
            logger.debug("Received an 'initiator-info' message with type='%s' and id='%s'", self.initiator_type, self.initiator_id)

        return self.RC_SUCCESS

    def stream_add(self, stream_name, message):
        '''Add a message to a stream, creating the stream if necessary'''

        ret_val = 0
        counter = 0

        logger.debug("Attempting to add message '%s' to stream '%s'", message, stream_name)

        while ret_val == 0:
            if self.rc != 0:
                logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1

            try:
                ret_val = self.redcon.xadd(self.roadblock_uuid + "__stream__" + stream_name, { 'msg': self.message_to_str(message) })
            except redis.exceptions.ConnectionError as con_error:
                logger.error("%s", con_error)
                logger.error("Stream add to '%s' failed due to connection error!", stream_name)
            except redis.exceptions.TimeoutError as con_error:
                logger.error("%s", con_error)
                logger.error("Stream add to '%s' failed due to a timeout error!", stream_name)

            if ret_val is None:
                logger.warning("Failed attempt %d to add message '%s' to stream '%s'", counter, message, stream_name)

                self.backoff(counter)
            else:
                logger.debug("Message '%s' was sent on the %d attempt with message ID '%s' on stream '%s'", message, counter, ret_val, stream_name)

        if self.message_log is not None:
            # if the message log is open then append messages to the queue
            # for later dumping
            # do something stream specific here
            self.messages["sent"].append(message)

        return self.RC_SUCCESS

    def key_delete(self, key):
        '''Delete a key from redis'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # this call should return the number of keys deleted which is
            # expected to be one, if not we retry
            ret_val = self.redcon.delete(key)

            if ret_val == 0:
                logger.warning("Failed attempt %d to delete key '%s'", counter, key)

                self.backoff(counter)

        return self.RC_SUCCESS

    def key_set_once(self, key, value):
        '''Set a key once in redis'''

        ret_val = 0
        counter = 0
        while ret_val == 0:
            if self.rc != 0:
                logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # this call should return one on success, if not we retry
            ret_val = self.redcon.msetnx( { key: value } )

            if ret_val == 0:
                logger.warning("Failed attempt %d to set key '%s' with value '%s' once", counter, key, value)

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
                logger.debug("self.rc != 0 --> breaking")
                break

            counter += 1
            # if this call returns 0 then it failed somehow since it
            # should be the size of the list after we have added to it, so
            # we retry
            ret_val = self.redcon.rpush(key, value)

            if ret_val == 0:
                logger.warning("Failed attempt %d to append value '%s' to key '%s'", counter, value, key)

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

        logger.info("Cleaning up")

        self.disable_timeout()

        if self.wait_for is not None:
            logger.info("Closing wait_for monitor thread")
            self.wait_for_monitor_exit.set()
            self.wait_for_monitor_thread.join()

            logger.info("Closing wait_for launcher thread")
            self.wait_for_launcher_thread.join()

        if self.con_pool_active.is_set():
            if self.roadblock_role == "leader":
                logger.info("Removing db objects specific to this roadblock")
                self.key_delete(self.roadblock_uuid)
                self.key_delete(self.roadblock_uuid + "__initialized")

                streams_to_clean = []
                for stream_name in ( "global", "leader", "followers" ):
                    streams_to_clean.append(stream_name)
                streams_to_clean.append(self.roadblock_leader_id)
                for stream_name in self.roadblock_followers:
                    streams_to_clean.append(stream_name)

                for stream_name in streams_to_clean:
                    msg_count = self.redcon.xlen(self.roadblock_uuid + "__stream__" + stream_name)
                    logger.debug("total messages on stream '%s': %d", stream_name, msg_count)

                    msgs_trimmed = self.redcon.xtrim(self.roadblock_uuid + "__stream__" + stream_name, maxlen = 0, approximate = False)
                    logger.debug("total messages deleted from stream '%s': %d", stream_name, msgs_trimmed)

                    msg_count = self.redcon.xlen(self.roadblock_uuid + "__stream__" + stream_name)
                    logger.debug("total messages on stream '%s': %d", stream_name, msg_count)

            if self.connection_watchdog_state == "enabled":
                logger.info("Closing connection pool watchdog")
                self.con_watchdog_exit.set()
                self.con_watchdog.join()

            logger.info("Closing connection pool")
            self.con_pool_active.clear()
            self.con_pool.disconnect()

        if self.message_log is not None:
            # if the message log is open then dump the message queue and
            # close the file handle
            print(f"{json.dumps(self.messages, cls = roadblock_json_encoder, indent = 4, separators=(',', ': '), sort_keys = False)}\n", file=self.message_log)
            self.message_log.close()

        logger.debug("Processed Messages:")
        for msg in self.processed_messages:
            logger.debug("\t%s", msg)

        if self.abort_event_thread is not None:
            # tell the abort event loop/thread to exit
            logger.info("Closing abort event handler")
            self.abort_event_loop.set()

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
                    logger.critical("Killing wait_for process due to timeout")
                    self.wait_for_process.kill()
                else:
                    logger.debug("wait_for process has already exited")
            else:
                logger.critical("The wait-for process object is missing")

        if self.con_pool_active.is_set() and self.initiator.is_set():
            # set a persistent flag that the roadblock timed out so that
            # any late arriving members know that the roadblock has
            # already failed.  done by the first member since that is the
            # only member that is guaranteed to have actually reached the
            # roadblock and be capable of setting this.
            self.key_set_once(self.roadblock_uuid + "__timedout", int(True))

        self.cleanup()

        if self.roadblock_role == "leader":
            if len(self.followers["online"]) != 0:
                logger.critical("These followers never reached 'online': %s", self.get_followers_list(self.followers["online"]))
            elif len(self.followers["ready"]) != 0:
                logger.critical("These followers never reached 'ready': %s", self.get_followers_list(self.followers["ready"]))
            elif self.roadblock_waiting.is_set():
                if len(self.followers["busy_waiting"]) != 0:
                    logger.critical("These followers were still 'busy waiting': %s", self.get_followers_list(self.followers["busy_waiting"]))

                if len(self.followers["waiting"]) != 0:
                    logger.critical("These followers did not respond to the heartbeat and were still 'waiting': %s", self.get_followers_list(self.followers["waiting"]))
            elif len(self.followers["gone"]) != 0:
                logger.critical("These followers never reached 'gone': %s", self.get_followers_list(self.followers["gone"]))

        return self.RC_SUCCESS

    def do_heartbeat_timeout(self):
        '''Handle a heartbeat timeout event'''

        if len(self.followers["waiting"]) != 0:
            logger.critical("Failed ending current heartbeat monitoring period -> heartbeat timeout")

            self.rc = self.RC_HEARTBEAT_TIMEOUT

            logger.info("Sending 'heartbeat-timeout' message")
            self.stream_add("followers", self.message_build("all", "all", "heartbeat-timeout"))

            self.timeout_internals()

            return self.rc
        else:
            logger.info("Successfully ending current heartbeat monitoring period")
            self.disable_timeout()

            # rebuild the tracking list by copying from a backup
            self.followers["waiting"] = copy.deepcopy(self.followers["waiting_backup"])

            logger.info("Sending 'leader-heartbeat' message")
            self.stream_add("followers", self.message_build("all", "all", "leader-heartbeat"))

            logger.info("Starting new heartbeat monitoring period")
            self.enable_timeout(self.heartbeat_timeout, self.heartbeat_handler, "heartbeat_handler_2")

        return self.RC_SUCCESS

    def do_timeout(self):
        '''Handle a roadblock timeout event'''

        self.rc = self.RC_TIMEOUT

        logger.critical("Roadblock failed with timeout")

        self.timeout_internals()

        return self.rc

    def timeout_handler(self):
        '''Handle roadblock timeout'''

        logger.debug("Starting timeout handler thread")

        self.timeout_active.clear()
        self.do_timeout()

        logger.debug("Finishing timeout handler thread")

        return self.rc

    def heartbeat_handler(self):
        '''Handle heartbeat timeout'''

        logger.debug("Starting heartbeat timeout handler thread")

        self.timeout_active.clear()
        self.do_heartbeat_timeout()

        logger.debug("Finishing heartbeat timeout handler thread")

        return self.RC_SUCCESS

    def abort_event_handler(self):
        '''thread to handle minor/major abort events signaled by the caller'''

        logger.debug("Starting abort event thread handler")

        while not self.abort_event_loop.is_set():
            #logger.debug("Starting a pass through the abort event loop")

            if self.major_abort_event is not None and self.major_abort_event.is_set():
                self.major_abort_event.clear()
                self.major_abort_event_processed.set()

                logger.critical("Exiting due to a major abort event")
                self.cleanup()

                # signal myself to exit
                self.watch_stream.clear()

                self.rc = self.RC_ERROR
            elif self.minor_abort_event is not None and self.minor_abort_event.is_set():
                self.minor_abort_event.clear()
                self.minor_abort_event_processed.set()

                logger.warning("Attempting to abort due to a minor abort event")
                self.leader_abort.set()

            time.sleep(0.01)
            #time.sleep(1.0)

        logger.debug("Finishing abort event thread handler")

        return self.RC_SUCCESS

    def connection_watchdog(self):
        '''Check if the redis connection is still open'''

        logger.debug("Starting connection watchdog thread")

        while not self.con_watchdog_exit.is_set():
            time.sleep(1)
            try:
                if self.con_pool_active.is_set():
                    ping_begin = time.time_ns()
                    self.redcon.ping()
                    ping_end = time.time_ns()
                    logger.debug("Connection watchdog ping succeeded in %f milliseconds", ((ping_end - ping_begin) / (10 ** 6)))
                else:
                    logger.error("Connection watchdog ping skipped due to disconnected state")
            except redis.exceptions.ConnectionError as con_error:
                logger.error("%s", con_error)
                logger.error("Connection watchdog ping failed")

        logger.debug("Finishing connection watchdog thread")

        return self.RC_SUCCESS

    def wait_for_process_io_handler(self):
        '''Handle the output logging of a --wait-for program/script process'''

        logger.debug("Starting wait for process io handler thread")

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

        logger.debug("Finishing wait for process io handler thread")

        return self.RC_SUCCESS

    def wait_for_process_monitor(self):
        '''Monitor the status of a --wait-for program/script process'''

        logger.debug("Starting wait for process monitor thread")

        logger.debug("The wait_for monitor is waiting to start")

        self.wait_for_monitor_start.wait()

        logger.debug("The wait_for monitor is starting")

        while not self.wait_for_monitor_exit.is_set():
            if self.wait_for_process is None:
                logger.critical("There is no wait_for_process to monitor")
                self.wait_for_monitor_exit.set()
            else:
                if self.wait_for_process.poll() is None:
                    logger.debug("The wait_for_process is still running")
                    time.sleep(1)
                else:
                    logger.info("The wait_for process has finished")
                    self.wait_for_monitor_exit.set()

                    if self.wait_for_waiting.is_set():
                        if self.wait_for_process.returncode != 0:
                            self.wait_for_io_handler_exited.wait()

                            log_contents = ""
                            with open(self.wait_for_log, "r", encoding = "ascii") as wait_for_log_fh:
                                log_contents = str(base64.b64encode(lzma.compress(wait_for_log_fh.read().encode("ascii"))), "ascii")

                            logger.critical("Sending 'follower-waiting-complete-failed' message")
                            self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-waiting-complete-failed", value = log_contents))
                        else:
                            logger.info("Sending 'follower-waiting-complete' message")
                            self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-waiting-complete"))

        logger.debug("Finishing the wait for process monitor thread")

        return self.RC_SUCCESS

    def wait_for_process_launcher(self):
        '''Handle the execution of a --wait-for program/script'''

        logger.debug("Starting wait for process launcher thread")

        ret_val = -1

        try:
            self.wait_for_process = subprocess.Popen(self.wait_for_cmd,
                                                     bufsize = 0,
                                                     encoding = 'ascii',
                                                     stdout = subprocess.PIPE,
                                                     stderr = subprocess.STDOUT)

            if self.wait_for_process is None:
                logger.critical("The wait_for process failed to launch")
                ret_val = 1
                self.wait_for_monitor_start.set()
            else:
                logger.info("The wait-for process is now running")

                self.wait_for_monitor_start.set()
                wait_for_io_thread = threading.Thread(target = self.wait_for_process_io_handler, args = (), name = "wait_for_monitor")
                wait_for_io_thread.start()

                ret_val = self.wait_for_process.wait()
                wait_for_io_thread.join()

        except PermissionError:
            logger.critical("Received a permission error when attempting to execute --wait-for command")
            ret_val = 1
            self.wait_for_monitor_start.set()

        logger.info("The wait_for process exited with return code %d", ret_val)

        logger.debug("Finishing wait for process launcher thread")

        return self.RC_SUCCESS

    def last_msg_id_advance(self, stream_name, msg_id):
        '''advance the last msg id for the specified stream'''

        if stream_name == self.roadblock_uuid + "__stream__global":
            self.global_prev_last_msg_id = self.global_last_msg_id
            self.global_last_msg_id = msg_id
            logger.debug("global_last_msg_id is now '%s'", self.global_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__leader":
            self.leader_prev_last_msg_id = self.leader_last_msg_id
            self.leader_last_msg_id = msg_id
            logger.debug("leader_last_msg_id is now '%s'", self.leader_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__followers":
            self.followers_prev_last_msg_id = self.followers_last_msg_id
            self.followers_last_msg_id = msg_id
            logger.debug("followers_last_msg_id is now '%s'", self.followers_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__" + self.my_id:
            self.personal_prev_last_msg_id = self.personal_last_msg_id
            self.personal_last_msg_id = msg_id
            logger.debug("personal_last_msg_id is now '%s'", self.personal_last_msg_id)

    def last_msg_id_revert(self, stream_name):
        '''revert the last msg id for the specified stream'''

        if stream_name == self.roadblock_uuid + "__stream__global":
            self.global_last_msg_id = self.global_prev_last_msg_id
            self.global_prev_last_msg_id = None
            logger.debug("global_last_msg_id is now '%s'", self.global_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__leader":
            self.leader_last_msg_id = self.leader_prev_last_msg_id
            self.leader_prev_last_msg_id = None
            logger.debug("leader_last_msg_id is now '%s'", self.leader_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__followers":
            self.followers_last_msg_id = self.followers_prev_last_msg_id
            self.followers_prev_last_msg_id = None
            logger.debug("followers_last_msg_id is now '%s'", self.followers_last_msg_id)
        elif stream_name == self.roadblock_uuid + "__stream__" + self.my_id:
            self.personal_last_msg_id = self.personal_prev_last_msg_id
            self.personal_prev_last_msg_id = None
            logger.debug("personal_last_msg_id is now '%s'", self.personal_last_msg_id)

    def message_processing_handler(self):
        '''process messages in a dedicated thread so the message retrieval loop can concentrate on that'''

        logger.debug("starting message processing thread")

        while True:
            if not self.watch_stream.is_set():
                if self.message_processing_force_exit.is_set():
                    logger.debug("exiting message processing loop without draining the queue")
                    break

                if self.message_processing_queue.empty():
                    logger.debug("exiting message processing loop")
                    break

                logger.debug("signal received to exit message processing loop but messages to process remain in the queue")

            msg = None
            try:
                msg = self.message_processing_queue.get(timeout = 0.001) # 1 millisecond
            except queue.Empty:
                # the following line can generate massive amounts of
                # output so it is commented out by default
                #logger.verbose_debug("received a message processing queue empty exception")
                continue

            if msg is None:
                logger.debug("received a null message")
                continue

            logger.debug("retrieved message from the message processing queue")

            ret_val = self.message_handle(msg)
            if ret_val != 0:
                logger.error("message generated return code %d", ret_val)
                self.watch_stream.clear()

            logger.debug("notifying message processing queue that message processing has completed")
            self.message_processing_queue.task_done()

        logger.debug("stopping message processing thread")

    def run_it(self):
        '''execute the roadblock'''

        logger.info("Executing Roadblock")
        logger.info("Roadblock UUID: %s", self.roadblock_uuid)

        if self.minor_abort_event is not None or self.major_abort_event is not None:
            self.abort_event_thread = threading.Thread(target = self.abort_event_handler, args = (), name = "abort_event_thread")
            self.abort_event_thread.start()

        if len(self.roadblock_leader_id) == 0:
            logger.critical("You must specify the leader's ID using --leader-id")
            return self.RC_INVALID_INPUT

        if self.roadblock_role == "leader":
            if len(self.roadblock_followers) == 0:
                logger.critical("There must be at least one follower")
                return self.RC_INVALID_INPUT
            if self.abort:
                self.leader_abort.set()

            # build some hashes for easy tracking of follower status
            for follower in self.roadblock_followers:
                self.followers["online"].add(follower, True)
                self.followers["ready"].add(follower, True)
                self.followers["gone"].add(follower, True)
                self.followers["waiting"].add(follower, True)
                self.followers["waiting_backup"].add(follower, True)

        if self.roadblock_role == "follower":
            self.my_id = self.roadblock_follower_id
        elif self.roadblock_role == "leader":
            self.my_id = self.roadblock_leader_id

        if self.message_log is not None:
            # open the message log, if specified
            try:
                self.message_log = open(self.message_log, "w", encoding="ascii")
            except IOError:
                logger.critical("Could not open message log '%s' for writing!", self.message_log)
                return self.RC_INVALID_INPUT

        self.define_msg_schema()
        self.define_usr_msg_schema()

        if self.user_messages is not None:
            # load the user messages, if specified
            try:
                with open(self.user_messages, "r", encoding="ascii") as user_messages:
                    self.user_messages = json.load(user_messages)
            except IOError:
                logger.critical("Could not load the user messages '%s'!", self.user_messages)
                return self.RC_INVALID_INPUT

            try:
                jsonschema.validate(instance=self.user_messages, schema=self.user_schema)
            except jsonschema.exceptions.SchemaError as exception:
                logger.critical(exception)
                logger.critical("Could not JSON validate the user messages!")
                return self.RC_INVALID_INPUT

        mytime = calendar.timegm(time.gmtime())
        logger.info("Current Time: %s", datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC"))

        # set the default timeout
        self.enable_timeout(self.roadblock_timeout, self.timeout_handler, "timeout_handler_1")
        cluster_timeout = mytime + self.roadblock_timeout
        logger.info("Timeout: %s", datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC"))

        if self.wait_for is not None:
            logger.info("Wait-For: True")
            logger.info("Wait-For Task: %s", self.wait_for)
            logger.info("Wait-For Log: %s", self.wait_for_log)
            self.wait_for_monitor_start = threading.Event()
            self.wait_for_launcher_thread = threading.Thread(target = self.wait_for_process_launcher, args = (), name = "wait_for_launcher")
            self.wait_for_monitor_thread = threading.Thread(target = self.wait_for_process_monitor, args = (), name = "wait_for_monitor")
            self.wait_for_monitor_exit = threading.Event()
            self.wait_for_launcher_thread.start()
            self.wait_for_monitor_thread.start()
        else:
            logger.info("Wait-For: False")

        # create the redis connections
        while not self.con_pool_active.is_set():
            try:
                self.con_pool = redis.ConnectionPool(host = self.roadblock_redis_server,
                                                     password = self.roadblock_redis_password,
                                                     port = 6379,
                                                     db = 0,
                                                     socket_connect_timeout = 5,
                                                     health_check_interval = 1,
                                                     connection_class = DebuggingConnection)
                self.redcon = redis.Redis(connection_pool = self.con_pool)
                self.redcon.ping()
                self.con_pool_active.set()
            except redis.exceptions.ResponseError as con_error:
                match = re.search(r"WRONGPASS", str(con_error))
                if match:
                    logger.error("Invalid username/password pair")
                    return self.RC_INVALID_INPUT
                else:
                    logger.error("%s", con_error)
                    logger.error("Redis connection could not be opened due to response error!")
                    time.sleep(3)
            except redis.exceptions.ConnectionError as con_error:
                logger.error("%s", con_error)
                logger.error("Redis connection could not be opened due to connection error!")
                time.sleep(3)
            except redis.exceptions.TimeoutError as con_error:
                logger.error("%s", con_error)
                logger.error("Redis connection could not be opened due to a timeout error!")
                time.sleep(3)

        logger.info("Connection watchdog: %s", self.connection_watchdog_state)
        if self.connection_watchdog_state == "enabled":
            logger.debug("Creating connection watchdog")
            self.con_watchdog_exit = threading.Event()
            self.con_watchdog = threading.Thread(target = self.connection_watchdog, args = (), name = "connection_watchdog")
            self.con_watchdog.start()

        logger.info("Role: %s", self.roadblock_role)
        if self.roadblock_role == "follower":
            logger.info("Follower ID: %s", self.roadblock_follower_id)
            logger.info("Leader ID: %s", self.roadblock_leader_id)
        elif self.roadblock_role == "leader":
            logger.info("Leader ID: %s", self.roadblock_leader_id)
            logger.info("Total followers: %d", len(self.roadblock_followers))
            logger.info("Followers: %s", self.roadblock_followers)
        if self.abort:
            logger.info("Abort: True")
        else:
            logger.info("Abort: False")

        # check if the roadblock was previously created and already timed
        # out -- ie. I am very late
        if self.key_check(self.roadblock_uuid + "__timedout"):
            logger.critical("Detected previous timeout for this roadblock")
            self.do_timeout()
            return self.rc

        # check if the roadblock has been initialized yet
        if self.key_set(self.roadblock_uuid, mytime):
            # i am creating the roadblock
            self.initiator.set()
            logger.info("Initiator: True")

            # create the streams by adding the first message
            logger.info("Creating consumer groups and streams")
            self.stream_add("global", self.message_build("all", "all", "global-stream-created"))
            self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "leader-stream-created"))
            self.stream_add("followers", self.message_build("all", "all", "followers-stream-created"))

            # share the cluster timeout
            logger.info("Sending 'timeout-ts' message")
            self.stream_add("global", self.message_build("all", "all", "timeout-ts", cluster_timeout))

            # share the initiator information
            logger.info("Sending 'initiator-info' message")
            self.stream_add("global", self.message_build("all", "all", "initiator-info"))
            self.initiator_type = self.roadblock_role
            self.initiator_id = self.my_id

            self.list_append(self.roadblock_uuid + "__initialized", int(True))
        else:
            logger.info("Initiator: False")

            # the roadblock already exists, make sure it is initialized
            # completely before proceeding
            logger.info("Waiting for roadblock initialization to complete")

            # wait until the initialized flag has been set for the roadblock
            while not self.key_check(self.roadblock_uuid + "__initialized"):
                if self.rc != 0:
                    logger.debug("self.rc != 0 --> breaking")
                    break

                time.sleep(1)
                logger.info(".")

            logger.info("Roadblock is initialized")

        # create the personal stream
        logger.info("Creating personal stream")
        self.stream_add(self.my_id, self.message_build_custom(self.roadblock_role, "personal-stream-created", self.roadblock_role, self.my_id, "personal-stream-created"))

        if self.roadblock_role == "follower":
            # tell the leader that I am online
            logger.info("Sending 'follower-online' message to the leader")
            self.stream_add("leader", self.message_build("leader", self.roadblock_leader_id, "follower-online"))
        elif self.roadblock_role == "leader":
            # tell the followers that the leader is online
            logger.info("Sending 'leader-online' message to the followers")
            self.stream_add("followers", self.message_build("all", "all", "leader-online"))

        self.message_processing_thread = threading.Thread(target = self.message_processing_handler, args = (), name = "message_processing_handler")
        self.message_processing_thread.start()

        loop_counter = 0
        empty_loop_counter = 0
        empty_loop_notification_level = 10
        while self.watch_stream.is_set():
            if self.rc != 0:
                logger.debug("self.rc != 0 --> exiting stream watching loop")

                # tell myself and the message processing handler that
                # we can quit
                logger.debug("disabling stream watching")
                self.watch_stream.clear()

                # tell the message processing thread to force exit
                # without processing any remaining messages in the
                # queue
                self.message_processing_force_exit.set()

                break

            loop_counter += 1

            msgs = []
            if self.con_pool_active.is_set():
                try:
                    if self.roadblock_role == "follower":
                        msgs = self.redcon.xread(streams = {
                            self.roadblock_uuid + "__stream__global": self.global_last_msg_id,
                            self.roadblock_uuid + "__stream__followers": self.followers_last_msg_id,
                            self.roadblock_uuid + "__stream__" + self.my_id: self.personal_last_msg_id
                        }, block = 1)
                    elif self.roadblock_role == "leader":
                        msgs = self.redcon.xread(streams = {
                            self.roadblock_uuid + "__stream__global": self.global_last_msg_id,
                            self.roadblock_uuid + "__stream__leader": self.leader_last_msg_id,
                            self.roadblock_uuid + "__stream__" + self.my_id: self.personal_last_msg_id
                        }, block = 1)
                except redis.exceptions.ConnectionError as con_error:
                    if self.con_pool_active.is_set():
                        logger.error("%s", con_error)
                        logger.error("Stream read failed due to connection error!")
                    else:
                        logger.debug("%s", con_error)
                        logger.debug("Stream read failed because the connection has been closed")
                except redis.exceptions.TimeoutError as con_error:
                    logger.error("%s", con_error)
                    logger.error("Stream read failed due to a timeout error!")

            if len(msgs) == 0:
                empty_loop_counter += 1

                if empty_loop_counter == empty_loop_notification_level:
                    logger.debug("failed to retrieve any messages from any streams for %d loops", empty_loop_notification_level)
                    empty_loop_counter = 0
            else:
                empty_loop_counter = 0

                logger.debug("retrieved messages from %d streams after %d loops since last retrieval", len(msgs), loop_counter)

                loop_counter = 0

                for stream in msgs:
                    stream_name = stream[0].decode()

                    logger.debug("retrieved %d messages from stream '%s' for processing", len(stream[1]), stream_name)

                    for msg_id, msg in stream[1]:
                        logger.debug("received msg=[%s] with msg_id=[%s] from stream '%s'", msg, msg_id, stream_name)

                        self.last_msg_id_advance(stream_name, msg_id)

                        msg = self.message_from_str(msg[b"msg"].decode())

                        message_for_me = self.message_for_me(msg)
                        if message_for_me is None:
                            logger.debug("reverting last message id update due to incomplete message")

                            self.last_msg_id_revert(stream_name)

                            logger.debug("stopping processing of messages from stream '%s' for this loop -- the messages need to be retrieved again", stream_name)

                            break

                        if not message_for_me:
                            logger.debug("received a message which is not for me!")
                        else:
                            if not self.message_validate(msg):
                                logger.error("received a message for me which did not validate! [%s]", msg)

                                logger.debug("reverting last message id update due to message validation failure")

                                self.last_msg_id_revert(stream_name)

                                logger.debug("stopping processing of messages from stream '%s' for this loop -- the messages need to be retrieved again", stream_name)

                                break

                            logger.debug("received a validated message for me!")

                            logger.debug("adding message to the message processing queue")
                            self.message_processing_queue.put(msg)

        logger.debug("Exited watch stream loop")

        logger.debug("joining message processing thread")
        self.message_processing_thread.join()
        logger.debug("message processing thread joined")

        if self.rc == 0:
            self.cleanup()

        logger.info("Exiting")

        if self.major_abort_event_processed.is_set() and self.rc != 0:
            logger.critical("Roadblock Completed with a major abort event")
            return self.RC_ERROR

        if self.rc == self.RC_HEARTBEAT_TIMEOUT:
            logger.critical("Roadblock Completed with a Heartbeat Timeout")
            return self.RC_HEARTBEAT_TIMEOUT

        if self.leader_abort_waiting:
            logger.critical("Roadblock Completed with a Waiting Abort")
            return self.RC_ABORT_WAITING

        if self.leader_abort.is_set() or self.follower_abort:
            logger.critical("Roadblock Completed with an Abort")
            return self.RC_ABORT

        if self.rc != self.RC_SUCCESS:
            logger.info("Roadblock Completed with an Error")
            return self.rc
        else:
            logger.info("Roadblock Completed Successfully")
            return self.RC_SUCCESS
