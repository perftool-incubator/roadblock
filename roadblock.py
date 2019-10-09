#!/usr/bin/python3

import argparse
import string
import datetime
import time
import calendar
import socket
import redis
import signal

# define some global variables
class t_global(object):
    args = None
    redcon = None
    pubsubcon = None
    first = False

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

    followers = { 'ready': {},
                  'gone': {} }

    if t_global.args.roadblock_role == 'leader':
        if len(t_global.args.roadblock_followers) == 0:
            print("ERROR: There must be at least one follower")
            return(-1)

        # build some hashes for easy tracking of follower status
        for follower in t_global.args.roadblock_followers:
            followers['ready'][follower] = True
            followers['gone'][follower] = True

    # define a signal handler that will respond to SIGALRM when a
    # timeout even occurs
    signal.signal(signal.SIGALRM, sighandler)

    # create the redis connections
    t_global.redcon = redis.Redis(host = t_global.args.roadblock_redis_server,
                                  port = 6379,
                                  password = t_global.args.roadblock_redis_password)
    t_global.pubsubcon = t_global.redcon.pubsub(ignore_subscribe_messages = True)

    print("Role: %s" % (t_global.args.roadblock_role))
    if t_global.args.roadblock_role == 'follower':
        print("Follower ID: %s" % (t_global.args.roadblock_follower_id))
    elif t_global.args.roadblock_role == 'leader':
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
        t_global.redcon.msetnx({t_global.args.roadblock_uuid + '__timeout-timestamp': cluster_timeout})
        t_global.redcon.rpush(t_global.args.roadblock_uuid + '__online-status', 'initialized')
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
        cluster_timeout = int(t_global.redcon.get(t_global.args.roadblock_uuid + '__timeout-timestamp'))
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

    print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))

    status_index = -1
    get_out = False
    while True:
        # retrieve unprocessed status messages
        status_list = t_global.redcon.lrange(t_global.args.roadblock_uuid + '__online-status', status_index+1, -1)

        # process any retrieved status messages
        if len(status_list):
            for msg in status_list:
                status_index += 1
                msg = msg.decode().split('/')
                #print("received msg=[%s] status_index=[%d]" % (msg, status_index))
                
                if msg[0] == 'initialized':
                    if t_global.args.roadblock_role == 'leader':
                        # listen for messages published from the followers
                        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + '__followers')

                        print("Signaling online")
                        t_global.redcon.rpush(t_global.args.roadblock_uuid + '__online-status', 'leader_online')

                        get_out = True
                elif msg[0] == 'leader_online':
                    if t_global.args.roadblock_role == 'follower':
                        print("Received online status from leader")

                        # listen for messages published from the leader
                        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + '__leader')

                        print("Publishing ready message")
                        t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', t_global.args.roadblock_follower_id + '/ready')

                        get_out = True

        if get_out:
            break
        else:
            time.sleep(1)

    if t_global.args.roadblock_role == 'leader':
        for msg in t_global.pubsubcon.listen():
            #print(msg)
            # msg should be in the format "<follower name>/<status>"
            msg = msg['data'].decode().split('/')
            if msg[1] == 'ready':
                if msg[0] in followers['ready']:
                    print("Received ready message from '%s'" % (msg[0]))
                    del followers['ready'][msg[0]]
                elif msg[0] in t_global.args.roadblock_followers:
                    print("Did I already process this ready message from follower '%s'?" % (msg[0]))
                else:
                    print("Received ready message from unknown follower '%s'" % (msg[0]))

            if len(followers['ready']) == 0:
                print("All followers ready")
                print("Publishing go message")
                t_global.redcon.publish(t_global.args.roadblock_uuid + '__leader', "go")
                break
    elif t_global.args.roadblock_role == 'follower':
        for msg in t_global.pubsubcon.listen():
            #print(msg)
            if msg['data'].decode() == 'go':
                print("Received go message from leader")
                # stop listening for messages published from the leader
                t_global.pubsubcon.unsubscribe(t_global.args.roadblock_uuid + '__leader')
                print("Publishing gone message")
                t_global.redcon.publish(t_global.args.roadblock_uuid + '__followers', t_global.args.roadblock_follower_id + '/gone')
                print("Cleaning up")
                cleanup()
                print("Exiting")
                return(0)

    if t_global.args.roadblock_role == 'leader':
        for msg in t_global.pubsubcon.listen():
            #print(msg)
            # msg should be in the format "<follower name>/<status>"
            msg = msg['data'].decode().split('/')
            if msg[1] == 'gone':
                if msg[0] in followers['gone']:
                    print("Received gone message from '%s'" % (msg[0]))
                    del followers['gone'][msg[0]]
                elif msg[0] in t_global.args.roadblock_followers:
                    print("Did I already process this gone message from follower '%s'?" % (msg[0]))
                else:
                    print("Received gone message from unknown follower '%s'" % (msg[0]))

            if len(followers['gone']) == 0:
                print("All followers gone")
                # stop listening for messages published from the followers
                t_global.pubsubcon.unsubscribe(t_global.args.roadblock_uuid + '__followers')
                print("Cleaning up")
                cleanup()
                print("Exiting")
                return(0)

if __name__ == "__main__":
    exit(main())
