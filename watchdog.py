#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
        3rd Task of FrogTek's Technical Test.

        Watch a given directory looking for new .json files and
        show statistics every n seconds.

        It has 2 thread, the first one is watching for files and the
        second one show statistics.
"""
import sys
import pyinotify
import sched
import time
import getopt
import os


from util_params import param_numeric_int
from eventhandler import DirectoryEventHandler

MSG_STATISTICS = ('"OrderCancelled":{}   '
                  '"OrderAccepted":{}   '
                  '"OrderPlaced":{}   Diferences:{},{},{}  Files:{}    ')

USAGE_PROGRAM = ('USAGE: watchdog.py [-i|--interval <number>] '
                 '[-o|--directory_to_watch] <directory_name>')

ERROR_PARAMS_GENERAL = "Error in parameters!"
ERROR_PARAMS_MISSING = "Some missing parameters!"
ERROR_PARAM_DIRECTORY = "Directory to watch doesn't exist"

MSG_WATCHING = 'Watching {}'
MSG_EXITING = "\nExiting\n"

eh = DirectoryEventHandler()


def show_statistics(sc, last_Orders_Cancelled,
                    last_Orders_Accepted,
                    last_Orders_Placed):
    delta_cancelled = eh.Order_Cancelled - last_Orders_Cancelled
    delta_accepted = eh.Order_Accepted - last_Orders_Accepted
    delta_placed = eh.Order_Placed - last_Orders_Placed

    print(MSG_STATISTICS.format(eh.Order_Cancelled,
                                eh.Order_Accepted,
                                eh.Order_Placed,
                                delta_cancelled,
                                delta_accepted,
                                delta_placed,
                                eh.Files), end='')
    print('\r', end='')
    sys.stdout.flush()
    sc.enter(5, 1, show_statistics, (sc, eh.Order_Cancelled,
                                     eh.Order_Accepted,
                                     eh.Order_Placed))


def watchfiles(directory_name, interval):
    """
        Multithreaded Way of watch new files and write statistics in
        a given interval

        eh is a global eventhandler that stores the information of
        the files read. The thread show_statistics use that information
        to print the statistics in intervals.
    """
    wm = pyinotify.WatchManager()
    wm.add_watch(directory_name, pyinotify.ALL_EVENTS)

    event_notifier = pyinotify.ThreadedNotifier(wm, eh)
    event_notifier.start()

    s = sched.scheduler(time.time, time.sleep)
    s.enter(interval, 1, show_statistics, (s, 0, 0, 0,))
    try:
        s.run()
    except (KeyboardInterrupt, SystemExit):
        print(MSG_EXITING)
        event_notifier.stop()
        sys.exit()


def main(argv):
    interval_str = ""

    interval = 0
    directory = ""
    try:
        opts, args = getopt.getopt(argv, "d:i:", ["directory_to_watch=",
                                                  "interval="])
    except getopt.GetoptError:
        print(ERROR_PARAMS_GENERAL)
        print(USAGE_PROGRAM)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-i", "--interval"):
            interval_str = arg
        if opt in ("-o", "--directory_to_watch"):
            directory = arg

    interval = param_numeric_int(interval_str)

    if not (interval or directory):
        print(ERROR_PARAMS_MISSING)
        print(USAGE_PROGRAM)
        sys.exit(2)

    if not os.path.isdir(directory):
        print(ERROR_PARAM_DIRECTORY)
        print(USAGE_PROGRAM)
        sys.exit(2)

    print(MSG_WATCHING.format(directory))
    watchfiles(os.path.abspath(directory), interval)


if __name__ == "__main__":
    main(sys.argv[1:])
