#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
        2nd Task of FrogTek's Technical Test.

        Generates json files in a given directory
"""
import io
import os
import getopt
import sys
import uuid
import datetime
import json
import random
from time import sleep

from util_params import param_numeric_int

ERROR_PARAMS_GENERAL = "Error in parameters!"
ERROR_PARAMS_MISSING = "Some missing parameters!"
ERROR_PARAM_OUTPUTDIRECTORY = "Output directory doesn't exist"

FORMAT_DATE_FILE = '%Y-%m-%d-%H-%M-%S-%f'
FORMAT_TIMESTAMP_UTC = '%Y-%m-%dT%H:%M:%SZ'

ORDER_PLACED = 'OrderPlaced'
ORDER_ACCEPTED = 'OrderAccepted'
ORDER_CANCELLED = 'OrderCancelled'
USAGE_PROGRAM = ('USAGE: generate-event.py [-n|--number-of-orders] <number> '
                 '[-b|--batch-size] <number> '
                 '[-i|--interval <number>] '
                 '[-o|--output-directory] <directory_name>')

PREFIX_ORDERS = "orders-"
EXTENSION_JSON = ".json"

FIELD_ORDERID = "OrderId"
FIELD_TIMESTAMPUTC = "TimeStampUtc"
FIELD_TYPE = "Type"
FIELD_DATA = "Data"
random.seed()


def generate(number_of_orders, batch_size, interval, output_directory):
    filename = ""
    fileoutput = None
    for n_order in range(0, number_of_orders):
        if (n_order % batch_size) == 0:  # create a new file
            if filename:
                fileoutput.close()
                sleep(interval)
            filename = generate_file_name(PREFIX_ORDERS,
                                          output_directory,
                                          EXTENSION_JSON)

            fileoutput = io.open(filename, 'w+', encoding='utf-8')

        order_data = generate_data()
        first_event = generate_order(ORDER_PLACED, order_data)
        if random.randint(1, 5) == 1:  # (n_order % 5) == 0:
            second_event = generate_order(ORDER_CANCELLED, order_data)
        else:
            second_event = generate_order(ORDER_ACCEPTED, order_data)
        fileoutput.write(first_event + '\n' + second_event + '\n')

    fileoutput.close()


def generate_file_name(prefix, output_directory, extension):
    timecreated = datetime.datetime.utcnow().strftime(FORMAT_DATE_FILE)
    return os.path.join(output_directory, prefix + timecreated + extension)


def generate_data():
    id = str(uuid.uuid4())
    timestamp = datetime.datetime.utcnow().strftime(FORMAT_TIMESTAMP_UTC)
    return {FIELD_ORDERID: id, FIELD_TIMESTAMPUTC: timestamp}


def generate_order(type_order, data):
    return json.dumps({FIELD_TYPE: type_order, FIELD_DATA: data})


def main(argv):
    number_of_orders_str = ""
    batch_size_str = ""
    interval_str = ""
    number_of_orders = -1
    batch_size = -1
    interval = -1
    output_directory = ""
    try:
        opts, args = getopt.getopt(argv, "n:b:i:o:", ["number-of-orders=",
                                                      "batch-size=",
                                                      "interval=",
                                                      "output-directory"])
    except getopt.GetoptError:
        print(ERROR_PARAMS_GENERAL)
        print(USAGE_PROGRAM)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-n", "--number-of-orders"):
            number_of_orders_str = arg
        if opt in ("-b", "--batch-size"):
            batch_size_str = arg
        if opt in ("-i", "--interval"):
            interval_str = arg
        if opt in ("-o", "--output-directory"):
            output_directory = arg

    number_of_orders = param_numeric_int(number_of_orders_str)
    batch_size = param_numeric_int(batch_size_str)
    interval = param_numeric_int(interval_str)

    if not (number_of_orders or batch_size or interval or output_directory):
        print(ERROR_PARAMS_MISSING)
        print(USAGE_PROGRAM)
        sys.exit(2)

    if not os.path.isdir(output_directory):
        print(ERROR_PARAM_OUTPUTDIRECTORY)
        print(USAGE_PROGRAM)
        sys.exit(2)

    generate(number_of_orders, batch_size, interval, output_directory)


if __name__ == "__main__":
    main(sys.argv[1:])
