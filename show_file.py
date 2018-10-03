#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import getopt
import sys
import re


USAGE_PROGRAM = ('USAGE: massage_file.py [-i|--inputfile] '
                 '<filename> [-o|--outputfile] <filename> '
                 '[-e|--encoding] <encoding>]')
ERROR_PARAMS_GENERAL = "Error in parameters!"
ERROR_PARAMS_MISSING = "Some missing parameters!"

ERROR_FIELD1_NAN = "Field number 1 is not numeric!:{}"

RE_ACCOUNT_NUMBER = '[0-9]{2,4}[-]{0,1}[/]{0,1}[0-9]{2,4}'
RE_ID = '[0-9]{1,4}'

MSG_STATISTICS = 'Lines Read:{} Records Written:{}\n'


def main(argv):
    inputfilename = ""
    outputfilename = ""
    encoding = ""
    try:
        opts, args = getopt.getopt(argv, "i:o:e:", ["inputfile=",
                                                    "outputfile=",
                                                    "encoding="])
    except getopt.GetoptError:
        print(ERROR_PARAMS_GENERAL)
        print(USAGE_PROGRAM)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-i", "--inputfile"):
            inputfilename = arg
        if opt in ("-o", "--outputfile"):
            outputfilename = arg
        if opt in ("-e", "--encoding"):
            encoding = arg

    if (not (inputfilename and outputfilename and encoding)):
        print(ERROR_PARAMS_MISSING)
        print(USAGE_PROGRAM)
        sys.exit(2)

    print("Inputfile:{} Outputfile:{} Encoding:{}".format(inputfilename,
                                                          outputfilename,
                                                          encoding))
    show_file(inputfilename, outputfilename, encoding)


def show_file(inputfilename, outputfilename, encoding):

    if os.path.exists(inputfilename):

        with io.open(inputfilename, encoding=encoding) as file:
            number_of_records = 0

            for num_line, line in enumerate(file):
                fields = line.split('\t')
                new_line = ""
                for field in fields:
                    if field == "":
                        field = "*"
                    if new_line:
                        new_line += '\t' + field
                    else:
                        new_line = field

                print('>' + new_line.replace('\t', '|').replace('\n', '!'))



def clean_string(self, dirty_string):
    return dirty_string.lstrip() \
                           .rstrip().replace('\n','!')


if __name__ == "__main__":
    main(sys.argv[1:])
