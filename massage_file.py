#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    1st Task of FrogTek's Technical Test.

    Transform (massage...) the given file to a standard TSV formatself.

    How to use it:

    massage_file.py -i data.tsv -o output.tsv -n 15

    Transform the file data.tsv to output.tsv using 15 parallel process

    The main idea is to cut the entire file in slices (=number of process)
    and process it in parallel, combining the results at the end

"""

import io
import os
import getopt
import sys

from multiprocessing import Pool
import math

from util_params import param_numeric_int
from paramprocess import InputParamProcess, OutParamProcess
from recordtsv import RecordTsvFrogTek, NEWLINE, TAB

USAGE_PROGRAM = ('USAGE: massage_file.py [-i|--inputfile] '
                 '<filename> [-o|--outputfile] <filename> '
                 '[-N|num_process <number of process>] [-v|--verbose]')

ENCODING_UTF16LE = 'utf-16le'
ENCODING_UTF8 = 'utf-8'
MSG_PARAMS_DETAIL = "Inputfile:{} Outputfile:{} Numprocess:{}"
ERROR_PARAMS_GENERAL = "Error in parameters!"
ERROR_PARAMS_MISSING = "Some missing parameters!"
ERROR_FILE_NOT_FOUND = "File not found {}"
ERROR_PARAM_NUMPROCESS = "Number of process must be a positive number"

ERROR_FIELD1_NAN = "Field number 1 is not numeric!:{}"

MSG_POSITION_LEN = "Process{}: Position:{} Length:{}"
MSG_TOTAL_BYTES = 'Total bytes {}'
MSG_JOIN = 'JOIN Proc:{} Proc:{} Result:{}'
MSG_STATISTICS = 'Lines Read:{} Records Written:{}\n'
MSG_DONE = 'DONE.'

ERROR_ABSOLUTE_INITIAL_GARBAGE = ("Error. File doesn't start with a "
                                  "complete record")
ERROR_ABSOLUTE_FINAL_GARBAGE = "Error. File doesn't end in a complete record"
ERROR_MIDDLE_GARBAGE = ("Error. There is a incomplete record after "
                        "join two adjacent incomplete records.")
PARTIAL_FILE = 'partial-{number:03d}.tsv'

def main(argv):
    """
        Get and check the command line arguments and start the process
    """
    inputfilename = ""
    outputfilename = ""
    num_process_str = ""
    verbose = False
    try:
        opts, args = getopt.getopt(argv, "i:o:n:v", ["inputfile=",
                                                     "outputfile=",
                                                     "num_process=",
                                                     "verbose"])
    except getopt.GetoptError:
        print(ERROR_PARAMS_GENERAL)
        print(USAGE_PROGRAM)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-i", "--inputfile"):
            inputfilename = arg
        if opt in ("-o", "--outputfile"):
            outputfilename = arg
        if opt in ("-n", "--num_process"):
            num_process_str = arg
        if opt in ("-v", "--verbose"):
            verbose = True

    if (not (inputfilename and outputfilename)):
        print(ERROR_PARAMS_MISSING)
        print(USAGE_PROGRAM)
        sys.exit(2)

    if param_numeric_int(num_process_str):
        if not num_process_str.isdigit:
            print(ERROR_PARAM_NUMPROCESS)
            sys.exit(2)
        else:
            numprocess = int(num_process_str)
    else:
        numprocess = 1

    if os.path.exists(inputfilename):
        total_length = os.path.getsize(inputfilename)//2
    else:
        print(ERROR_FILE_NOT_FOUND.format(inputfilename))

    print(MSG_PARAMS_DETAIL.format(inputfilename,
                                   outputfilename,
                                   numprocess))

    massage_file(inputfilename,
                 outputfilename, numprocess, total_length, verbose)


def massage_file(inputfilename,
                 outputfilename,
                 numprocess,
                 total_length,
                 verbose):

    """
            The file is cut in n slices and all of them are processed
            separately. At the end all results are combined.

            A slice returns a list with 3 fields:
                initial_garbage: characters found before a complete records
                final_garbage: characters found after the last complete records
                filename: The  file where the complete records are stored
                          (without the garbage)
    """
    params = list()
    tam_slice = math.ceil(total_length / numprocess)
    pending = total_length
    position = 0
    if verbose:
        print(MSG_TOTAL_BYTES.format(total_length))
    number_of_records_read = 0
    number_of_lines_read = 0
    for proc in range(0, numprocess):
        if pending < tam_slice:
            tam_slice = pending
        if verbose:
            print(MSG_POSITION_LEN.format(proc, position, tam_slice))
        params.append(InputParamProcess(process_id=proc,
                                        input_filename=inputfilename,
                                        position=position,
                                        length=tam_slice,
                                        is_initial=(proc == 0),
                                        is_final=(pending <= tam_slice)))
        pending -= tam_slice
        position += tam_slice*2  # UTF16 has double bytes than UTF8

    with Pool(processes=numprocess) as pool:
        output = pool.map(process_slice, params)

    with io.open(outputfilename, 'w+', encoding=ENCODING_UTF8) as file_output:
        for numproc, par_output in enumerate(output):
            number_of_records_read += par_output.number_of_records
            number_of_lines_read += par_output.number_of_lines

            if numproc == 0 and par_output.initial_garbage:  # 1st Slice
                print(ERROR_ABSOLUTE_INITIAL_GARBAGE)

            if numproc < numprocess-1:
                par_next = output[numproc+1]
                if not par_output.final_garbage and par_next.initial_garbage:
                    new_slice = (par_next.initial_garbage + NEWLINE)
                    append_content_to_file(par_output.filename,
                                           new_slice,
                                           skip_last_separator=True)
                append_file_to_output(file_output, par_output.filename)
                if par_output.final_garbage and not par_next.initial_garbage:
                    append_content_to_output(file_output,
                                             par_output.final_garbage)
                if par_output.final_garbage and par_next.initial_garbage:
                    new_slice = (par_output.final_garbage
                                 + par_next.initial_garbage)
                    new_proc_id = 9000 + par_output.process_id
                    new_param_slice = InputParamProcess(process_id=new_proc_id,
                                                        input_string=new_slice,
                                                        is_initial=False,
                                                        is_final=True)

                    str_output = process_slice_string(new_param_slice)
                    append_content_to_output(file_output,
                                             str_output.result_string)
                    if verbose:
                        print(MSG_JOIN.format(par_output.process_id,
                                              par_output.process_id+1,
                                              str_output.result_string))
                    number_of_records_read += str_output.number_of_records
                    if str_output.initial_garbage or str_output.final_garbage:
                        print(ERROR_MIDDLE_GARBAGE)
            elif numproc == numprocess-1:  # The final Slice
                append_file_to_output(file_output, par_output.filename)
                if par_output.final_garbage:
                    print(ERROR_ABSOLUTE_FINAL_GARBAGE)

            if not verbose:
                os.remove(par_output.filename)
    print(MSG_STATISTICS.format(number_of_lines_read, number_of_records_read))
    print(MSG_DONE)


def process_slice(input_param_process):
    """
        Given a slice (file, start_position and length) process the records and
        write them to a file. The inital and final garbage are returned to
        be combine later.

    """
    initial_search_needed = (input_param_process.position > 0)
    number_of_records = 0
    initial_garbage = ""
    final_garbage = ""
    outputfilename = PARTIAL_FILE.format(number=input_param_process.process_id)
    record = RecordTsvFrogTek(is_first_record=input_param_process.position == 0)

    with io.open(input_param_process.filename, encoding=ENCODING_UTF16LE) as file_input, \
         io.open(outputfilename, 'w+', encoding=ENCODING_UTF8) as file_output:
        file_input.seek(input_param_process.position)
        number_of_records = 0
        pending_length = input_param_process.length
        for num_line, line in enumerate(file_input):
            if pending_length <= len(line):
                line = line[:pending_length]
                pending_length -= len(line)
            else:
                pending_length -= len(line)
                if line[len(line)-1] == NEWLINE:
                    #  It's not the same reading file lines than split('\n')
                    line = line[:len(line)-1]
            current_separator = NEWLINE
            for field in line.split(TAB):
                record.add(field, current_separator)
                current_separator = TAB
                if record.iscomplete():
                    if initial_search_needed:
                        initial_search_needed = False
                        initial_garbage = record.get_initial_garbage()
                    number_of_records += 1
                    file_output.write(record.generate_line())
                    record.reset()
            if not pending_length:
                break

        if record.ispartial():
            final_garbage = record.get_final_garbage()

        return OutParamProcess(process_id=input_param_process.process_id,
                               initial_garbage=initial_garbage,
                               output_filename=outputfilename,
                               final_garbage=final_garbage,
                               number_of_records=number_of_records,
                               number_of_lines=num_line+1)


def process_slice_string(input_param_process):
    """
        Basically is the same than process_slice but read from a string.
        It was the first non-parallel version of the algorithmself. It
        is used to extract a entire record from the combination of
        the final_garbage and initial_garbage of two adjacents slices
    """
    initial_search_needed = not input_param_process.is_initial
    number_of_records = 0

    initial_garbage = ""
    final_garbage = ""
    result = ""

    record = RecordTsvFrogTek(is_first_record=input_param_process.is_initial)
    for num_line, line in enumerate(input_param_process.input_string.split(NEWLINE)):
        current_separator = NEWLINE
        for field in line.split(TAB):
            record.add(field, current_separator)
            current_separator = TAB
            if record.iscomplete():
                if initial_search_needed:
                    initial_search_needed = False
                    initial_garbage = record.get_initial_garbage()
                number_of_records += 1
                result += record.generate_line()
                record.reset()

    if record.ispartial():
        final_garbage = record.get_final_garbage()
    return OutParamProcess(process_id=input_param_process.process_id,
                           initial_garbage=initial_garbage,
                           result_string=result,
                           final_garbage=final_garbage,
                           number_of_records=number_of_records,
                           number_of_lines=num_line + 1)


def append_content_to_file(filename, content, skip_last_separator=False):
    """
        Auxiliary function. Append a string to the end of a file. Optionally
        can overwrite the final character. The file is opened and closed.

    """
    with io.open(filename, 'r+', encoding=ENCODING_UTF8) as file:
        if skip_last_separator:
            file.seek(0, os.SEEK_END)
            file.seek(file.tell()-1, os.SEEK_SET)
        file.write(content)


def append_content_to_output(file_output, content):
    """
        Auxiliary function. Append a string to the end of a file.
        The file has to be open before use it.

    """
    file_output.write(content)


def append_file_to_output(file_output, filename):
    """"
        Auxiliary function. Append the content of a file to the end of
        file_output
    """
    with io.open(filename, 'r', encoding=ENCODING_UTF8) as file:
        # file_output.write(file.read()) #loads entire file in memory!!!!
        for line in file:
            file_output.write(line)


def string_printable(str):
    """
        Auxiliary function. Just for debugging. It's a easy way to see
        non printable characters like \t and \n
    """
    return str.replace(TAB, '|').replace(NEWLINE, '!')

if __name__ == "__main__":
    main(sys.argv[1:])
