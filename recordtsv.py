# -*- coding: utf-8 -*-

"""
    A class to model a well formed Record in data.tsv

    The idea is detect the type of a field by a regular expression.
    Only if 5 consecutive fields of  are correctly detected the record is
    created. Otherwise a initial or final garbage is created.

    
"""

import re

NEWLINE = '\n'
TAB = '\t'


class RecordTsvFrogTek:
    NUMBER_OF_FIELDS = 5
    NUM_FIELD_ID = 1
    NUM_FIELD_FIRST_NAME = 2
    NUM_FIELD_LAST_NAME = 3
    NUM_FIELD_ACCOUNT = 4
    NUM_FIELD_EMAIL = 5
    RE_ACCOUNT_NUMBER = '^[0-9]{2,4}[-]{0,1}[/]{0,1}[0-9]{2,4}$'
    RE_ID = '^[0-9]{1,5}$'  # It works until 99999 records
    RE_EMAIL = '^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$'

    def __init__(self, is_first_record=False):
        self.current_field = 0
        self.fields = list()
        self.is_first_record = is_first_record
        self.is_first_line = True
        self.separator_break_fields = NEWLINE
        self.sincronization_made = False
        self.initial_garbage = ""
        self.initial_garbage_count = 0
        self.final_garbage = ""
        self.final_garbage_count = 0

    def reset(self):
        self.current_field = 0
        self.fields.clear()
        self.is_first_record = False
        self.is_fist_line = False
        self.separator_break_fields = NEWLINE
        self.sincronization_made = True
        self.initial_garbage = ""
        self.initial_garbage_count = 0
        self.final_garbage = ""
        self.final_garbage_count = 0

    def add(self, str_field, current_separator):
        self.current_field += 1
        self.add_final_garbage(str_field)

        self.separator_break_fields = current_separator
        clean_field = self.clean_string(str_field)
        if self.is_first_record:
            self.add_field(str_field)
        else:
            if self.current_field == self.NUM_FIELD_ID:
                if re.match(self.RE_ID, clean_field):
                    self.add_field(str_field)
                else:
                    self.add_initial_garbage(str_field)
            elif self.current_field in (self.NUM_FIELD_FIRST_NAME,
                                        self.NUM_FIELD_LAST_NAME):
                if clean_field:
                    if ( re.match(self.RE_ACCOUNT_NUMBER, clean_field.lower())
                         or re.match(self.RE_ID, clean_field)
                         or re.match(self.RE_EMAIL, clean_field)):
                        self.add_initial_garbage(str_field)
                    else:
                        self.add_field(str_field)
                else:
                    self.add_field(str_field)  # empty names are allowed
            elif self.current_field == self.NUM_FIELD_ACCOUNT:
                # account number has to be numeric (or kind-of...)
                if re.match(self.RE_ACCOUNT_NUMBER, clean_field):
                    self.add_field(str_field)
                else:
                    # If it isn't the account number then it's part of the
                    # first_name/last_name fields
                    self.current_field -= 1
                    self.organize_names_fields(str_field)
            elif self.current_field == self.NUM_FIELD_EMAIL:
                # e-mail can't be empty
                if not clean_field:
                    self.organize_account_field(str_field)
                    self.current_field -= 1
                    #self.add_initial_garbage(str_field)
                else:
                    if re.match(self.RE_EMAIL, clean_field.lower()):
                        self.add_field(clean_field)
                    else:
                        self.add_initial_garbage(str_field)
            else:
                self.add_field(clean_field)
        self.separator_break_fields = TAB

    def add_initial_garbage(self, str):
        if self.sincronization_made:
            self.current_field -= 1
        else:
            self.current_field = 0
            for field in self.fields:
                self.initial_garbage += field
                self.initial_garbage_count += 1
            self.fields.clear()
            if self.initial_garbage_count:
                self.initial_garbage += self.separator_break_fields + str
            else:
                self.initial_garbage_count += 1
                self.initial_garbage = str

    def add_final_garbage(self, str):
        if self.final_garbage_count:
            self.final_garbage += self.separator_break_fields + str
        else:
            self.final_garbage_count += 1
            self.final_garbage = str

    def add_field(self, str):
        if self.separator_break_fields == TAB or len(self.fields) > 0:
            self.fields.append(self.separator_break_fields + str)
        else:
            self.fields.append(str)

    def get_initial_garbage(self):
        return self.initial_garbage

    def get_final_garbage(self):
        return self.final_garbage

    def iscomplete(self):
        return self.current_field == self.NUMBER_OF_FIELDS

    def ispartial(self):
        return (self.current_field > 0
                and self.current_field < self.NUMBER_OF_FIELDS)

    def organize_names_fields(self, str_field):
        local_separator = self.fields[2][0:1]
        if local_separator == TAB:
            self.fields[2] = (local_separator
                              + '"'
                              + self.fields[2][1:]
                              + self.separator_break_fields
                              + str_field + '"')
        else:
            local_separator = self.fields[1][0:1]
            if local_separator == TAB:
                self.fields[1] = (local_separator
                                  + '"'
                                  + self.fields[1][1:]
                                  + self.fields[2] + '"')
                self.fields[2] = (self.separator_break_fields
                                  + str_field)

    def organize_account_field(self, str_field):
        local_separator = self.fields[3][0:1]
        self.fields[3] = (local_separator
                          + '"'
                          + self.fields[3][1:]
                          + self.separator_break_fields
                          + '"')

    def generate_line(self):
        line = ""
        for field in self.fields:
            line = line + field

        return line + NEWLINE

    def clean_string(self, dirty_string):
        return dirty_string.lstrip() \
                           .rstrip()
