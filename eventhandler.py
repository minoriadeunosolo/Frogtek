import pyinotify
import os
import io
import json
import fcntl
from time import sleep


class DirectoryEventHandler(pyinotify.ProcessEvent):

    def __init__(self):
        self._Order_Cancelled = 0
        self._Order_Accepted = 0
        self._Order_Placed = 0
        self._Files = 0
        self._lastfilename = ""

    @property
    def Order_Cancelled(self):
        return self._Order_Cancelled

    @property
    def Order_Accepted(self):
        return self._Order_Accepted

    @property
    def Order_Placed(self):
        return self._Order_Placed

    @property
    def Files(self):
        return self._Files

    def process_IN_CREATE(self, event):
        #    print("\nbloqueado")
        i=2

    def process_IN_CLOSE_WRITE(self, event):
        if event.pathname.lower()[-5:] == ".json":
            try:
                self.process_file(event.pathname)
            except Exception as inst:
                print(type(inst))

    def process_file(self, filename):
        self._Files += 1
        temp = self._Order_Placed
        with io.open(filename, 'r', encoding='utf-8') as file_input:
            for num_line, line in enumerate(file_input):
                record = json.loads(line)
                type_order = record["Type"]
                if type_order == "OrderAccepted":
                    self._Order_Accepted += 1
                elif type_order == "OrderCancelled":
                    self._Order_Cancelled += 1
                elif type_order == "OrderPlaced":
                    self._Order_Placed += 1
        if temp == self._Order_Placed:
            print("Fichero sin contenido")
