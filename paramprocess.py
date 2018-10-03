class InputParamProcess:
    def __init__(self,
                 process_id, input_filename="",
                 position=0, length=0,
                 is_initial=True, is_final=False,
                 input_string=""):
        self._process_id = process_id
        self._filename = input_filename
        self._position = position
        self._length = length
        self._is_initial = is_initial
        self._is_final = is_final
        self._input_string = input_string


    @property
    def process_id(self):
        return self._process_id

    @property
    def filename(self):
        return self._filename

    @property
    def input_string(self):
        return self._input_string

    @property
    def position(self):
        return self._position

    @property
    def length(self):
        return self._length

    @property
    def is_initial(self):
        return self._is_initial

    @property
    def is_final(self):
        return self._is_final





class OutParamProcess:
    def __init__(self,
                 process_id,
                 initial_garbage=0, output_filename="", final_garbage="",
                 result_string="",
                 number_of_records = 0,
                 number_of_lines = 0):
        self._process_id = process_id
        self._initial_garbage = initial_garbage
        self._filename = output_filename
        self._final_garbage = final_garbage
        self._result_string = result_string
        self._number_of_records = number_of_records
        self._number_of_lines = number_of_lines

    @property
    def process_id(self):
        return self._process_id

    @property
    def initial_garbage(self):
        return self._initial_garbage

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value):
        self._filename = value

    @property
    def final_garbage(self):
        return self._final_garbage

    @property
    def result_string(self):
        return self._result_string

    @result_string.setter
    def result_string(self, value):
        self._result_string = value

    @property
    def number_of_records(self):
        return self._number_of_records

    @property
    def number_of_lines(self):
        return self._number_of_lines
