
import io
import os
import getopt
import sys
import re
from multiprocessing import Pool
import math


class parametro:
  def __init__(self, numproc, file):
    self.numproc = numproc
    self.filename = file
    self.result = ""


def process_slice(p):
    with io.open(p.filename) as file:
        file.seek(p.numproc)
        p.result = file.read(10)

    return p


filename = 'output10.tsv'
numprocess = 10
params = list()
for numproc in range(0, numprocess):
    params.append(parametro(numproc, filename))

with Pool(processes=numprocess) as pool:
    output = pool.map(process_slice, params)

for o in output:
    print('Proceso:{} Reusltado:{}'.format(o.numproc, o.result))
