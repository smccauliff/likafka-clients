import numpy as np
import scipy as sp
from scipy import signal
from matplotlib import pyplot as plt
import sys

def readData(fname) :
    benchmark = np.genfromtxt(fname, delimiter=',')
    benchmark[:,2] = sp.signal.medfilt(benchmark[:,2]) * 1000 # and convert to microseconds
    return benchmark
stringLengthPrefix20 = readData(sys.argv[1])
stringLengthPrefix50 = readData(sys.argv[2])
stringLengthPrefixHashMap20 = readData(sys.argv[3])
stringLengthPrefixHashMap50 = readData(sys.argv[4])

plt.plot(stringLengthPrefix20[:,1], stringLengthPrefix20[:,2], '-',
         stringLengthPrefix50[:,1], stringLengthPrefix50[:,2], '.',
         stringLengthPrefixHashMap20[:,1], stringLengthPrefixHashMap20[:,2], '--',
         stringLengthPrefixHashMap50[:,1], stringLengthPrefixHashMap50[:,2], '-.')
plt.legend(['20% prefix, List', '50% prefix, List', '20% prefix, HashMap', '50% prefix, HashMap'], loc='lower right')
plt.title('Effects of string length on header parsing nHeaders=20')
plt.ylabel('micros')
plt.xlabel('header key length')
plt.show()