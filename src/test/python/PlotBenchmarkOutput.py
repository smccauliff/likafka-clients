import numpy as np


from matplotlib import pyplot as plt
import sys

def parseResultsFile(fileName) :
    startOfData = False
    numPyMatrix = None
    allTests = []
    name = ""
    with open(fileName,'r') as f :
        for line in f :
            if line.startswith("Warmup") :
                startOfData = 1
                continue
            if not startOfData :
                continue
            parts = line.split(',')
            if len(parts) != 2 :
                if not numPyMatrix is None :
                    allTests.append(name)
                    allTests.append(numPyMatrix)
                name = line
                numPyMatrix = None
                continue
            itemCount = int(parts[0])
            timeMs = float(parts[1])
            #print("itemCount " + str(itemCount) + " timeMs" + str(timeMs))
            if numPyMatrix is None :
                numPyMatrix = np.array([itemCount, timeMs]).astype('float')
            else :
                numPyMatrix = np.vstack([numPyMatrix, [itemCount, timeMs]])
            print(numPyMatrix)
        allTests.append(name)
        allTests.append(numPyMatrix)
    return allTests

allTests = parseResultsFile(sys.argv[1])

lineStyles = ['-', '--', '.', '-.', ':', ':.']
from itertools import cycle
lineCycler = cycle(lineStyles)
plotArgs = []
names = []
for i in range(0, len(allTests)) :
    if (i % 2) == 0 :
        names.append(allTests[i])
    else :
        test = allTests[i]
        test[:,1] = test[:, 1] * 1000 # scale to microseconds
        plt.plot(test[:,0], test[:,1], next(lineCycler))
        # plotArgs.extend([itemCount, allTests[i][:,1], ])

plt.legend(names, loc="lower right", fontsize=9, ncol=3)
plt.ylabel(sys.argv[2])
plt.xlabel('number of headers')
plt.gca().set_yscale('log', basey=2)
plt.show()


