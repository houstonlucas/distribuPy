from distribuPy import *
import math
import random

def getData(n, upperBound):
    data = []
    for i in range(n):
        data.append(random.random()*upperBound)
    return data

def main():
    ip = "127.0.01"
    port = 25565
    data = getData(500000, 5000.0)
    server = MergeSortServer(10, data, 5000)
    server.setup(ip, port)
    server.startAll()
    server.spin()
    # server.writeData()

class MergeSortServer(DistributedTaskManager):
    def __init__(self, tasksPerJob, data, itemsPerDivision):
        self.data = data
        self.sorted = []
        self.numItems = len(data)
        self.itemsPerDivision = itemsPerDivision
        self.numReplies = 0

        self.numJobIntegrators = 2
        self.jobsToPop = 2

        self.numDivs = int(math.ceil(float(self.numItems)/self.itemsPerDivision))
        print("numDivs: {}".format(self.numDivs))

        DistributedTaskManager.__init__(self, tasksPerJob)

    def writeData(self):
        f = open("out","w+")
        for val in self.sorted:
            f.write(str(val)+ "\n")
        f.close()

    def mergeOperation(self, toMerge):
        merged = []
        index1, index2 = 0,0
        l1 = len(self.sorted)
        l2 = len(toMerge)
        while(index1 < l1 and index2 < l2):
            x = self.sorted[index1]
            y = toMerge[index2]
            if(x<y):
                merged.append(x)
                index1 += 1
            else:
                merged.append(y)
                index2 += 1
        #sorted is out of new elements
        if(index1 == l1):
            merged += toMerge[index2:]
        else:
            merged += self.sorted[index1:]
        self.sorted = merged

    # Repetition is finished when all divisions have returned.
    def isRepetitionFinished(self,):
        return self.numReplies == self.numDivs

    # Simulation is finished after 1 repetition.
    def isSimulationFinished(self,):
        return self.repetitionsFinished >= 1

    # User defines how the given task is broken up, yielding tasks to be sent
    # to a client, yielding None if there are no more tasks to be given.
    def taskGenerator(self):
        # TODO was working here
        index = 0
        divNum = 0
        while index < self.numItems:
            upper = min(index + self.itemsPerDivision, self.numItems)
            yield (divNum, self.data[index:upper])
            divNum += 1
            index = upper

    #TODO Should this just be wrapped up inside the setNextRepetition function?
    # User defines what reseting the responses entails.
    # Note: This function is called at the begining of each repetition.
    def resetResponses(self):
        pass

    # User defines how to prepare for the next repetition.
    # Note: This function is called at the begining of each repetition.
    def setNextRepetition(self):
        pass

    # User defines how to record a response that comes back from a client.
    # this entails putting something into the jobBuffer for the recordJob
    # function to use at a later time.
    def recordResponse(self, task, response):
        with self.responseLock:
            self.jobBuffer.append( (task, response) )

    # User overwrites this function to define what it means to record a job,
    # these jobs are being pulled out of the jobBuffer which is populated by
    # the recordResponse function that the user overwrites.
    def recordJob(self, job):
        task, response = job
        taskID, taskData = task
        with self.responseLock:
            response = eval(response)
            self.mergeOperation(response)
            self.numReplies += 1


if __name__ == '__main__':
    main()