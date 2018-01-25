import threading
import socket
import time
import sys
import traceback


### Server Objects ###

# TODO Server Side
# Implement adding/removing servers while running.
# Build CLI framework.
# Build GUI framework.

class DistributedTaskManager:
    def __init__(self, tasksPerJob=20):
        self.jobBuffer = []
        self.dropBuffer = []
        self.servers = []

        self.stop = False
        self.verbose = True

        self.tasksPerJob = tasksPerJob
        self.connectionBacklogMax = 5
        self.connected = 0
        self.connectedLock = threading.Lock()

        # Larger messages will be received in chunks of this size.
        self.bytesPerReceive = 1024
        # Number of bytes to use in smaller messages.
        self.smallMessageSize = 10
        # How long between attempts to establish a server binding.
        self.retryWaitTime = 5
        # Number of seconds until server times out on connection.
        self.serverTimeOut = 2.0
        # How many seconds the simulationManagerThread sleeps between pollings.
        self.managerSleepTime = 0.1

        # Name of a repetition. For example if this were set to 'Frame' the
        # debug log might contain something like: Frame 42 finished in 3.14159 seconds.
        self.repetitionName = "Repetition"

        # The number of jobIntegration threads to be running.
        self.numJobIntegrators = 8
        # The number of jobs that each jobIntegration thread will work with at
        # any given time.
        # TODO Find better name for this variable
        self.jobsToPop = 10

        self.repetitionsFinished = 0

        self.responseLock = threading.Lock()
        self.resetResponses()

        self.taskGen = self.taskGenerator()
        self.taskGenLock = threading.Lock()

        self.simThread = threading.Thread(target=self.simulationManagementThread)

        # Reads the ClientCode file.
        self.readInClientCode()

    # Adds a socket to the given ip and port. Trying again every retryWaitTime
    # seconds until a successful binding happens.
    def persistSetup(self, ip, port):
        connected = False
        self.servers.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        while not connected:
            try:
                self.servers[-1].bind((ip, port))
                connected = True
            except:
                self.log("Failed to establish, trying again...")
                time.sleep(self.retryWaitTime)
        self.log("Server setup on {}".format(ip))

    # Adds a socket to the given ip and port. Exiting on failure to bind.
    def setup(self, ip, port):
        try:
            self.servers.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            self.servers[-1].bind((ip, port))
        except:
            self.log("Could not establish server on {}".format(ip))
            self.stop = True
            # TODO maybe instead return bad exit code.
            sys.exit(1)
        self.log("Server setup on {}".format(ip))

    # Starts all servers in the servers list.
    def startAll(self, ):
        for server in self.servers:
            thread = threading.Thread(target=self.start, args=(server,))
            thread.start()

    # Stars the given server, also starts the simThread if it wasn't
    # already alive.
    def start(self, server):
        if not self.simThread.isAlive():
            self.simThread.start()

        server.listen(self.connectionBacklogMax)
        server.settimeout(self.serverTimeOut)

        # Until told to stop listen for connections and start a new Thread to
        # handle that connection.
        while not self.stop:
            try:
                sock, port = server.accept()
                t = threading.Thread(target=self.clientCommunicationThread, args=(sock,))
                t.start()
            except socket.timeout:
                continue
            except:
                self.stop = True
                break
            self.log("CONNECTED TO: %s" % str(port))

        try:
            server.close()
        except:
            pass
        self.stop = True
        self.log("EXITING MAIN")

    # Wait until the other threads have stopped. Returns true if everything ran
    # correctly, false otherwise.
    def spin(self, sleepTime=1.0):
        try:
            while not self.stop:
                time.sleep(sleepTime)
        except:
            self.log("Closing down...")
            self.stop = True
            return False
        return True

    def simulationManagementThread(self, ):
        self.createJobIntegrationThreads()

        time.sleep(0.1)
        # repeat until all repetitions have been processed
        while (not self.stop and not self.isSimulationFinished()):
            startTime = time.time()
            self.setNextRep()

            # TODO I don't think both loops are needed. Merge them?
            # repeat until current repetition is finished
            while not self.stop and not self.isRepetitionFinished():
                time.sleep(self.managerSleepTime)  # pull me off the processor so others can work

            # wait until jobBuffer is empty
            while not self.stop and len(self.jobBuffer) != 0:
                time.sleep(self.managerSleepTime)

            dur = time.time() - startTime
            self.log("{} {} finished in {} seconds".format(self.repetitionName, self.repetitionsFinished, dur))

            if not self.stop:
                self.repetitionsFinished += 1

        self.stop = True
        self.closeServers()
        self.log("Ending simulationManagementThread")

    # Closes all server connections.
    def closeServers(self):
        for server in self.servers:
            try:
                server.close()
            except Exception as err:
                sockName = sock.getsockname()
                self.log("There was an error closing server: {}".format(sockName))
                self.log(err)

    # Creates all the jobIntegration threads.
    def createJobIntegrationThreads(self):
        for i in range(self.numJobIntegrators):
            jobThread = threading.Thread(target=self.jobIntegrationThread)
            jobThread.start()

    # This thread pops up to 'jobsToPop' synchronously from the jobBuffer.
    # After it has a set of jobs to integrate it releases the lock
    def jobIntegrationThread(self):
        while not self.stop:
            jobs = []
            jobsPopped = 0

            if (len(self.jobBuffer) != 0):
                with self.responseLock:
                    while (jobsPopped < self.jobsToPop and len(self.jobBuffer) != 0):
                        jobs.append(self.jobBuffer.pop())
                        jobsPopped += 1

            for job in jobs:
                self.recordJob(job)

                # Calls user defined setNextRepetition and resetResponses, then resets the
                # taskGenerator
                # TODO should this exist or should the user be charged with this in the
                # set nextRepetition function?

    def setNextRep(self):
        self.setNextRepetition()
        self.resetResponses()
        self.taskGen = self.taskGenerator()

    # Updates connected count asynchronously.
    def changeConnectedCount(self, diff):
        with self.connectedLock:
            self.connected += diff

    # Sends client instructions to the client.
    def setupClient(self, sock):
        # TODO do the labels really need to be there?
        clientInstructions = str([self.clientLabels, self.clientCode])
        # Send size of code
        sock.send(str(len(clientInstructions)).zfill(self.smallMessageSize))
        confirm = sock.recv(self.smallMessageSize)
        # Send instructions
        sock.send(clientInstructions)
        confirm = sock.recv(self.smallMessageSize)

    # Reads the instructions for the client and send them to the 
    def readInClientCode(self):
        f = open("ClientCode.py", "r")
        clientCodelines = f.readlines()
        self.clientLabels = clientCodelines[0]
        self.clientCode = ''.join(clientCodelines[2:])

    # Thread that handles distributing jobs to its connection
    def clientCommunicationThread(self, sock):
        self.changeConnectedCount(1)

        # Setup the client
        try:
            self.setupClient(sock)
        except socket.error, err:
            self.log("[ERROR] {}\n".format(err[1]))
            sock.close()
            return

        # Issue jobs to client
        while not self.stop:
            # Package up tasks into jobs
            toClient, tasksInJob = self.getPackagedJob()
            if toClient == "":
                continue

            # Send, receive, and handle jobs
            try:
                self.sendLargeMessage(sock, toClient)
                responses = self.receiveLargeMessage(sock)
                self.handleResponses(tasksInJob, responses)
            except Exception as err:
                self.log(err)
                # Clean up remainder of job
                peerName = str(sock.getpeername())
                self.log("{} has dropped!".format(peerName))
                self.log("Dropped jobs will be added to the drop buffer.")
                if (tasksInJob != [None]):
                    for task in tasksInJob:
                        self.dropBuffer.append(task)
                break
        try:
            sock.send("Close")
            sock.close()
        except:
            pass
        self.changeConnectedCount(-1)
        self.log("Ending clientCommunicationThread")

        # Pulls the next task from the user-defined taskGenerator.
        # After all original tasks are used, this pulls from the dropBuffer
        # until the dropBuffer is empty at which point the function returns
        # None, signaling the receiving clientCommunicationThread to wait.

    def getNextTask(self):
        with self.taskGenLock:
            task = next(self.taskGen, None)
            if task == None and len(self.dropBuffer) != 0:
                return self.dropBuffer.pop()
            else:
                return task

    # Returns a package with multiple tasks as a string.
    # The structure of a package is task descriptions seperated by underscores.
    # TODO should this be pushed on the user to define?
    def getPackagedJob(self, ):
        toClient = ""
        tasksInJob = []
        for i in range(self.tasksPerJob):
            task = self.getNextTask()
            if task != None:
                tasksInJob.append(task)
                # TODO the underscore should be some generic delimiter that the
                # user can set. If this is changed then also send the delimiter
                # across to the clients so they can properly parse messages.
                toClient += "_" + str(task[1])
        return toClient[1:], tasksInJob

    # Records the tasks and corresponding responses by placing them into the jobBuffer.
    def handleResponses(self, tasks, responses):
        # loop through a split up responses and place the peices into the jobBuffer
        split_responses = responses.split("_")
        for index, response in enumerate(split_responses):
            self.recordResponse(tasks[index], response)

    # Receives a message from a client. First receiving the size of the message
    # to be received, looping to receive the entire message in chunks of at
    # most bytesPerReceive number of bytes.
    def receiveLargeMessage(self, sock):
        incomingSize = int(sock.recv(self.smallMessageSize))
        sock.send("CONFIRMED")
        responses = ""
        bytesReceived = 0
        while (bytesReceived < incomingSize):
            bytesRemaing = incomingSize - bytesReceived
            if bytesRemaing > self.bytesPerReceive:
                recvSize = self.bytesPerReceive
            else:
                recvSize = bytesRemaing
            msgPart = sock.recv(recvSize)
            bytesReceived += len(msgPart)
            responses += msgPart
        if (responses == ''):
            raise Exception
        return responses

    # First sends the size of the message that will be sent, then receives a
    # a confirmation (mainly used for synchronizing this communication)
    def sendLargeMessage(self, sock, msg):
        sock.send(str(len(msg)).zfill(self.smallMessageSize))
        sock.recv(self.smallMessageSize)
        sock.send(msg)

    # TODO replace with https://docs.python.org/2/library/logging.html
    def log(self, msg):
        if self.verbose:
            print(msg)

    ######################################################
    #### User must overwrite the following functions. ####
    ######################################################

    # User defines when a repetition is finished.
    def isRepetitionFinished(self, ):
        raise NotImplementedError

    # User defines when the simulation is finished.
    def isSimulationFinished(self, ):
        raise NotImplementedError

    # User defines how the given task is broken up, yielding tasks to be sent
    # to a client, yielding None if there are no more tasks to be given.
    def taskGenerator(self):
        raise NotImplementedError

    # TODO Should this just be wrapped up inside the setNextRepetition function?
    # User defines what reseting the responses entails.
    # Note: This function is called at the begining of each repetition.
    def resetResponses(self):
        raise NotImplementedError

    # User defines how to prepare for the next repetition.
    # Note: This function is called at the begining of each repetition.
    def setNextRepetition(self):
        raise NotImplementedError

    # User defines how to record a response that comes back from a client.
    # this entails putting something into the jobBuffer for the recordJob
    # function to use at a later time.
    def recordResponse(self, task, response):
        raise NotImplementedError

    # User overwrites this function to define what it means to record a job,
    # these jobs are being pulled out of the jobBuffer which is populated by
    # the recordResponse function that the user overwrites.
    def recordJob(self):
        raise NotImplementedError

    ### Client Objects ###

    ###TODO Client-side
    # Implement some type of confirmation for code being received.
    # handle the delimiting of responses for user?
    # Implement workload settings
    # Create CLI/GUI interface (connect/disconnect/change workload settings)


class DistributedTaskClient:
    def __init__(self):
        self.clientTask = ClientTask()

    def setup(self, ip, port):
        self.clientSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.clientSock.connect((ip, port))
        except:
            print("COULD NOT CONNECT TO SERVER.\nExiting...")
            exit()

        print("Connected to server (%s)" % str(self.clientSock.getpeername()))

        self.clientTask.receiveTaskInstructions(self.clientSock)
        self.clientTask.interpretTaskInstructions()

    def run(self):
        self.clientTask.run(self.clientSock)


class ClientTask:
    def __init__(self):
        self.MIN_MESSAGE_SIZE = 10

    def interpretTaskInstructions(self):
        instructions = eval(self.clientSetupStr)
        names = instructions[0]
        code = instructions[1]
        exec (code)
        names = eval(names)
        for name in names:
            self.__dict__[name] = eval(name)

    def receiveLargeMessage(self, sock):
        # get size of message to be received
        val = sock.recv(self.MIN_MESSAGE_SIZE)
        if val == "Close":
            return val
        incomingSize = int(val)
        sock.send("CONFIRMED")
        responses = ""
        bytesReceived = 0
        # receive message
        while (bytesReceived < incomingSize):
            bytesRemaing = incomingSize - bytesReceived
            if bytesRemaing > 1024:
                recvSize = 1024
            else:
                recvSize = bytesRemaing
            msgPart = sock.recv(recvSize)
            bytesReceived += len(msgPart)
            responses += msgPart
        if (responses == ''):
            raise Exception
        return responses

    def sendLargeMessage(self, sock, msg):
        sock.send(str(len(msg)).zfill(10))
        sock.recv(10)
        sock.send(msg)

    def receiveTaskInstructions(self, sock):
        # get instructions
        self.clientSetupStr = self.receiveLargeMessage(sock)
        sock.send("CONFIRMED")

    def run(self, sock):
        while 1:
            try:
                msg = self.receiveLargeMessage(sock)
                if msg != "Close":
                    ans = self.task(self, msg)
                    self.sendLargeMessage(sock, ans)
                else:
                    # TODO change this to a log message
                    print("Received close, disconnecting...")
                    break
            except Exception as e:
                traceback.print_exc()
                # TODO change this to a log message
                print("Error encountered, exiting...")
                break
        sock.close()
