import threading
import socket
import time
import sys
import traceback


# Server Objects #

# TODO Server Side
# Implement adding/removing servers while running.
# Build CLI framework.
# Build GUI framework.

class DistributedTaskManager:
    def __init__(self, tasks_per_job=20):
        self.job_buffer = []
        self.drop_buffer = []
        self.servers = []

        self.client_labels = []
        self.client_code = ''

        self.stop = False
        self.verbose = True

        self.tasks_per_job = tasks_per_job
        self.connection_backlog_max = 5
        self.connected = 0
        self.connected_lock = threading.Lock()

        # Larger messages will be received in chunks of this size.
        self.bytesPerReceive = 1024
        # Number of bytes to use in smaller messages.
        self.small_message_size = 10
        # How long between attempts to establish a server binding.
        self.retry_wait_time = 5
        # Number of seconds until server times out on connection.
        self.server_timeout = 2.0
        # How many seconds the simulationManagerThread sleeps between pollings.
        self.manager_sleep_time = 0.1

        # Name of a repetition. For example if this were set to 'Frame' the
        # debug log might contain something like: Frame 42 finished in 3.14159 seconds.
        self.repetition_name = "Repetition"

        # The number of jobIntegration threads to be running.
        self.num_job_integrators = 8
        # The number of jobs that each jobIntegration thread will work with at
        # any given time.
        # TODO Find better name for this variable
        self.jobsToPop = 10

        self.repetitions_finished = 0

        self.responseLock = threading.Lock()
        self.reset_responses()

        self.task_gen = self.task_generator()
        self.task_gen_lock = threading.Lock()

        self.simThread = threading.Thread(target=self.simulation_management_thread)

        # Reads the ClientCode file.
        self.read_in_client_code()

    # Adds a socket to the given ip and port. Trying again every retryWaitTime
    # seconds until a successful binding happens.
    def persist_setup(self, ip, port):
        connected = False
        self.servers.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        while not connected:
            try:
                self.servers[-1].bind((ip, port))
                connected = True
            except:
                self.log("Failed to establish, trying again...")
                time.sleep(self.retry_wait_time)
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
    def start_all(self, ):
        for server in self.servers:
            thread = threading.Thread(target=self.start, args=(server,))
            thread.start()

    # Stars the given server, also starts the simThread if it wasn't
    # already alive.
    def start(self, server):
        if not self.simThread.isAlive():
            self.simThread.start()

        server.listen(self.connection_backlog_max)
        server.settimeout(self.server_timeout)

        # Until told to stop listen for connections and start a new Thread to
        # handle that connection.
        while not self.stop:
            try:
                sock, port = server.accept()
                t = threading.Thread(target=self.client_communication_thread, args=(sock,))
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
    def spin(self, sleep_time=1.0):
        try:
            while not self.stop:
                time.sleep(sleep_time)
        except:
            self.log("Closing down...")
            self.stop = True
            return False
        return True

    def simulation_management_thread(self, ):
        self.create_job_integration_threads()

        time.sleep(0.1)
        # repeat until all repetitions have been processed
        while not self.stop and not self.is_simulation_finished():
            start_time = time.time()
            self.set_next_rep()

            # TODO I don't think both loops are needed. Merge them?
            # repeat until current repetition is finished
            while not self.stop and not self.is_repetition_finished():
                time.sleep(self.manager_sleep_time)  # pull me off the processor so others can work

            # wait until jobBuffer is empty
            while not self.stop and len(self.job_buffer) != 0:
                time.sleep(self.manager_sleep_time)

            dur = time.time() - start_time
            self.log("{} {} finished in {} seconds".format(self.repetition_name, self.repetitions_finished, dur))

            if not self.stop:
                self.repetitions_finished += 1

        self.stop = True
        self.close_servers()
        self.log("Ending simulationManagementThread")

    # Closes all server connections.
    def close_servers(self):
        for server in self.servers:
            try:
                server.close()
            except Exception as err:
                sock_name = server.getsockname()
                self.log("There was an error closing server: {}".format(sock_name))
                self.log(err)

    # Creates all the jobIntegration threads.
    def create_job_integration_threads(self):
        for i in range(self.num_job_integrators):
            job_thread = threading.Thread(target=self.job_integration_thread)
            job_thread.start()

    # This thread pops up to 'jobsToPop' synchronously from the jobBuffer.
    # After it has a set of jobs to integrate it releases the lock
    def job_integration_thread(self):
        while not self.stop:
            jobs = []
            jobs_popped = 0

            if len(self.job_buffer) != 0:
                with self.responseLock:
                    while jobs_popped < self.jobsToPop and len(self.job_buffer) != 0:
                        jobs.append(self.job_buffer.pop())
                        jobs_popped += 1

            for job in jobs:
                self.record_job(job)

                # Calls user defined setNextRepetition and resetResponses, then resets the
                # taskGenerator
                # TODO should this exist or should the user be charged with this in the
                # set nextRepetition function?

    def set_next_rep(self):
        self.set_next_repetition()
        self.reset_responses()
        self.task_gen = self.task_generator()

    # Updates connected count asynchronously.
    def change_connected_count(self, diff):
        with self.connected_lock:
            self.connected += diff

    # Sends client instructions to the client.
    def setup_client(self, sock):
        # TODO do the labels really need to be there?
        client_instructions = str([self.client_labels, self.client_code])
        # Send size of code
        sock.send(str(len(client_instructions)).zfill(self.small_message_size))
        sock.recv(self.small_message_size)
        # Send instructions
        sock.send(client_instructions)
        sock.recv(self.small_message_size)

    # Reads the instructions for the client and send them to the 
    def read_in_client_code(self):
        f = open("ClientCode.py", "r")
        client_code_lines = f.readlines()
        self.client_labels = client_code_lines[0]
        # line 1 is the delimiter
        self.client_code = ''.join(client_code_lines[2:])

    # Thread that handles distributing jobs to its connection
    def client_communication_thread(self, sock):
        self.change_connected_count(1)

        # Setup the client
        try:
            self.setup_client(sock)
        except socket.error, err:
            self.log("[ERROR] {}\n".format(err.message))
            sock.close()
            return

        # Issue jobs to client
        while not self.stop:
            # Package up tasks into jobs
            to_client, tasks_in_job = self.get_packaged_job()
            if to_client == "":
                continue

            # Send, receive, and handle jobs
            try:
                send_large_message(sock, to_client, self.small_message_size)
                responses = receive_large_message(sock, self.small_message_size)
                self.handle_responses(tasks_in_job, responses)
            except Exception as err:
                self.log(err)
                # Clean up remainder of job
                peer_name = str(sock.getpeername())
                self.log("{} has dropped!".format(peer_name))
                self.log("Dropped jobs will be added to the drop buffer.")
                if tasks_in_job != [None]:
                    for task in tasks_in_job:
                        self.drop_buffer.append(task)
                break
        try:
            sock.send("Close")
            sock.close()
        except:
            pass
        self.change_connected_count(-1)
        self.log("Ending clientCommunicationThread")

        # Pulls the next task from the user-defined taskGenerator.
        # After all original tasks are used, this pulls from the dropBuffer
        # until the dropBuffer is empty at which point the function returns
        # None, signaling the receiving clientCommunicationThread to wait.

    def get_next_task(self):
        with self.task_gen_lock:
            task = next(self.task_gen, None)
            if task is None and len(self.drop_buffer) != 0:
                return self.drop_buffer.pop()
            else:
                return task

    # Returns a package with multiple tasks as a string.
    # The structure of a package is task descriptions seperated by underscores.
    # TODO should this be pushed on the user to define?
    def get_packaged_job(self, ):
        to_client = ""
        tasks_in_job = []
        for i in range(self.tasks_per_job):
            task = self.get_next_task()
            if task is not None:
                tasks_in_job.append(task)
                # TODO the underscore should be some generic delimiter that the
                # user can set. If this is changed then also send the delimiter
                # across to the clients so they can properly parse messages.
                to_client += "_" + str(task[1])
        return to_client[1:], tasks_in_job

    # Records the tasks and corresponding responses by placing them into the jobBuffer.
    def handle_responses(self, tasks, responses):
        # loop through a split up responses and place the peices into the jobBuffer
        split_responses = responses.split("_")
        for index, response in enumerate(split_responses):
            self.record_response(tasks[index], response)

    # TODO replace with https://docs.python.org/2/library/logging.html
    def log(self, msg):
        if self.verbose:
            print(msg)

            ######################################################
            #    User must overwrite the following functions.    #
            ######################################################

    # User defines when a repetition is finished.
    def is_repetition_finished(self, ):
        raise NotImplementedError

    # User defines when the simulation is finished.
    def is_simulation_finished(self, ):
        raise NotImplementedError

    # User defines how the given task is broken up, yielding tasks to be sent
    # to a client, yielding None if there are no more tasks to be given.
    def task_generator(self):
        raise NotImplementedError

    # TODO Should this just be wrapped up inside the setNextRepetition function?
    # User defines what reseting the responses entails.
    # Note: This function is called at the begining of each repetition.
    def reset_responses(self):
        raise NotImplementedError

    # User defines how to prepare for the next repetition.
    # Note: This function is called at the begining of each repetition.
    def set_next_repetition(self):
        raise NotImplementedError

    # User defines how to record a response that comes back from a client.
    # this entails putting something into the jobBuffer for the recordJob
    # function to use at a later time.
    def record_response(self, task, response):
        raise NotImplementedError

    # User overwrites this function to define what it means to record a job,
    # these jobs are being pulled out of the jobBuffer which is populated by
    # the recordResponse function that the user overwrites.
    def record_job(self, job):
        raise NotImplementedError


# Client Objects #

# TODO Client-side
# Implement some type of confirmation for code being received.
# handle the delimiting of responses for user?
# Implement workload settings
# Create CLI/GUI interface (connect/disconnect/change workload settings)


class DistributedTaskClient:
    def __init__(self):
        self.clientSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientTask = ClientTask()

    def setup(self, ip, port):
        try:
            self.clientSock.connect((ip, port))
        except:
            print("COULD NOT CONNECT TO SERVER.\nExiting...")
            exit()

        print("Connected to server (%s)" % str(self.clientSock.getpeername()))

        self.clientTask.receive_task_instructions(self.clientSock)
        self.clientTask.interpret_task_instructions()

    def run(self):
        self.clientTask.run(self.clientSock)


class ClientTask:
    def __init__(self):
        self.small_message_size = 10
        self.clientSetupStr = ""

    def interpret_task_instructions(self):
        instructions = eval(self.clientSetupStr)
        names = instructions[0]
        code = instructions[1]
        exec(code)
        names = eval(names)
        for name in names:
            self.__dict__[name] = eval(name)

    def receive_task_instructions(self, sock):
        # get instructions
        self.clientSetupStr = receive_large_message(sock, self.small_message_size)
        sock.send("CONFIRMED")

    def run(self, sock):
        while 1:
            try:
                msg = receive_large_message(sock, self.small_message_size)
                if msg != "Close":
                    # TODO: This should almost certainly be made an abstract function
                    ans = self.task(self, msg)
                    send_large_message(sock, ans, self.small_message_size)
                else:
                    # TODO change this to a log message
                    print("Received close, disconnecting...")
                    break
            except:
                traceback.print_exc()
                # TODO change this to a log message
                print("Error encountered, exiting...")
                break
        sock.close()


# Helper Functions #

def send_large_message(sock, msg, min_message_size):
    sock.send(str(len(msg)).zfill(min_message_size))
    sock.recv(min_message_size)
    sock.send(msg)


# Receives a message from a socket. First receiving the size of the message
# to be received, looping to receive the entire message in chunks of at
# most recv_size number of bytes.

def receive_large_message(sock, min_message_size):
    # get size of message to be received
    val = sock.recv(min_message_size)
    if val == "Close":
        return val
    incoming_size = int(val)
    sock.send("CONFIRMED")
    responses = ""
    bytes_received = 0
    # receive message
    while bytes_received < incoming_size:
        bytes_remaining = incoming_size - bytes_received
        if bytes_remaining > 1024:
            recv_size = 1024
        else:
            recv_size = bytes_remaining
        msg_part = sock.recv(recv_size)
        bytes_received += len(msg_part)
        responses += msg_part
    if responses == '':
        raise Exception
    return responses
