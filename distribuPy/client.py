#!/usr/bin/python
import socket
import numpy as np
import time
import traceback


###TODO LONG-TERM
    # Implement some type of confirmation for code being received.
    # handle the delimiting of responses for user?
    # Implement workload settings
    # Create CLI/GUI interface (connect/disconnect/change workload settings)

def main():

    dtc = DistributedTaskClient()
    ip = "127.0.0.1"
    port = 25565 #picked this at random
    dtc.setup(ip, port)
    print("Setup complete")
    dtc.run()

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
         
        print("Connected to server (%s)"%str(self.clientSock.getpeername()))

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
        exec(code)
        names = eval(names)
        for name in names:
            self.__dict__[name] = eval(name)

    def receiveLargeMessage(self, sock):
        #get size of message to be received
        val = sock.recv(self.MIN_MESSAGE_SIZE)
        if val == "Close":
            return val
        incomingSize = int(val)
        sock.send("CONFIRMED")
        responses = ""
        bytesReceived = 0
        #receive message
        while(bytesReceived < incomingSize):
            bytesRemaing = incomingSize - bytesReceived
            if bytesRemaing > 1024:
                recvSize = 1024
            else:
                recvSize = bytesRemaing
            msgPart = sock.recv(recvSize)
            bytesReceived += len(msgPart)
            responses += msgPart
        if(responses == ''):
            raise Exception
        return responses

    def sendLargeMessage(self, sock, msg):
        sock.send(str(len(msg)).zfill(10))
        sock.recv(10)
        sock.send(msg)

    def receiveTaskInstructions(self, sock):
        #get instructions
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

if __name__ == '__main__':
    main()