#!/usr/bin/env python

import sys
import glob
import time
sys.path.append('./gen-py/')
sys.path.insert(0, glob.glob('../lib/py/build/lib*')[0])

from proj_dir import ImageProcessing

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


# function to write information into log files 
def log(info):
  f = open("clientLog.txt", "a+")
  f.write(info)
  f.close()


def getServerAddress():
    machine = open('machine.txt', 'r')
    lines = machine.readlines()
    serverAddress = ""
    for line in lines:
        comp = line.split(" ") # extract first two items, name - address
        if ("server" in comp[0]):
            serverAddress = comp[1].rstrip()

    print(serverAddress)
    return serverAddress
  
def main():
    # Make socket for server machine IP address - TO DO - process machine.txt?
    # transport = TSocket.TSocket('kh4250-05.cselabs.umn.edu', 9090) - machine.txt
    serverAddress = getServerAddress()
    transport = TSocket.TSocket(serverAddress, 7090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = ImageProcessing.Client(protocol)

    # Connect!
    transport.open()

    print('Start image processing...')
    # pass the folder name which contains all images to be processed
    folderName = "data/input_dir/"
    elapsedTime = client.processImages(folderName)

    # get a result: a notification with the elapsed time for the job
    print("Job is DONE! The task %s is finished with elapsed time of %f" % (folderName, elapsedTime))
    log("Job is DONE! The task " + folderName + " is finished with elapsed time of " + str(elapsedTime) + "\n\n")
    # Close!
    transport.close()


if __name__ == '__main__':
    try:
        log("Starting client ..." + str(time.time()) + "\n")
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
