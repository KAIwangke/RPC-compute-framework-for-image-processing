import glob
import sys
import time
from email.mime import image
from locale import currency
import cv2
import numpy as np
import os
import random

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../lib/py/build/lib*')[0])

from proj_dir import computeNode

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


# GLOBAL VARIABLES
loadProb = 0
port = 0
policy = ""
currNodeName = ""

# function to write information into log files 
def log(info):
  f = open("computeNodeLog.txt", "a+")
  f.write(info)
  f.close()

# process config.txt to get the scheduling polivy and load probability of each node
def processConfig():
  config = open('config.txt', 'r')
  lines = config.readlines()

  for line in lines:
    line = line.replace(" ","")
    comp = line.split(':') # len 2 list
    if (comp[0] == "policy"):
      global policy
      policy = comp[1].rstrip()
    elif (comp[0] == currNodeName): # node information, save in the nodes dictionary
      global loadProb
      loadProb = float(comp[1])
  
  print(policy)
  print(loadProb)

# in order to get server address
def getServer():
  server = ""
  machine = open('machine.txt', 'r')
  lines = machine.readlines()
  for line in lines:
    comp = line.split(" ") # extract first two items, name - address
    if (comp[0] == currNodeName):
      server = comp[1].rstrip()

  return server

# actual function which process the image and save the result in the output_dir
def imageProcessTime(fileName, delay):
  if (delay):
    time.sleep(3)
  current = fileName
  pathoutput = "data/output_dir"
  img = cv2.imread(current) 
  img_blur = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
  img_blur = cv2.GaussianBlur(img_blur, (3,3), 0) 
  # thrs1 = cv2.getTrackbarPos('thrs1', 'edge')
  # thrs2 = cv2.getTrackbarPos('thrs2', 'edge')
  edge = cv2.Canny(img_blur, 300, 300, apertureSize=5)
  vis = img.copy()
  vis = np.uint8(vis/2.)
  vis[edge != 0] = (0, 255, 0)

  filename = os.path.basename(current)
  completeName = os.path.join(pathoutput, filename)
  cv2.imwrite(completeName, vis)


# inject depays based on the load probability
def injectDelay():
  num = random.randint(1, 10)
  print(num)
  if (num >= 1 and num <= loadProb*10):
    print("sleep for 3 seconds...")
    return True 
  return False

# check if reject based on the load probability
def checkReject():
  num = random.randint(1, 10)
  # reject if within range 
  print("generated num is: %d" % (num))
  print(loadProb*10)
  if (num >= 1 and num <= loadProb*10):
    print("reject the task")
    return True
  
  return False

# assume we know the policy and load prob
class ComputeNodeHandler:

  elapsedTime = -1

  def __init__(self):
    self.log = {}

  # return -1 means reject the task, otherwise return elapsed time of processing the image task
  def singleImageProcess(self, fileName):
    start = time.time()
    
    processConfig()

    elapsedTime = -1
    global policy
    print("policy is " + policy)
    if(policy == "load-balance"):
      # if node rejects the task, return -1
      if(checkReject()):
        return -1

    delay = injectDelay()
    elapsedTime = imageProcessTime(fileName, delay)
    end = time.time()
    elapsedTime = (end-start)
    # finish processing image
    print("the elapsed time of processing image %s is %f" % (fileName, elapsedTime))
    log("the elapsed time of processing image " + str(fileName) + " is " + str(elapsedTime) + "\n")
    return elapsedTime

if __name__ == '__main__':
  log("Starting compute node ..." + str(time.time()) + "\n")
  # Arguments passed - 0/1/2/3, which represents which node to connect
  nodeNum = sys.argv[1]
  print("Index of node: ", sys.argv[1])
  
  currNodeName = "node_" + str(nodeNum)
  print(currNodeName)

  # get the corresponding port of the compute node
  nodePort = 0
  if (currNodeName == "node_0"):
    nodePort = 6091
  elif (currNodeName == "node_1"):
    nodePort = 6092
  elif (currNodeName == "node_2"):
    nodePort = 6093
  else:
    nodePort = 6094

  handler = ComputeNodeHandler()
  processor = computeNode.Processor(handler)
  # set to be "0.0.0.0" to allow for remote connections
  transport = TSocket.TServerSocket(host="0.0.0.0", port=nodePort)
  tfactory = TTransport.TBufferedTransportFactory()
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()

  # multithreaded server for compute node
  server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

  print('Starting the compute node server...')
  log('Starting the compute node server...')
  server.serve()
  print('done.')
  log('done')
