#!/usr/bin/env python

import glob
import sys
import os 
import random
import time
import threading
from queue import Queue

sys.path.append('./gen-py/')
sys.path.insert(0, glob.glob('../lib/py/build/lib*')[0])

from proj_dir import ImageProcessing
from proj_dir import computeNode

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# record attempted tasks and rejected tasks of each node
# nodesTasks = {"node_1":[0,0], "node_2":[0,0],...}
nodesTasks = {}

# record each node's machine address
nodes = {}
# nodes = {
#   "node_1":"kh4250-08.cselabs.umn.edu",
#   "node_2":"kh4250-08.cselabs.umn.edu",
#   "node_3":"kh4250-08.cselabs.umn.edu",
#   "node_4":"kh4250-08.cselabs.umn.edu"}

# queue that contains unfinished/rejected tasks
tasksQueue = Queue() 
# tasksQueue = {
#   "data/input_dir/baboon.jpg":-1,
#   "data/input_dir/fruits.jpg":-1,
#   "data/input_dir/mask.png":-1,
#   "data/input_dir/squirrel_cls.jpg":-1}


serverAddress = ""
totalTasks = 0
tasks = []
policy = ""
threadLock = threading.Lock()

# function to write information into serverLog files 
def log(info):
  f = open("serverLog.txt", "a+")
  f.write(info)
  f.close()

# process the config.txt to get current scheduling policy 
def processConfig():
  config = open('config.txt', 'r')
  lines = config.readlines()
  for line in lines:
    line = line.replace(" ","")
    comp = line.split(':') # len 2 list
    if (comp[0] == "policy"):
      global policy
      policy = comp[1].rstrip()
  
  print("policy is %s" % policy)
  log("policy is " + policy + "\n")
    
# process machine.txt to get each node's address and node's name
def processMachine():
  global nodes 
  global nodesTasks
  global serverAddress
  machine = open('machine.txt', 'r')
  lines = machine.readlines()
  for line in lines:
    comp = line.split(" ") # extract first two items, name - address
    if ("node" in comp[0]):
      nodes[comp[0]] = comp[1].rstrip()
      nodesTasks[comp[0]] = [1,0]
    elif ("server" in comp[0]):
      serverAddress = comp[1].rstrip()
  
  print("nodes: ")
  print(nodes)
  print("\n")
  print("nodesTasks")
  print(nodesTasks)
  log("nodes: " + str(nodes) + "\n")
  log("nodeTaskss: " + str(nodesTasks) + "\n")

# function that processes the input folder and save each image filename in a list imagesList
def split(folderName):
  path = folderName
  dirList = os.listdir(path)
  totalTasks = len(dirList)
  imagesList = dirList
  for i in range(totalTasks):
    imagesList[i] = path + dirList[i]
    tasksQueue.put(imagesList[i])

  return totalTasks, imagesList

# helper function which connects to the compute node via RPC by passing hostname, port and image task
def createRPCWithNode(node_name, task, port):
  threadLock.acquire()
  address = nodes[node_name]
  # increase the sent tasks of the current node in the nodesTasks
  nodesTasks[node_name][0] += 1
  threadLock.release()
  log("Start connecting to compute node " + node_name + "\n")
  print("Start connecting to compute node %s " % node_name)

  transport = TSocket.TSocket(address, port)

  # Buffering is critical. Raw sockets are very slow
  transport = TTransport.TBufferedTransport(transport)

  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  # Create a client to use the protocol encoder
  client = computeNode.Client(protocol)

  # Connect!
  transport.open()

  # connect to compute node via RPC and get the elapsed time of processing the current image task
  curr_time = client.singleImageProcess(task)
  
  # -1 represents the node rejects the current image task
  if (curr_time != -1):
    print("Elapsed time of current task %s is %f by %s \n" % (task, curr_time, node_name))
    log("Elapsed time of current task " + str(task) + "is " + str(curr_time) +  " by" +str(node_name) + "\n")
  else:
    threadLock.acquire()
    # increases the rejected tasks of the current node in the nodesTasks
    nodesTasks[node_name][1] += 1
    # push back the rejected task into the tasksQueue again
    tasksQueue.put(task)
    threadLock.release()
    print("Task %s is rejected by the current node %s \n" % (task, node_name))
    log("Task " + str(task) + " is rejected by the current node " + str(node_name) + "\n" )

  # Close!
  transport.close()
  return curr_time

# helper function that checks if any nodes that haven't receive any tasks yet
def checkZeroTasksNode():
  zeroTasks = []
  for x in nodesTasks:
    if nodesTasks[x][0] == 1:
      zeroTasks.append(x)
  
  return zeroTasks

# select node by reject/sendTasks ratio under load-balance policy if all rejection ratios are the same then randomly choose one from 4 nodes
# if there are any nodes with no assigned tasks, then choose from those nodes first
# it will return the chosen node name
def loadBalanceSelectNode():
  nodeList = ["node_0","node_1","node_2","node_3"]
  # the dictionary keeps rejection ratio of each node and initializes rejection ratio to be 0 
  nodesRatio = {"node_0":0, "node_1":0, "node_2":0, "node_3":0}

  zeroTaskNodes = checkZeroTasksNode()
  # choose from zeroTaskNodes first
  if (len(zeroTaskNodes) != 0):
    return random.choice(zeroTaskNodes)
  else:
    largestNode = random.choice(nodeList)
    largestRatio = nodesTasks[largestNode][1]/nodesTasks[largestNode][0]
    # equal is true if all ratios are the same, otherwise false
    equal = True
    val = nodesTasks["node_0"][1]/nodesTasks["node_0"][0]
    # for loop to check if ratios are the same and find the node with largest rejection ratio
    for x in nodesTasks:
      if (val != nodesTasks[x][1]/nodesTasks[x][0]):
        equal = False

      nodesRatio[x] = nodesTasks[x][1]/nodesTasks[x][0]

      if (nodesRatio[x] > largestRatio):
        largestNode = x
        largestRatio = nodesRatio[x]

    if (not equal):
      nodeList.remove(largestNode)

    return random.choice(nodeList)

# return the node name by randomly choose from 4 nodes
def randomSelectNode():
  node_list = ["node_0","node_1","node_2","node_3"]
  node_name = random.choice(node_list)
  return node_name

# customized thread class
class myThread(threading.Thread): 
  def __init__(self, name):
    threading.Thread.__init__(self)
    self.name = name

  # run the thread, each thread will use the while loop to check if the tasks queue is empty or not
  # pop a task out if queue is not empty, push back a rejected task, else ext the thread
  def run(self):
    global nodesTasks
    print("Starting " + self.name)
    log("Starting " + self.name + "\n")
    currPolicy = ""
    threadLock.acquire()
    currPolicy = policy
    threadLock.release()
    print(policy)
    # check if the queue is empty or not, if queue is not empty, the thread keeps poping task
    while (not tasksQueue.empty()):
      threadLock.acquire()
      # pop the top task from tasks queue {"img1"}
      curr_task = tasksQueue.get()
      threadLock.release()

      node_name = ""
      port = 0
      
      #  schedule node based on policy
      if (currPolicy == "load-balance"):
        threadLock.acquire()
        node_name = loadBalanceSelectNode()
        threadLock.release()
      elif (currPolicy == "random"): 
        node_name = randomSelectNode()
        print("randomly choose node %s " % (node_name))
      else:
        print("Can't recognize policy...")
        break

      # specify the port for each compute node
      if (node_name == "node_0"):
        port = 6091
      elif (node_name == "node_1"):
        port = 6092
      elif (node_name == "node_2"):
        port = 6093
      else:
        port = 6094

      # connect to the node via RPC
      taskTime = createRPCWithNode(node_name, curr_task, port)

    print("Exiting " + self.name)
    log("Exiting " + self.name + "\n")
  

# function to create num threads and return the threads list
def createThreads(num):
  threads = []
  for x in range(num):
    threads.append(myThread("Thread-"+str(x+1)))
  return threads

# Handler for the RPC function
class ImageProcessingHandler:

  def __init__(self):
    self.log = {}
  
  def processImages(self, folderName):
    start = time.time()

    #### split tasks ####
    totalTasks, tasks = split(folderName)
    print("There are %d total tasks" % (totalTasks))
    log("There are " + str(totalTasks) + " total tasks \n")
    print(tasks)
    print("\n")

    processConfig()
    print("policy is : %s" % (policy))
    
    #### create 4 threads for each task ####

    numThreads = 4
    threads = createThreads(numThreads)

    # start 4 threads
    for x in range(numThreads):
      threads[x].start()

    ## wait for all threads finish ##
    for t in threads:
      t.join()

    end = time.time()

    ### update nodesTasks back to the correct sent tasks because we initialize it to be [1,0] ####
    for node in nodesTasks:
      nodesTasks[node][0] -= 1

    log("nodeTasks are " + str(nodesTasks) + "\n")
    print("nodeTasks are " + str(nodesTasks))

    print("All tasks are finished and all threads are finishing. Exit Main thread.")
    log("All tasks are finished and all threads are finishing. Exit Main thread.\n\n")
    
    # return elapsed time of finishing all tasks
    return (end-start)


if __name__ == '__main__':
    log("Starting server  ..." + str(time.time()) + "\n")
    
    # get the serve address from machine.txt
    processMachine()
    handler = ImageProcessingHandler()
    processor = ImageProcessing.Processor(handler)
    print(serverAddress)
    # set to be "0.0.0.0" to allow for remote connections
    transport = TSocket.TServerSocket(host="0.0.0.0", port=7090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
    log("done\n\n")
 

    
