import grpc
from concurrent import futures
from threading import Thread
import sys
import time
sys.path.insert(0,"generatedBuffers")
import generatedBuffers.protocolBufferReducer_pb2_grpc as pb2_grpc_reducer
import generatedBuffers.protocolBufferReducer_pb2 as pb2_reducer
import generatedBuffers.protocolBufferMaster_pb2_grpc as pb2_grpc_master
import generatedBuffers.protocolBufferMaster_pb2 as pb2_master
import uuid
import os
import pdb

class Reducer(pb2_grpc_reducer.ReducerServicer):
  def __init__(self,host,port):
    self.reducerHost = host
    self.reducerPort = port
    self.reducerUUID = str(uuid.uuid1())
    self.masterHost = "localhost"
    self.masterPort = 7000
    self.masterChannel = grpc.insecure_channel('{}:{}'.format(self.masterHost, self.masterPort))
    self.masterStub = pb2_grpc_master.MasterStub(self.masterChannel)
    if not os.path.exists("Output/"):
      os.mkdir("Output")
    self.outputDirectory = "Output/"
  
  def cartisianProduct(self,list1,list2):
    finalVals = []
    for val1 in list1:
      for val2 in list2:
        finalVals.append(str(val1)+" "+str(val2))
    print(finalVals)
    return finalVals
  
  def parseFile(self,inputFile,naturalJoinDict):
    file = open(inputFile,"r+")
    fileLines = file.readlines()
    for line in fileLines:
      if line[-1] == '\n':
        line = line[:-1]
      lineSplit = line.split(" ")
      word = lineSplit[0]
      tableNum = int(lineSplit[-1])
      lineSplit = lineSplit[1:-1]
      if word not in naturalJoinDict.keys():
        naturalJoinDict[word] = [[],[]]
      for val in lineSplit:
        naturalJoinDict[word][tableNum-1].append(val)

  def naturalJoin(self,inputFileList,outputFileName):
    print("Running Natural join operations")
    outputFilePath = self.outputDirectory+outputFileName
    naturalJoinDict = {}
    for tmpFile in inputFileList:
      self.parseFile(tmpFile,naturalJoinDict)
    file = open(outputFilePath,"w+")
    file.write("Name Age Role\n")
    for key in naturalJoinDict.keys():
      cartesianProductRes = self.cartisianProduct(naturalJoinDict[key][0],naturalJoinDict[key][1])
      for val in cartesianProductRes:
        file.write(str(key)+" ")
        file.write(val)
        file.write("\n")


  def GetInputForReducerOperations(self, request, context):
    print("Received Inputs running Reduce operations")
    # pdb.set_trace()
    inputFileList = []
    for filePath in request.fileInputList:
      inputFileList.append(filePath)
    outputFileName = request.outputFileName
    reducerThread = Thread(target=self.naturalJoin,args = [inputFileList,outputFileName])
    reducerThread.start()

    response = pb2_reducer.ReducerOutput()
    response.status = True
    response.message = "File received successfully"
    return response

def Main():
  print("Enter the host")
  host = input()
  print("Enter the port")
  port = int(input())
  reducerObj = Reducer(host,port)
  reducer = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  pb2_grpc_reducer.add_ReducerServicer_to_server(reducerObj,reducer)
  reducer.add_insecure_port('[::]:'+str(port))
  reducer.start()
  reducer.wait_for_termination()
Main()