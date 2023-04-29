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
  
  def parseFile(self,inputFile,invertedIndexDict):
    file = open(inputFile,"r+")
    fileLines = file.readlines()
    for line in fileLines:
      if line[-1] == '\n':
        line = line[:-1]
      lineSplit = line.split(" ")
      key=lineSplit[0]
      if key not in invertedIndexDict.keys():
        invertedIndexDict[key] = []
      for i in range(1,len(lineSplit)):
        invertedIndexDict[key].append(lineSplit[i])

  
  def naturaljoin(self,inputFileList,outputFileName):
    outputFilePath = self.outputDirectory+outputFileName
    invertedIndexDict = {}
    for tmpFile in inputFileList:
      self.parseFile(tmpFile,invertedIndexDict)
    outFile = open(outputFilePath,"w+")
    for key in invertedIndexDict.keys():
      line = str(key)
      for val in invertedIndexDict[key]:
        line += " "
        line += val
      outFile.write(line)
      outFile.write("\n")
    outFile.close()

  def GetInputForReducerOperations(self, request, context):
    print(request)
    print(request.fileInputList)
    # pdb.set_trace()
    inputFileList = []
    for filePath in request.fileInputList:
      inputFileList.append(filePath)
    outputFileName = request.outputFileName
    reducerThread = Thread(target=self.naturaljoin,args = [inputFileList,outputFileName])
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