import grpc
from concurrent import futures
import sys
import time
import os
from threading import Lock
sys.path.insert(0,"generatedBuffers")
import generatedBuffers.protocolBufferMaster_pb2_grpc as pb2_grpc_master
import generatedBuffers.protocolBufferMaster_pb2 as pb2_master
import generatedBuffers.protocolBufferMapper_pb2_grpc as pb2_grpc_mapper
import generatedBuffers.protocolBufferMapper_pb2 as pb2_mapper
import generatedBuffers.protocolBufferReducer_pb2_grpc as pb2_grpc_reducer
import generatedBuffers.protocolBufferReducer_pb2 as pb2_reducer

lock = Lock()
class Master(pb2_grpc_master.MasterServicer):
  def __init__(self,p_mapperCount,p_reducerCount,inputDirectoryPath):
    self.mapperCount = p_mapperCount
    self.reducerCount = p_reducerCount
    self.inputDirectoryPath = inputDirectoryPath
    self.mapperList = []
    self.reducerList = []
    self.reducerFileList = []
    self.cntMapperOutputReceived = 0

    print("Enter the list of Mappers")
    for i in range(0,self.mapperCount):
      print("Enter the host of "+str(i+1)+" Mapper: ",end="")
      host = input()
      print("Enter the port of "+str(i+1)+" Mapper: ",end="")
      port = int(input())
      mapperChannel = grpc.insecure_channel('{}:{}'.format(host, port))
      mapperStub = pb2_grpc_mapper.MapperStub(mapperChannel)
      self.mapperList.append([host, port, mapperChannel, mapperStub])
    print()

    print("Enter the list of reducers")
    for i in range(0,self.reducerCount):
      print("Enter the host of "+str(i+1)+" Reducer: ",end="")
      host = input()
      print("Enter the port of "+str(i+1)+" Reducer: ",end="")
      port = int(input())
      reducerChannel = grpc.insecure_channel('{}:{}'.format(host, port))
      reducerStub = pb2_grpc_reducer.ReducerStub(reducerChannel)
      self.reducerList.append([host, port, reducerChannel, reducerStub])
      self.reducerFileList.append([])
    print()

  def invokeMappers(self):
    inputFiles = os.listdir(self.inputDirectoryPath)
    q = int(len(inputFiles)/self.mapperCount)
    r = len(inputFiles)%self.mapperCount
    fileDivision = []
    cntCount = 0
    
    for i in range(0,self.mapperCount):
      tmpFiles = []
      for _ in range(0,q):
        tmpFiles.append(inputFiles[cntCount])
        cntCount += 1
      fileDivision.append(tmpFiles)
    
    for i in range(0,r):
      fileDivision[i].append(inputFiles[cntCount])
      cntCount += 1
    
    mapperListInd = 0
    for shard in fileDivision:
      request = pb2_mapper.MapperInput()
      for file in shard:
        request.fileInputList.append("./InputFiles/"+file)
      request.reducerCount = self.reducerCount
      response = self.mapperList[mapperListInd][3].GetInputForMapperOperations(request)
      mapperListInd+=1
      print(response)
      print()
  
  def invokeReducers(self):
    cntIndexReducer = 0
    for reducerInputList in self.reducerFileList:
      request = pb2_reducer.ReducerInput()
      for filePath in reducerInputList:
        request.fileInputList.append(filePath)
      request.outputFileName = "output"+str(cntIndexReducer)+".txt"
      response = self.reducerList[cntIndexReducer][3].GetInputForReducerOperations(request)
      cntIndexReducer += 1
      print(response)
      print()

  
  def GetIntermediateResults(self, request, context):
    global lock
    lock.acquire()
    self.cntMapperOutputReceived += 1
    cntCount = 0
    for intermediateFile in request.fileInputList:
      self.reducerFileList[cntCount].append(intermediateFile)
      cntCount += 1
    
    if self.cntMapperOutputReceived == self.mapperCount:
      print(self.reducerFileList)
      self.invokeReducers()
    
    response = pb2_master.IntermediateOutput()
    response.status = True
    response.message = "Intermediate Results successfully received"
    lock.release()
    return response


def Main():
  host = "localhost"
  port = 7000
  myMaster = Master(2,2,"./InputFiles")
  master = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
  pb2_grpc_master.add_MasterServicer_to_server(myMaster,master)
  master.add_insecure_port('[::]:'+str(port))
  master.start()
  input()
  myMaster.invokeMappers()
  master.wait_for_termination()

Main()
