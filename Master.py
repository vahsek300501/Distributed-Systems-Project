import grpc
from concurrent import futures
import sys
import time
import os
sys.path.insert(0,"generatedBuffers")
import generatedBuffers.protocolBufferMaster_pb2_grpc as pb2_grpc_master
import generatedBuffers.protocolBufferMaster_pb2 as pb2_master
import generatedBuffers.protocolBufferMapper_pb2_grpc as pb2_grpc_mapper
import generatedBuffers.protocolBufferMapper_pb2 as pb2_mapper

class Master(pb2_grpc_master.MasterServicer):
  def __init__(self,p_mapperCount,p_reducerCount,inputDirectoryPath):
    self.mapperCount = p_mapperCount
    self.reducerCount = p_reducerCount
    self.inputDirectoryPath = inputDirectoryPath
    self.mapperList = []
    self.reducerList = []

    print("Enter the list of Mappers")
    for i in range(0,self.mapperCount):
      print("Enter the host of "+str(i+1)+" Mapper: ",end="")
      host = input()
      print("Enter the port of "+str(i+1)+" Mapper: ",end="")
      port = int(input())
      mapperChannel = grpc.insecure_channel('{}:{}'.format(host, port))
      mapperStub = pb2_grpc_mapper.MapperStub(mapperChannel)
      self.mapperList.append([host,port,mapperChannel,mapperStub])
    print()
    # os.mkdir("MapperDirectory")

    print("Enter the list of reducers")
    for i in range(0,self.reducerCount):
      print("Enter the host of "+str(i+1)+" Reducer: ",end="")
      host = input()
      print("Enter the port of "+str(i+1)+" Reducer: ",end="")
      port = int(input())
      self.reducerList.append([host,port])
    print()
    os.mkdir("ReducerDirectory")

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
  
  def GetIntermediateResults(self, request, context):
    pass

myMaster = Master(3,2,"./InputFiles")
input()
myMaster.invokeMappers()