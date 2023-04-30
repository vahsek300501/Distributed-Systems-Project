import grpc
from concurrent import futures
from threading import Thread
import sys
import time
sys.path.insert(0,"generatedBuffers")
import generatedBuffers.protocolBufferMapper_pb2_grpc as pb2_grpc_mapper
import generatedBuffers.protocolBufferMapper_pb2 as pb2_mapper
import generatedBuffers.protocolBufferMaster_pb2_grpc as pb2_grpc_master
import generatedBuffers.protocolBufferMaster_pb2 as pb2_master
import uuid
import os
class Mapper(pb2_grpc_mapper.MapperServicer):
  def __init__(self,host,port):
    self.mapperHost = host
    self.mapperPort = port
    self.mapperUUID = str(uuid.uuid1())
    self.masterHost = "localhost"
    self.masterPort = 7000
    self.masterChannel = grpc.insecure_channel('{}:{}'.format(self.masterHost, self.masterPort))
    self.masterStub = pb2_grpc_master.MasterStub(self.masterChannel)

    if not os.path.exists("MapperDirectory/"):
      os.mkdir("MapperDirectory")
    self.mapperDir = "MapperDirectory/"+self.mapperUUID
    os.mkdir("MapperDirectory/"+self.mapperUUID)
  
  def writeToFile(self,fileLineList,reducerCount):
    filePathList = []
    fileList = []
    for i in range(0,reducerCount):
      filePath = self.mapperDir+"/"+str(i)+".txt"
      filePtr = open(filePath,"w+")
      filePathList.append((filePath,filePtr))
    
    for val in fileLineList:
      key = val[:val.find(" ")]
      keyHash = len(str(key))%reducerCount
      filePath,filePtr = filePathList[keyHash]
      filePtr.write(val)
      filePtr.write("\n")
    
    for fileVar in filePathList:
      fileVar[1].close()
      fileList.append(fileVar[0])
    return fileList

  def parseFile(self,filePath,tableNum,fileLineList):
    file = open(filePath,"r+")
    fileLines = file.readlines()
    file.close()
    fileLines = fileLines[1:]
    for line in fileLines:
      if line[-1] == '\n':
        line = line[:-1]
      line = line +" "
      line = line + str(tableNum)
      fileLineList.append(line)
    

  def naturalJoin(self,inputFileList,reducerCount):
    print("Computing Natural Joins")
    time.sleep(2)
    fileLineList = []
    for filePath in inputFileList:
      print(filePath)
      tableNum = int(filePath[filePath.rfind(".")-1])
      self.parseFile(filePath,tableNum,fileLineList)
    print(fileLineList)

    fileList = self.writeToFile(fileLineList,reducerCount)
    request = pb2_master.IntermediateInput()
    for val in fileList:
      request.fileInputList.append(val)
    print("Invoking Master")
    response = self.masterStub.GetIntermediateResults(request)
    print(response)
      
  def GetInputForMapperOperations(self, request, context):
    print("Received Inputs running Map operations")
    inputFileList = []
    for filePath in request.fileInputList:
      inputFileList.append(filePath)
    reducerCount = request.reducerCount
    invertedIndexThread = Thread(target=self.naturalJoin,args=[inputFileList,reducerCount])
    invertedIndexThread.start()

    response = pb2_mapper.MapperOutput()
    response.status = True
    response.message = "Files Received Successfully"
    return response

def Main():
  print("Enter the host")
  host = input()
  print("Enter the port")
  port = int(input())
  mapperObj = Mapper(host,port)
  mapper = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  pb2_grpc_mapper.add_MapperServicer_to_server(mapperObj,mapper)
  mapper.add_insecure_port('[::]:'+str(port))
  mapper.start()
  mapper.wait_for_termination()
Main()