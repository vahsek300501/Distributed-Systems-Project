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
  
  def parseFile(self,inputFile,invertedIndexDict):
    file = open(inputFile,"r+")
    fileLines = file.readlines()
    for line in fileLines:
      fields = line.strip().split(',')
      for i in range(1,len(fields)):
        if fields[0] not in invertedIndexDict.keys():
          invertedIndexDict[fields[0]] = []
        invertedIndexDict[fields[0]].append((fields[i],inputFile.split("_")[1]))

  def partitionFiles(self,invertedIndexDict,reducerCount):
    filePathList = []
    fileList = []
    for i in range(0,reducerCount):
      filePath = self.mapperDir+"/"+str(i)+".txt"
      filePtr = open(filePath,"w+")
      filePathList.append((filePath,filePtr))
    
    for key in invertedIndexDict.keys():
      line = str(key)
      keyHash = len(str(key))%reducerCount
      filePath,filePtr = filePathList[keyHash]
      for val in invertedIndexDict[key]:
        line += " "
        line += val[0]+","+val[1]
      filePtr.write(line)
      filePtr.write("\n")
    
    for fileVar in filePathList:
      fileVar[1].close()
      fileList.append(fileVar[0])
    return fileList
    
  def naturaljoin(self,inputFileList, reducerCount):
    time.sleep(2)
    invertedIndexDict = {}
    for file in inputFileList:
      self.parseFile(file,invertedIndexDict)
    fileList = self.partitionFiles(invertedIndexDict,reducerCount)
    request = pb2_master.IntermediateInput()
    for val in fileList:
      request.fileInputList.append(val)
    response = self.masterStub.GetIntermediateResults(request)
    print(response)
      
  def GetInputForMapperOperations(self, request, context):
    inputFileList = []
    for filePath in request.fileInputList:
      inputFileList.append(filePath)
    reducerCount = request.reducerCount
    NaturalJoinThread = Thread(target=self.naturaljoin,args=[inputFileList,reducerCount])
    NaturalJoinThread.start()

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