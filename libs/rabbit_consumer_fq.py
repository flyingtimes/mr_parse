# -*- coding=utf-8 -*-
import pika
import multiprocessing
import time
import json
import zipfile
import os
import gzip,tarfile
from lxml import etree
import logging
import content_parser
from content_parser import extract_info_from_file_name
from content_parser import iter_content
from shutil import copyfile

# 这个文件负责处理消息
# 一个消息对应于一个15分钟压缩包文件，这里会将它用多进程进行解压，解压后的文件
# 传递给 iter_content() 进行解析
# 解析的结果，返回到output数组

# 进程处理函数
def thread_callback(subtask_item):
    decode_json = json.loads(subtask_item)
    client_name = decode_json["client_name"]
    task_content = decode_json["task"]
    filename = decode_json["file"]
    unzip_path = decode_json["unzip_path"]
    output=[]
    # 解压第二重压缩包，按照不同方式解压gz和zip两种格式文件
    try:
      if task_content.endswith('.gz'):
        f = gzip.open(unzip_path+task_content, 'rb')
        file_info=extract_info_from_file_name(unzip_path+task_content)
        # 调用外部过程，获取解析结果
        output = iter_content(f,file_info)
        f.close()
        os.remove(unzip_path+task_content)

      elif task_content.endswith('.zip'):
        f =zipfile.ZipFile(unzip_path+task_content,'r')
        a_file=f.namelist()[0]
        f.extract(a_file,unzip_path)
        f.close()
        f = open(unzip_path+a_file, 'rb')
        # 获取输出结果
        file_info=extract_info_from_file_name(unzip_path+a_file)
        # 调用外部过程，获取解析结果
        output = iter_content(f,file_info)
        f.close()
        os.remove(unzip_path+a_file)
        os.remove(unzip_path+task_content)
      else:
        return []
    except Exception, e:
      print "error while extracting %s:%s" % (filename,str(e))
      return []
      
    thread_name = multiprocessing.current_process().name
    #info = "(%s,%s):sub task %s finished.date=%s,file=%s" %(client_name,thread_name,task_content,decode_json["date"],decode_json["file"])
    #lock.acquire()
    #lock.release()
    return output
  


def init(l):
    global lock
    lock = l    
class Rabbit_consumer:
  myoptions = {}
  def extract_helper(self,unzip_type,filename,to_path):
    mfile = []
    copyfile(filename, to_path+filename.split('/')[-1])
    filename = to_path+filename.split('/')[-1]
    if unzip_type=="zip": 
      f = zipfile.ZipFile(filename,'r')
      mfile=f.namelist()
    elif unzip_type=="gz":
      f = tarfile.open(filename,'r:gz')
      mfile = f.getnames()
    else:
      return mfile
    # 开始解压缩
    for a_file in mfile:
      if a_file.endswith('xml.gz') or a_file.endswith('.zip'):
        f.extract(a_file,path = to_path)
    f.close()
    os.remove(filename)
    return mfile

  def get_rb_channel(self,options,logging):
    self.myoptions = options
    self.logging = logging
    # 初始化rabbitMQ
    self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                   options.master,options.master_port))
    channel = self.connection.channel()
    channel.queue_declare(queue='mr-storm', durable=True)
    channel.basic_qos(prefetch_count=1)  # 类似权重，按能力分发，如果有一个消息，就不在给你发
    return channel
  def disconnect_channel(self):
    try:
      self.connection.close()
    except Exception,e:
      a=1
    
  def unzip_first(self,filename):
    mfile=[]
    # 解压第一重压缩包
    try:
      if filename.endswith('.zip'):
        mfile = self.extract_helper("zip",filename,self.myoptions.unzip_path)
      elif filename.endswith('.gz'):
        mfile = self.extract_helper("gz",filename,self.myoptions.unzip_path)
    except Exception,e:
      self.logging.error("unable to open big zip file %s,Exception details:%s" %(filename,str(e)))
      return []
    return mfile
  
  def append_options_to_task(self,subtask,decode_json):
    task_with_options = []
    # 往任务中添加原始文件可选信息
    for item in subtask:
      item_dict = {
        "client_name":self.myoptions.client_name,
        "ip":decode_json["ip"],
        "date":decode_json["date"],
        "file":decode_json["file"],
        "unzip_path":self.myoptions.unzip_path,
        "task":item
      }
      task_with_options.append(json.dumps(item_dict))
    return task_with_options
  
  # rabbitMQ 消息处理函数
  def rabbit_callback(self,ch, method, properties, body):
      decode_json = json.loads(body)
      self.ch = ch
      self.method = method
      filename = decode_json["file"]
      if not ('MRO' in filename.split('/')[-1] and 'TD' in filename.split('/')[-1]):
        self.ch.basic_ack(delivery_tag = method.delivery_tag)  # 告诉生产者，消息处理完成
        return
      try:
        if os.path.exists('/mem_swap'):
          os.system('rm -rf /mem_swap/*')
      except Exception,e:
        a=1
        
      subtask = self.unzip_first(filename)
      # 第一重解压失败，该任务完结，写错误日志
      if not subtask:
        print "faild by sub"
        self.ch.basic_ack(delivery_tag = method.delivery_tag)  # 告诉生产者，消息处理完成
        return
      # 解压成功，开始分线程二次解压  
      task_with_options = self.append_options_to_task(subtask,decode_json)
      
      l = multiprocessing.Lock()
      
      batch_size = self.myoptions.threadnum
      batch_num = len(task_with_options)/batch_size
      all_rs = []
      self.logging.error("total %d sub task found in %s." % (len(task_with_options),decode_json["file"]))
      if (len(task_with_options)>=batch_size):
        for m in range(0,batch_num):
          try:
            pool = multiprocessing.Pool(processes=self.myoptions.threadnum,initializer=init, initargs=(l,))
            thread_result = pool.map(thread_callback,task_with_options[m*batch_size:(m+1)*batch_size])
            if not all_rs:
              all_rs = thread_result
            else:
              all_rs.extend(thread_result)
            pool.close()
          except Exception,e:
            self.logging.error(str(e))
      
      if (len(task_with_options)%batch_size!=0):
        
        pool = multiprocessing.Pool(processes=self.myoptions.threadnum,initializer=init, initargs=(l,))
        thread_result = pool.map(thread_callback,task_with_options[batch_num*batch_size:len(task_with_options)])
        
        if not all_rs:
          all_rs = thread_result
        else:
          all_rs.extend(thread_result)
        pool.close()
      decode_file_name = self.myoptions.client_name+'_'+ decode_json["date"]+'_'+decode_json["ip"]+'_'+decode_json["file"].split('/')[-1]+'.txt'
      try:
        f = open(self.myoptions.decode_path+decode_file_name,'w')
        for m in all_rs:
          for h in m:
            f.write(h+"\n")
        f.close()
      except Exception,e:
        self.logging.error("%s can not write %s to %s:%s" % (self.myoptions.client_name,decode_file_name,self.myoptions.decode_path,str(e)))
        time.sleep(5)
        return
      try:
        ch.queue_declare(queue='mr-decoded', durable=True)
        ch.basic_publish(exchange='',
                         routing_key='mr-decoded',
                         body=self.myoptions.decode_path+decode_file_name,
                         properties=pika.BasicProperties(
                          delivery_mode=2,  # make message persistent
                         )
                      )
        self.ch.basic_ack(delivery_tag = self.method.delivery_tag)  # 告诉生产者，消息处理完成
      except Exception,e:
        self.logging.error("%s can not send decode message :%s"%(self.myoptions.decode_path+decode_file_name,str(e)))
        
