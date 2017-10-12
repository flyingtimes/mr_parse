# -*- coding=utf-8 -*-
import sys
from argparse import ArgumentParser
sys.path.insert(0, 'libs')
from rabbit_consumer_fq import Rabbit_consumer
import logging
import pika
import time
import random
import string
import os

message_count = 0
message_buff = ''
last_produce_time = int(round(time.time() * 1000))

def get_options():
  parser = ArgumentParser()
  parser.add_argument('--master-hostname', type=str,dest='master', help='set up master hostname',metavar='MASTER', default='sscloud15')
  parser.add_argument('--unzip-path', type=str,dest='unzip_path', help='set up temp unzip path',metavar='UNZIP_PATH', default='./unzip/')
  parser.add_argument('--log-path', type=str,dest='log_path', help='set up temp log path',metavar='UNZIP_PATH', default='./logs/')
  parser.add_argument('--master-port', type=int,dest='master_port', help='set up master service port',metavar='MASTER_PORT', default=5672)
  options = parser.parse_args()
  return options
def read_from_file(filename):
  content = ''
  i = 0
  for i in range(0,3):
    try:
      with open(filename,"r") as f:
        for line in f.readlines():
          content+=line
      os.remove(filename)
      break
    except Exception,e:
      logging.error("unable to read file %s:%s"%(filename,str(e)))
      time.sleep(3)
  return content
							
def rabbit_callback(ch, method, properties, body):
  global message_count,message_buff,last_produce_time
  current_time = int(round(time.time() * 1000))
  delta_time = current_time - last_produce_time
  content = read_from_file(body)
  if not message_buff:
    message_buff = content
  else:
    message_buff+=content
  message_count += 1
                    
  if message_count>150 or delta_time>300000:
    try:
      salt = ''.join(random.sample(string.ascii_letters + string.digits, 16))
      with open("/cephfs2/mr_db/"+salt+".txt","w") as f:
        f.write(message_buff)
      message_buff = ''
      message_count = 0
      last_produce_time = current_time
      os.system("sshpass -p gpadmin ssh gpadmin@sscloud10 \"bash import_to_db.sh /ceph/cephfs2/mr_db/"+salt+".txt rnodb.m_mro_rsrp_all_15\"")
    except Exception,e:
      logging.error("%s import to db fail:%s" % (salt,str(e)))
      
  ch.basic_ack(delivery_tag = method.delivery_tag)
                    
      
                    

# 获取命令行参数
options = get_options()

# log file configuration        
logging.basicConfig(level=logging.ERROR,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S',filename=options.log_path+"decode_to_db.log",filemode='a')

# 初始化rabbitMQ

# 初始化rabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(
               options.master,options.master_port))
channel = connection.channel()
channel.queue_declare(queue='mr-decoded', durable=True)
channel.basic_qos(prefetch_count=1)  # 类似权重，按能力分发，如果有一个消息，就不在给你发

# 配置要消费的消息
channel.basic_consume(
                      rabbit_callback,# 如果收到消息，就调用callback
                      queue='mr-decoded',
                      # no_ack=True  # 一般不写，处理完接收处理结果。宕机则发给其他消费者
                      )



# 开始获取消息
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()




