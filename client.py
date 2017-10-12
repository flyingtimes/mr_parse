# -*- coding=utf-8 -*-
import sys
from argparse import ArgumentParser
sys.path.insert(0, 'libs')
from rabbit_consumer_fq import Rabbit_consumer
import logging
import time
import socket
def get_options():
  parser = ArgumentParser()
  parser.add_argument('--client-name', type=str,dest='client_name', help='set up a client name',metavar='CLIENT_NAME', required=True)
  parser.add_argument('--thread-number', type=int,dest='threadnum', help='set up multi thread number',metavar='THREAD_NUMBER', default=4)
  parser.add_argument('--master-hostname', type=str,dest='master', help='set up master hostname',metavar='MASTER', default='sscloud15')
  parser.add_argument('--unzip-path', type=str,dest='unzip_path', help='set up temp unzip path',metavar='UNZIP_PATH', default='./unzip/')
  parser.add_argument('--log-path', type=str,dest='log_path', help='set up temp log path',metavar='UNZIP_PATH', default='./logs/')
  parser.add_argument('--master-port', type=int,dest='master_port', help='set up master service port',metavar='MASTER_PORT', default=5672)
  parser.add_argument('--decode-path', type=str,dest='decode_path', help='set up decode file path',metavar='DECODE_PATH', default='./decode/')
  options = parser.parse_args()
  return options

# 获取命令行参数
options = get_options()
options.client_name=socket.gethostname()

# log file configuration        
logging.basicConfig(level=logging.ERROR,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S',filename=options.log_path+"parser.log",filemode='a')

# 初始化rabbitMQ
RC = Rabbit_consumer()
channel = RC.get_rb_channel(options,logging)
# 配置要消费的消息
channel.basic_consume(
                      RC.rabbit_callback,# 如果收到消息，就调用callback
                      queue='mr-storm',
                      # no_ack=True  # 一般不写，处理完接收处理结果。宕机则发给其他消费者
                      )



# 开始获取消息
print(' [*] Waiting for messages. To exit press CTRL+C')
try:
  logging.error("comsuming...")
  RC = Rabbit_consumer()
  channel = RC.get_rb_channel(options,logging)
  channel.basic_consume(
                    RC.rabbit_callback,# 如果收到消息，就调用callback
                    queue='mr-storm',
                    # no_ack=True  # 一般不写，处理完接收处理结果。宕机则发给其他消费者
                    )
  channel.start_consuming()
except Exception,e:
  logging.error("Main thread break:%s" % str(e))
  time.sleep(3)
    
  



