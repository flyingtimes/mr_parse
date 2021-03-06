# -*- coding=utf-8 -*-
import pika
from numpy import random

connection = pika.BlockingConnection(pika.ConnectionParameters(
               'sscloud15',5673))  # 默认端口5672，可不写
channel = connection.channel()
#声明queue
channel.queue_declare(queue='mr-storm', durable=True)  # 若声明过，则换一个名字

aa=[
"TD-LTE_MRO_ZTE_OMC1_20170911000000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911000000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911001500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911001500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911003000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911003000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911004500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911004500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911010000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911011500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911013000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911014500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911020000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911021500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911023000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911024500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911030000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911031500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911033000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911034500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911040000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911041500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911043000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911044500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911050000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911051500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911053000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911054500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911060000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911061500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911063000.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911064500.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911070000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911070000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911071500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911071500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911073000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911073000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911074500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911074500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911080000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911080000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911081500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911081500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911083000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911083000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911084500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911084500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911090000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911090000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911091500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911091500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911093000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911093000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911094500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911094500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911100000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911100000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911101500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911101500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911103000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911103000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911104500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911104500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911110000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911110000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911111500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911111500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911113000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911113000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911114500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911114500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911120000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911120000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911121500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911121500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911123000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911123000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911124500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911124500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911130000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911130000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911131500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911131500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911133000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911133000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911134500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911134500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911140000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911140000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911141500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911141500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911143000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911143000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911144500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911144500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911150000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911150000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911151500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911151500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911153000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911153000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911154500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911154500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911160000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911160000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911161500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911161500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911163000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911163000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911164500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911164500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911170000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911170000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911171500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911171500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911173000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911173000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911174500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911174500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911180000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911180000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911181500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911181500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911183000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911183000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911184500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911184500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911190000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911190000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911191500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911191500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911193000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911193000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911194500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911194500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911200000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911200000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911201500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911201500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911203000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911203000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911204500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911204500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911210000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911210000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911211500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911211500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911213000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911213000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911214500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911214500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911220000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911220000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911221500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911221500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911223000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911223000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911224500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911224500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911230000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911230000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911231500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911231500_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911233000_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911233000_2.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911234500_1.zip",
"TD-LTE_MRO_ZTE_OMC1_20170911234500_2.zip"
]
for h in aa:
  l="/cephfs2/docker/mr-storm/data/download/188.2.169.247_MR/MRO/20170911/"+h
  uu='{"ip":"127.0.0.1","date":"20170911","file":%s' % l
  print uu
  #channel.basic_publish(exchange='',
                     routing_key='mr-storm',
                      #body='{"ip":"127.0.0.1","date":"20170812","file":"/Users/gmcc/code/rabbitMQ/TD-LTE_MRO_HUAWEI_188002135194-188002145084_20170901124500_001.zip"}',
                    body=uu,
                      properties=pika.BasicProperties(
                          delivery_mode=2,  # make message persistent
                         )
                      )

print(" [x] Sent 100 'Hello World!'")
connection.close()
