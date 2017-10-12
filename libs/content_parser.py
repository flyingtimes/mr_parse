# -*- coding=utf-8 -*-

import time
import json
import os
from lxml import etree
import logging
from datetime import datetime

def extract_info_from_file_name(file_name):
    raw_name = file_name.split('/')[-1]
    fields = raw_name.split('_')
    res={}
    res['network_type']=fields[0]
    res['mr_type']=fields[1]
    res['vendor']=fields[2]
    res['enodebid']=fields[4]
    res['date']=fields[5].split('.')[0][:8]
    res['hour']=fields[5].split('.')[0][8:10]
    res['minute']=fields[5].split('.')[0][10:12]
    return res
  
# process every file content			
def fast_iter(context, func,file_info,output):
  iter_result={}
  try:
    for event, elem in context:
      func(elem,file_info,iter_result)
  except Exception,mm:
    a=1
  for line in iter_result:
      res=iter_result[line]
      if line=="LteScEarfcn" or line=="LteNcEarfcn" or line=="LteScTadv" or line=="LteScRSRP" or line=="LteNcRSRP" or line=="field_len":
        continue
      if len(res["date"])==8:
        currenttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s_res=str(res["enodebid"])+","+res["date"][0:4]+"-"+res["date"][4:6]+"-"+res["date"][6:8]+" "+str(res["hour"])+":"+str(res["minute"])+":00,"+currenttime+","+str(res["rsrpcount"])+","+str(res["rsrpcount_110"])+","+str(res["rsrpcount_nc_strong"])+","
        s_res+='%d,%d,%d,%d,%d,%d,%d,%d' % (iter_result[line]["ydlt_rsrpcount"],iter_result[line]["ydlt_rsrpcount_110"],iter_result[line]["yddx_rsrpcount"],iter_result[line]["yddx_rsrpcount_110"],iter_result[line]["lt_rsrpcount"],iter_result[line]["lt_rsrpcount_113"],iter_result[line]["dx_rsrpcount"],iter_result[line]["dx_rsrpcount_113"])
        output.append(s_res)

# process every line in content		
def iter_func(elem,file_info,iter_result):
  neighbour_cell_num=0
	#一个 elem 代表一个采样点
  # 寻找这个厂家的counter的标签顺序信息
  if elem.tag=='smr' and not iter_result.has_key('LteScRSRP'):
		i=0
		for field_name in elem.text.lstrip().rstrip().split(' '):
			if field_name=='MR.LteScRSRP':
				iter_result["LteScRSRP"]=i
			if field_name=='MR.LteNcRSRP':
				iter_result["LteNcRSRP"]=i
			if field_name=='MR.LteScEarfcn':
				iter_result["LteScEarfcn"]=i
			if field_name=='MR.LteNcEarfcn':
				iter_result["LteNcEarfcn"]=i
			if field_name=='MR.LteScTadv':
				iter_result["LteScTadv"]=i	
			i=i+1
			
		iter_result['field_len']=len(elem.text.lstrip().rstrip().split(' '))
  if elem.tag!='object':
    return
  if not elem.get("id"):
		return
  if ":" in elem.get("id"):
		return

  # 该采样点中的小区id如果未被处理过，初始化输出结果的数组
  if not iter_result.has_key(elem.get("id")):
    elemid = elem.get("id")
    iter_result[elemid]={}
    iter_result[elemid]["enodebid"]='460-00-'+str(int(elemid)/256)+'-'+str(int(elemid)%256)
    iter_result[elemid]["date"]=file_info['date']
    iter_result[elemid]["hour"]=file_info['hour']
    iter_result[elemid]["minute"]=file_info['minute'] 
    iter_result[elemid]["rsrpcount"]=0
    iter_result[elemid]["rsrpcount_110"]=0
    iter_result[elemid]["rsrpcount_nc_strong"]=0
    iter_result[elemid]["rsrpsum"]=0
    iter_result[elemid]["tasum"]=0
    iter_result[elemid]["tacount"]=0
    iter_result[elemid]["ydlt_rsrpcount"]=0
    iter_result[elemid]["ydlt_rsrpcount_110"]=0
    iter_result[elemid]["yddx_rsrpcount"]=0
    iter_result[elemid]["yddx_rsrpcount_110"]=0
    iter_result[elemid]["lt_rsrpcount"]=0
    iter_result[elemid]["lt_rsrpcount_113"]=0
    iter_result[elemid]["dx_rsrpcount"]=0
    iter_result[elemid]["dx_rsrpcount_113"]=0
    
	# 处理采样点中的每一个邻区关系对
  same_freq_nc_strong_pairs=0
  i=0
  ja=0
  jb=0
  ha=0
  hb=0
  for ele in elem.getchildren():
    # f 是每一个主邻关系对的数据字符串
    f = ele.text.lstrip().rstrip().split(' ')
    #采样点测量报告有几种，要匹配测量报告的字段个数一致的才行
    if len(f)==iter_result['field_len'] and f[iter_result['LteScRSRP']]!='NIL':
      #rsrpcount，rsrpsum,rsrp_110
      if i==0:
				iter_result[elem.get("id")]['rsrpcount']+=1
				val=int(f[iter_result['LteScRSRP']])
				iter_result[elem.get("id")]["rsrpsum"]+=val
				if val>=31:
					iter_result[elem.get("id")]["rsrpcount_110"]+=1
      #TA count
      if i==0 and f[iter_result['LteScTadv']]!='NIL':
				iter_result[elem.get("id")]['tacount']+=1
				iter_result[elem.get("id")]['tasum']+=int(f[iter_result['LteScTadv']])
      #计算联通异频率测量的统计结果
      if (f[iter_result['LteNcEarfcn']]!='NIL' and f[iter_result['LteNcEarfcn']]=='1650' and f[iter_result['LteNcRSRP']]!='NIL'):
        # 移动的信号只计算一次
        if ja==0:
          iter_result[elem.get("id")]['ydlt_rsrpcount']+=1
          iter_result[elem.get("id")]['lt_rsrpcount']+=1
          val=int(f[iter_result['LteScRSRP']])
          if val>=31:
            iter_result[elem.get("id")]['ydlt_rsrpcount_110']+=1
          ja=ja+1
        # 联通要寻求最强的一个信号计算
        if jb==0:
          val=int(f[iter_result['LteNcRSRP']])
          if val>=28:
            iter_result[elem.get("id")]['lt_rsrpcount_113']+=1
            jb=jb+1            
      #计算电信异频率测量的统计结果
      if (f[iter_result['LteNcEarfcn']]!='NIL' and (f[iter_result['LteNcEarfcn']]=='1825' or f[iter_result['LteNcEarfcn']]=='100') and f[iter_result['LteNcRSRP']]!='NIL'):
        # 移动的信号只计算一次
        if ha==0:
          iter_result[elem.get("id")]['yddx_rsrpcount']+=1
          iter_result[elem.get("id")]['dx_rsrpcount']+=1
          val=int(f[iter_result['LteScRSRP']])
          if val>=31:
            iter_result[elem.get("id")]['yddx_rsrpcount_110']+=1
          ha=ha+1
        # 电信要寻求最强的一个信号计算
        if hb==0:
          val=int(f[iter_result['LteNcRSRP']])
          if val>=28:
            iter_result[elem.get("id")]['dx_rsrpcount_113']+=1
            hb=hb+1
      #MR样本点中测量到的同频邻区f[4]==f[6]的电平和主小区电平（主小区RSRP＞-110dBm）f[0]>31差大于-6dBf[1]-f[0]>-6且满足以上条件的同频邻区数目大于等于3的样本点的比例
      if (f[iter_result['LteNcRSRP']]!='NIL' and f[iter_result['LteScEarfcn']]!='NIL' and f[iter_result['LteNcEarfcn']]!='NIL' and f[iter_result['LteScEarfcn']]==f[iter_result['LteNcEarfcn']] and f[iter_result['LteScRSRP']]>31 and int(f[iter_result['LteNcRSRP']])-int(f[iter_result['LteScRSRP']])>-6):
        same_freq_nc_strong_pairs+=1
      i+=1
      if same_freq_nc_strong_pairs>2:
        iter_result[elem.get("id")]['rsrpcount_nc_strong']+=1    
    
# 解析函数
def iter_content(f,file_info):
  context = etree.iterparse(f, events=('end',), )
  output=[]

  fast_iter(context,iter_func,file_info,output)

  return output
