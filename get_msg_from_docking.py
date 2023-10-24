#coding=utf-8

import json
import logging
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from kafka.errors import KafkaError, KafkaTimeoutError

import time
import sys
import pickle
import re

from multiprocessing import Process
import threading
import queue


queue = queue.Queue()

#全局变量统计推送数量
total = 0

#lock = threading.Lock()

class KafkaToKafka():
	
	partition_id = 0

	def InitConsumer(self, partition_id=1):
		try:
			consumer = KafkaConsumer(
						bootstrap_servers=['37.40.8.9:9092','37.40.8.92:9092','37.40.8.183:9092'],
						auto_offset_reset='latest',
						group_id='fusion-ingress-image-consumer08310901',
						value_deserializer=lambda x: json.loads(x.decode('utf-8'))
						)
			consumer.assign([TopicPartition('dockingMessage',partition_id)])
		except Exception as e:
			logging.exception("Init Consumer Failed：%s", str(e))
		
		return consumer

	def InitProducer(self):
		try:
			producer = KafkaProducer(
							bootstrap_servers=['MRS-ZB-KAFKA01:50003'],
							batch_size=563840,
							linger_ms=30000,
							acks=0,
							compression_type="gzip",
							value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		except Exception as e:
			logging.exception("Init Producer Failed：%s", str(e))

		return producer

	def Consumer(self):
		try:
			kk = KafkaToKafka()
			print("Consume Partition:",self.partition_id)
			consumer = kk.InitConsumer(partition_id=self.partition_id)
			producer = kk.InitProducer()
	
			#lock.acquire()
			batch = []
			for msg in consumer:
				result = {}
				
				data = json.dumps(msg.value)
				message = json.loads(data)
			
                #result['is_save_image'] = True	
				#capture_time
				result['capture_time'] = message['capture_time']
				if '-' not in str(result['capture_time']):
					time_obj = time.localtime(float(result['capture_time']))
					result['capture_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time_obj)

				#region_id
				if 'region_id' in message:
					result['region_id'] = int(message['region_id'])
				else:
					if 'dip_region_id' in message:
						result['region_id'] = int(message['dip_region_id'])
					elif 'item_params' in message:
							data1 = message['item_params']
							data2 = eval(str(json.loads(json.dumps(data1))))
							data3 = data2['video_params']
							if data3['private_data']:
								data4 = json.loads(data3['private_data'])
								if 'REGION_ID' in data4:
									result['region_id'] = data4['REGION_ID']
								else:
									result['region_id'] = 0
				#location_id
				if 'loc_id' in message:
					result['loc_id'] = message['loc_id']
				else:
					if 'item_params' in message:
						data1 = message['item_params']
						data2 = eval(str(json.loads(json.dumps(data1))))
						data3 = data2['video_params']
						if data3['private_data']:
							data4 = json.loads(data3['private_data'])
							result['loc_id'] = data4['LOC_ID']
			
				#device_id
				if 'dev_id' in message:
					result['dev_id'] = str(message['dev_id'])
				else:
					#视频流不存在device_id
					result['dev_id'] = '0'

				#image_url
				result['image_url'] = message['image_url']
                
                #result['is_save_image'] = True

				#source_id
				try:
					if 'source_id' in message and message['source_id'] != '':
						result['source_id'] = int(message['source_id'])
					else:
						result['source_id'] = 0
                #result['is_save_image'] = True

				except Exception as e:
					print(message)
					print(str(e))
					break
				#detection
			#	if 'detection' in message:
			#	    result['detection'] = message['detection']
			#	else: 
			#		if 'objects' in message:
			#			#result['detection'] = message['objects'][0]['detection']
			#			data = message['objects']
			#			match = re.search(r'"detection":({.*?})', str(data))
			#			if match:
			#				result['detection'] = eval(match.group(1))
				#license_plate
				if 'license_plate1' in message:
					result['license_plate'] = message['license_plate1']
				result['is_save_image'] = True
                #print(result)
				queue.put(result)
				#print(queue.qsize())
				
				while not queue.empty():
					batch.append(queue.get())
					if len(batch) == 1000:
						for i in range(1000):
							producer.send("fusionImageMessage", batch[i])
							producer.flush()
						print("Producer",self.partition_id, "send 1000 messages success!")
						batch = []
						total = total + 1000

			#lock().release()

		except Exception as e:
			logging.exception("Consume Failed：%s", str(e))


	def hourly_timer():
		while True:
			current_time = time.strftime("%H:%M:%S", time.localtime())
			if current_time == "00:00:00":
				logging.info(current_time,":Producer have sended", total, "messages")
			time.sleep(60)  # 每分钟检查一次时间


def main():
	
	logging.basicConfig(filename='get_msg_from_docking.log', level=logging.DEBUG)

	try:
		kk = KafkaToKafka()
		producer = kk.InitProducer()
		
		try:
			kk.hourly_timer()
			
			process_list = []
			for i in range(8):
					#consumer = kk.InitConsumer(i)
					p = Process(target=kk.Consumer)
					p.start()
					process_list.append(p)
					print("Start Consumer Process",i)
					kk.partition_id += 1
			for i in process_list:
				p.join
		except Exception as e:
			logging.exception("start process failed:", str(e))

	except KeyboardInterrupt:
		logging.exception("ctrl c 终止运行")

if __name__ == "__main__":
	try:
		main()
	finally:
		sys.exit()
















