import serial
import struct
import queue
import threading

import time
import numpy
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import struct
import time
import json
import pprint
import datetime
import dateutil
import dateutil.tz
import traceback
import os.path
from wunderground import pws_upload;

def getPackets(serdev, q):
	while True:
		try:
			d = serdev.readline()
		except serial.serialutil.SerialException:
			break

		if len(d) > 0:			
			q.put(d)

def int_map(x, in_min, in_max, out_min, out_max):	
	# from https://www.arduino.cc/en/Reference/Map
	return (x - in_min) * (out_max - out_min) / (in_max - in_min) + out_min;

def cvt_temp(x):
	t = (x[0] << 8) + x[1]
	# check sign bit	
	if (t & 0x8000) == 0x8000:
		# if set, invert and add one to get value, then make negative
		t = -((t ^ 0xffff) + 1)
	return round(t / 160.0, 2)

# additional sensor definitions from
# https://github.com/kobuki/VPTools
sensor_table = {2: 'cap_voltage',
				4: 'uv_index',
				5: 'rain_secs',
				6: 'solar_irradiation',
				7: 'panel_voltage',
				8: 'outside_temperature',
				9: '10m_wind_gust',
				10: 'outside_humidity',
				14: 'rain_tips'}
sensor_decode = {4: lambda x: round(  ((x[0] << 8) + (x[1] >> 6)) / 50.0, 2),
				 5: lambda x: x,
				 6: lambda x: round(  ((x[0] << 8) + (x[1] >> 6)) * 1.757936, 2),
				 8: cvt_temp,
				 9: lambda x: x[0],
				 10: lambda x: round( (((x[1] >> 4) << 8) + x[0]) / 10.0, 2), # in %
				 14: lambda x: x[0] # this is a one byte value with the number of ticks
				 }
def decodePacket(pkt):
	'''
	pkt is a hex string like: C FFC1 30 5 BF FF C1 8A F3 20 FF FF
	The first byte is the channel, then RSSI as a 16 bit signed in,
	then the remaining 10 bytes are the davis packet as defined here:
	https://github.com/dekay/im-me/blob/master/pocketwx/src/protocol.txt#L51

	'''
	# parse hex digits from the space separated string
	parts = filter(None, pkt.decode("utf-8").split(' '))
	b = list(map(lambda x: int(x, 16), parts))

	chan, rssi, davis = b[0], b[1], b[2:]
	# check sign bit
	if (rssi & 0x8000) == 0x8000:
		# if set, invert and add one to get value, then make negative
		rssi = -((rssi ^ 0xffff) + 1)


	info={'chan': chan, 'rssi': rssi}

	sid = davis[0] >> 4
	info['sensor_data'] = davis[3:5]
	if sid in sensor_table:
		info['sensor_name'] = sensor_table[sid]
		info[sensor_table[sid]] = sensor_decode[sid](davis[3:5])
		info['sensor_value'] = sensor_decode[sid](davis[3:5])
	else:
		info['sensor_name'] = 'Unknown (0x{:02x})'.format(davis[0]>>4)
		
	info['iss_id'] = davis[0] & 0x7
	info['battery_low'] = (davis[0] & 0x8) == 0x8
	info['raw_packet'] = davis

	info['wind_speed'] = davis[1]
	info['wind_direction'] = round(int_map(davis[2], 0, 255, 0, 359), 1)
	pprint.pprint(info)


def loadpacket(s):
	"""
	s is a string with key:value pairs separated by commas
	raw is a - sepearted list of hex values 
	"""
	d = {}
	parts = s.split(",")
	for part in parts:
		k,v = part.split(":")
		try:
			d[k.strip()] = float(v.strip()) if '.' in v else int(v.strip())
		except ValueError:
			d[k.strip()] = v.strip()
	
	if 'raw' in d:		
		d['raw'] = b''.join([struct.pack("B", int(x,16)) for x in d['raw'].split("-")])
	if 'packets' in d:
		packets, lost, ratio = d['packets'].split('/')
		d['packets'] = {'received': int(packets),
						'lost': int(lost),
						'ratio': float(ratio)}
	return d


def update(col, doc):
	"""
	update mongodb with the current document of readings.
	
	there is one document for every ten minute interval

	document structure:

	<station name> (collection)
		<year>
		<month>
		<day>
		<hour>
		<minute> (00, 10, 20, 30, 40, 50)

		data: {
		   rssi:{
		      average
		      times: [times] (seconds from YYYY/MM/DD HH:00:00)
		      values: [values]},
		   windd:{
		      average
		      times: [times]
		      values: [values]},
		   ... 
		   <for all data fields>

		meta: {
			batt: [],
			channel: [],
			raw: [],
			datetime: []
		}
	
	"""

	# all possible data fields the station will send
	data = ['rssi', 'windd', 'windv', 'uv', 'solar',
			'rain', 'rainsecs', 'temp', 'rh', 'windgust',
			'soilleaf', 'vcap', 'vsolar', 'fei', 'delta']
	# metadata to keep
	meta  = ['batt', 'channel', 'raw', 'datetime']

	# sample time
	stime = doc['datetime'] 
	# document index 
	doctime = datetime.datetime(stime.year, stime.month, stime.day, stime.hour, 
								minute=10*(stime.minute // 10), 
								second=0,microsecond=0, tzinfo=dateutil.tz.tzutc())
	sampletime = (stime - doctime).total_seconds()

	print("{} -- {}".format(stime, doctime))

	dbdoc={}
	dbdoc['year'] = doctime.year
	dbdoc['month']= doctime.month
	dbdoc['day'] = doctime.day
	dbdoc['hour'] = doctime.hour
	dbdoc['minute'] = doctime.minute

	doc_cur = col.find(dbdoc)

	if doc_cur.count() == 0:
		dbdoc['data'] = {}
		dbdoc['meta'] = {}
		for d in data:
			dbdoc['data'][d] = {}
			if d in doc:
				dbdoc['data'][d]['average'] = doc[d]
				dbdoc['data'][d]['min'] = doc[d]
				dbdoc['data'][d]['max'] = doc[d]
				dbdoc['data'][d]['values'] = [doc[d]]
				dbdoc['data'][d]['times'] = [sampletime]
			else:
				dbdoc['data'][d]['average'] = None
				dbdoc['data'][d]['min'] = None
				dbdoc['data'][d]['max'] = None
				dbdoc['data'][d]['values'] = []
				dbdoc['data'][d]['times'] = []
		for d in meta:
			dbdoc['meta'][d] = {}
			if d in doc:
				dbdoc['meta'][d]['values'] = [doc[d]]
				dbdoc['meta'][d]['times'] = [sampletime]
			else:				
				dbdoc['meta'][d]['values'] = []
				dbdoc['meta'][d]['times'] = []
		col.insert(dbdoc)
	else:
		dbdoc = doc_cur[0]
		dbpush = {}
		dbset = {}
		for d in data:
			if d in doc:
				dbpush['data.{}.values'.format(d)] = doc[d]
				dbpush['data.{}.times'.format(d)] = sampletime

				#dbdoc['data'][d]['values'].append(doc[d])
				#dbdoc['data'][d]['times'].append(sampletime)

				#dbdoc['data'][d]['average'] = numpy.average(dbdoc['data'][d]['values'])
				#dbdoc['data'][d]['min'] = min(dbdoc['data'][d]['values'])
				#dbdoc['data'][d]['max'] = max(dbdoc['data'][d]['values'])

				v = dbdoc['data'][d]['values']
				v.append(doc[d])
				dbset['data.{}.average'.format(d)] = numpy.average(v)
				dbset['data.{}.min'.format(d)] = min(v)
				dbset['data.{}.max'.format(d)] = max(v)
		for d in meta:
			if d in doc:
				#dbdoc['meta'][d]['values'].append(doc[d])
				#dbdoc['meta'][d]['times'].append(sampletime)

				dbpush['meta.{}.values'.format(d)] = doc[d]
				dbpush['meta.{}.times'.format(d)] = sampletime
		
		#pprint.pprint({'$push': dbpush, '$set': dbset})
		#col.update({'_id': dbdoc['_id']}, dbdoc)
		col.update({'_id':dbdoc['_id']}, {'$push': dbpush, '$set': dbset})


CONFIG_FILE = 'config.json'

if __name__=="__main__":
	

	if os.path.exists(CONFIG_FILE):
		with open(CONFIG_FILE, 'r') as f:
			config = json.load(f)
	else:
		print("Create the file {}.".format(CONFIG_FILE))
		exit(-12)

	db = MongoClient(config['mongo_url'],
					#socketTimeoutMS = 60000,
					connectTimeoutMS = 60000,
					socketKeepAlive = True,					
					tz_aware=True).get_default_database()
	col = db[config['mongo_db']]

	pws = pws_upload(config['wunderground_id'], config['wunderground_password'], rt=True)

	q = queue.Queue()
	try:
		port, baud = (config['serial_device'], 19200)
		ser = serial.Serial(port, baud, timeout=5)

	except serial.serialutil.SerialException:
		print("Failed to open port: {}".format(port))
		exit(-1)

	t = threading.Thread(name  = 'DavisRadioInputThread', 
						 target= getPackets,
						 args = (ser, q),
						 daemon= True)
	
	t.start()


	while True:
		try:
			d = q.get()[:-2].decode('utf-8') # strip cr lf

			now = datetime.datetime.now(dateutil.tz.tzlocal())


			print('got: [{:d}]: {:s}'.format(len(d), d))
			#decodePacket(d)

			if not d.startswith('raw:'):
				continue
				
			d = loadpacket(d)
						
			utc = now.astimezone(dateutil.tz.tzutc())
			d['datetime'] = utc

			pprint.pprint(d);

			try:
				# wunderground update
				print(pws.update(d, utc))

			except Exception:
				traceback.print_exc()
				

			for i in range (16):
				try:
					# push to database
					update(col, d)
					break
				except AutoReconnect:
					time.sleep(15 + (2**i))
					print("mongo connection failed, reconnect.")
			



		except KeyboardInterrupt:
			ser.close()
			break


	
	print("closing port.")
	t.join()	
	print("Goodbye.")