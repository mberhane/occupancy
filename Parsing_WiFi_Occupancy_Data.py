# parsing WiFi Occupancy data
from datetime import datetime
import csv
from pymongo import MongoClient
import re
import collections
import math
#import datetime, calendar, time
#import urllib, json
import matplotlib.pyplot as plt
#from dateutil import tz
import os
import zipfile


#Unzips files into New folder and creates a single list of data from all the CSV files
directory = 'C:\Users\Michael\OneDrive\city'                    #path of the zip file
directory_1 = 'C:\Users\Michael\OneDrive\city\extracted'        #path of the extracted file
directory_2 = 'C:\Users\Michael\OneDrive\city\Clients Per Floor'
       
too_short = []

#delete occupancy and results files
if os.path.exists('C:\Users\Michael\OneDrive\city\Occupancy.csv'):
	os.remove('C:\Users\Michael\OneDrive\city\Occupancy.csv')
if os.path.exists('C:/Users/Michael/OneDrive/city/results.csv'):
	os.remove('C:/Users/Michael/OneDrive/city/results.csv')

for filename in os.listdir(directory):
	if filename.endswith(".zip"):
		zip_ref = zipfile.ZipFile(directory_2 + "\\" + filename, 'r')
		zip_ref.extractall(directory_1)
		zip_ref.close()
	else:
		continue

	data = []
	num = 0
	length = 0
	for filename in os.listdir(directory_1):
		if filename.endswith(".csv"):
			with open(directory_1+"\\"+filename) as f:

				raw_data = csv.reader(f)

				for row in raw_data:

					try:
						length = length + 1
						#NOTE: Some of the files say "EST" instead of "EDT"; will add functionaility to support both
						q = datetime.strptime(row[1], '%a %b %d %H:%M:%S EST %Y')
						

						if num == 2:

							day = q
							day = day.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
							print day
							  
							row[1] = q.time()                   
						
						num = num + 1
						z = re.findall('^UMD > ([0-9]+)', row[0])
						
						row[0] = z[0]
						
					except :
						
						continue
					
					data.append(row)

				



	#Gives number of buildings for which WiFi data is available

	lst1, lst2, lst3 = [[],[],{}]
	for row in data:
		try:

	   		lst1.append((row[0]))
	   		#lst2.append(row[4])
	   		#lst3[row[0]] = row[5]
	   	except : continue

	bldng_lst = ['001', '004', '006', '009', '011', '014', '021', '022', '029', '037', '039', '042', '044', '046', '141']
	#bldng_lst = ['014']
	floor_lst = sorted(set(lst2))

	wcount = 0 
	
	temp_dict, final_data = [{}, {}]
	for bldng_num in bldng_lst:
		for row in data:
			if row[0] == bldng_num:
				wcount = wcount + 1
				temp_dict[row[1]] = temp_dict.get(row[1], 0) + int(row[3])

		final_data[bldng_num] = temp_dict
		
		temp_dict = {}

	skip_list = []
	with open('C:\Users\Michael\OneDrive\city\Occupancy.csv', 'wb') as f:
		writer = csv.writer(f, delimiter= ',', lineterminator = '\n')
		for i in sorted(final_data.keys()):
			writer.writerow([i])
		
			if(len(final_data[i].items()) == 0):
				print("No data found for " + i + "!")
				skip_list.append(i)
				continue
			for j ,k in sorted(final_data[i].items()):
				writer.writerow([j, k])


		if(wcount == 0):
			print("No data found for any target buildings!")
			continue

	with open('C:\Users\Michael\OneDrive\city\Occupancy.csv', 'rb') as file:

		#let's try this with like, a less ambitious approach
		reader = csv.reader(file, delimiter = ',', lineterminator = '\n')
		count = 0
		occupancy = collections.OrderedDict()
		current = "begin"
		vals = []

		for x in reader:
			if (len(x) == 1):
				if(current == "begin"):
					current = x[0]
					vals = []
					continue
				else:
					occupancy[current] = vals
					current = x[0]
					vals = []
					continue
			else:
				vals.append(int(x[1]))



	#Now, the more fun part; the long list is dropped into chunks of 96 for actual results
	for building in occupancy.keys():
		length = len(occupancy[building])
		bigCount = 0
		it = 1
		vals = occupancy[building]
		result = []


		while(bigCount < length):
				
			step = length / float(96)
			cur = bigCount
			smallCount = 0
			avg = 0
			
			
			
			while(bigCount < (step * it)):

				avg = avg + vals[bigCount]
				bigCount = bigCount + 1
				smallCount = smallCount + 1

			#Finished adding to average
			
			avg = avg / smallCount 
			result.append(avg)
			it = it + 1
			

		occupancy[building] = result
		

	#Write the values to a new file, for dummies
	with open('C:/Users/Michael/OneDrive/city/results.csv', 'w') as f:
		writer = csv.writer(f, delimiter= ',', lineterminator = '\n')

		for building in occupancy.keys():
			lst = occupancy[building]
			writer.writerow([building])
			for i in range(0, 96, 1):
				writer.writerow([str(int(i / 4)) + ':' + str(i % 4 * 15), str(lst[i])])

	


	'''The next important part of the code will be to upload to the database. This will be done by creating a new 
	"document" for each building, entering a value for "day", then an array correlating to time and occupancy, uploaded 
	using pymongo to occupancy database. '''

	
	client = MongoClient('mongodb://dashboard:people@128.8.215.93:27017/occupancy')
	posts = client['occupancy']
	posts_c = posts['buildings']

	for building in occupancy.keys():
		lst = occupancy[building]
		occu = []
		occu = collections.OrderedDict()
		post = posts_c.find_one({'building':building})
		if(post == None):
			for i in range(0, 96, 1):
				time = (int(i / 4 * 100)) + (i % 4 * 15)
				occu[(str(time).zfill(4))] = lst[i]

			#create the post
			
			post = {
			
			'building' : str(building),
			'data' : [{'day' : day, 'data' : occu}]  
			}
	
			posts_c.insert_one(post)
		else:
			#post exists.
			for i in range(0, 96, 1):
				
				time = (int(i / 4 * 100)) + (i % 4 * 15)
				occu[(str(time).zfill(4))] = lst[i]
			#append to post, instead of making a new one. 
			#create document.
			doc = {'day' : day, 'data' : occu}
			post['data'].append(doc)
			#insert updated data into post
			posts_c.replace_one({'building': building}, post)


	for filename in os.listdir(directory_1):
		os.remove(directory_1 + "\\" + filename)




