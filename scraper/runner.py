import pandas as pd
import json
import re
from mask import masked_domain
from mask import masked_domain
import csv
import random
import sys
sys.path.insert(0, '../../mongo_toolkit')
from q_to_csv import get_cursor
from itertools import chain
from pymongo import InsertOne, UpdateOne
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
import time
from pprint import pprint

def cueBulkWrite(cursor, q, batch_size):
	modified = 0
	writeConcern = ''
	try:
		results = cursor.bulk_write(q, ordered = False)
		results = results.bulk_api_result
		print (results)
		modified += results['nModified']
	except BulkWriteError as bwe:
		writeConcern = bwe.details
		modified += bwe['nModified']

def updateDoc(): #refresh instance
	updateDoc = 	{
						"descriptionFrom" : '',
						"description" :  '',
						'OldDescription' : '',
					}
	return updateDoc

def dotNotationGet(document, field):
	current = ''
	for item in field.split('.'):
		current = current.get(item) if current else document.get(item)
	return current

def pipeline(org, locations, methods = [], doTest = False):
	found = False
	foundMethod = False
	for m in methods:
		mfunc = eval(m) # dangerous method but appropriate for scripting
		if not found:
			found = mfunc(org, locations, test = doTest) 
			foundMethod = m
	return found, re.sub('source', '',foundMethod, re.I)

def generateGeographicKeys(sample): 
	# get geo values associated with location based on field structure
	locCues = []
	if isinstance(sample, dict):
		signatureLocationKeys = ['location.country.name.common', 'location.name']
		for slk in signatureLocationKeys:
			locCues.append(dotNotationGet(sample, slk))
	else:
		signatureLocationKeys = ['location', 'focusedCountry']
		locCues = [sample[x] for x in signatureLocationKeys]
	return locCues

def replaceVal(location, collection, assertkey = 'descriptionFrom', uniquekey = ['crunchbaseUUID'],\
				batch_size = 30, limit = False, locationHelpers = [],\
				projectkeys = [], update = False, printResponses = False): #pass several unique find keys if necessary
	

	if update:
		print ('Update set to True. Writing...')
	localeQuery = {"$or":[ {"location.name": location}, {"location": location}], assertkey : {"$exists" : False}}
	defaultProjected = {'_id' : 1, 'name' : 1, 'description' : 1, 'location' : 1, 'focussedCountry' : 1, 'scraperlog' :1}
	affirmProjected  = {**defaultProjected, **{k:1 for k in uniquekey + projectkeys}} 
	shellCursor = get_cursor(collection = collection)
	allRecords = shellCursor.find(localeQuery, affirmProjected)
	allRecords = list(allRecords)
	if allRecords:
		limitmsg = 'with limit: %s' % limit if limit else 'without limit'
		print ('Query matched with %s records. Beginning update sequence %s!\n' % (len(allRecords), limitmsg))
		start = time.time()
		first = allRecords[0]
		locationCues = generateGeographicKeys(first) + locationHelpers
		if locationCues:
			print ('Identified geographical search terms: %s' % str(locationCues))
			totalModified = 0
			bulkQueue = []
			allRecords =  allRecords if not limit else allRecords[:limit]
			for item in allRecords:
				item = dict(item)
				if len(bulkQueue) == batch_size and update is True:
					result = cueBulkWrite(shellCursor, bulkQueue, batch_size)
					totalModified += result
					bulkQueue = [] #refresh
				companyName = item.get('name')
				matchOn = {"name" : companyName, "_id" : item['_id']}
				foundReplacementDSC = False
				new = updateDoc()
				if assertkey not in item.keys(): #condition not necessary, query already matches docs without
					foundDSC, method = pipeline(companyName, locationCues, doTest = printResponses)
					if foundDSC:
						similaritytest = item.get('description')
						if similaritytest:
							similaritytest = similaritytest.lower().strip()
						else:
							similaritytest = 'void'
						if foundDSC.lower().strip() not in similaritytest:
							print ('%s found replacement description for %s!' % (method, companyName))
							foundReplacementDSC = foundDSC
							new['descriptionFrom'] = method
						else:
							print ('%s"s description is the same!' % companyName)
					else:
						print ('')
						# print ('No description candidate found for %s! Retaining CB default!' % companyName)
				if foundReplacementDSC:
					new['description'] = foundReplacementDSC
					new['OldDescription'] = item.get('description')
					print ('Proposed %s' % json.dumps({**new, **{'name' : companyName}}, indent =4))
					bulkQueue.append(UpdateOne(matchOn, {"$set" : new}))
			if bulkQueue and update is True:
				bulkremainder = cueBulkWrite(shellCursor, bulkQueue, len(bulkQueue))
				totalModified += bulkremainder
			print ('Modified %s / %s' % (totalModified, len(allRecords)))
			print ('\n', '-' * 50, '\n')
			print ('Completed in %s seconds' % str(int(time.time() - start)))



passParams = {
"assertkey" : "descriptionFrom" ,
"uniquekey" : ["crunchbaseUUID"], 
"batch_size" : 30, 
"limit" : False, 
"projectkeys" : [], 
"update" : True,
"locationHelpers" : ["Korea"],
 "printResponses" : False
 }

"""
Params
-----------

location = case sensitive
assertKey = field exclusion condition for update
uniqueKey = key least likely to be modified  - akin to _id and valuable for queries where string fields commonly fail w/ index conflicts
update = False: non-write run printing relevant document updates, True: Write 
responseTest = print responses codes
locationHelpers = manual add fields auto signatureKeys for regex match 

"""

replaceVal('seoul',collection = 'ShellImportCompanies', **passParams)

