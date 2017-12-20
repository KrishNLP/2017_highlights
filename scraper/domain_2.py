import requests
import random
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
from itertools import chain

def source(company_name, locationCues,  test = False):

	"""
	Get and filter results by location based on first candidate from searchResult only

	"""
	descriptionFields = ['description', 'pitch'] #retain order for priority
	query = "query=%s&hitsPerPage=30&page=0" % company_name # API params
	form_data = {"requests":[{"indexName":"companies","params": query}]}
	dataRequest = "https://219wx3mpv4-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%203.24.3%3BJS%20Helper%202.21.2&x-algolia-application-id=219WX3MPV4&x-algolia-api-key=b528008a75dc1c4402bfe0d8db8b3f8e"
	response = requests.post(dataRequest, data = json.dumps(form_data))

	compilePattern = re.compile('|'.join(locationCues), re.I)
	if response.status_code == 200:
		results = response.json()['results'][0]['hits']
		if results:
			best = results[0]
			if test:
				print (best.get('description'), best.get('entity_locations'))
			geographicals = best.get('entity_locations')
			if geographicals:
				locations = pd.DataFrame(geographicals) 
				entity_slug = best.get('entity_slug')
				useLocKeys = ['city_name', 'country_name']
				allLocations = list(chain.from_iterable(list(map(lambda x: list(locations[x]), useLocKeys))))
				allLocations = [ll for ll in allLocations if ll]
				if allLocations:
					try:
						if re.search(compilePattern, ' '.join(list(map(lambda x: x.lower(), allLocations)))):
							findPitch = best.get('pitch')
							findDescription = best.get('description')
							url = "<masked>" + entity_slug
							if findPitch or findDescription:
								if len(findDescription) > len(findPitch):
									return findDescription
								else:
									return findPitch
					except AttributeError:
						return None


