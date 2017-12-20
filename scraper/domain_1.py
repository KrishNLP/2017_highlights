import requests
from bs4 import BeautifulSoup
import random
import re
import json
from datetime import datetime

browser_agent = [ua.rstrip('" ').lstrip('useragent="') for ua in open('../mac_user_agents.txt', 'r').read().split('\n')]

#chars not entertained by url string
dissallowed_char = ['&']

def get_meta_description(soup):
	#basic meta get for in-domain company landing page
	allMetaTags = list(map(lambda mm: mm.attrs, soup.find_all('meta')))
	description = [mt['content'] for mt in allMetaTags if mt.get('property') == 'og:description']
	return description[0] if allMetaTags else []

def div_to_dict(d_, h_):

	"""
	Alligns html tags and generates useable dictionary of search result header data
	"""
	def clean(text):
		return re.sub(r'\n|\t','' , text) if text else ''

	follow_up_url = [a.get('href') for a in d_.find_all('a', limit = 1)][0]
	name, description = [clean(i.text) for i in h_.find_all(re.compile("h"))] 
	get_tags = ['desc' , 'value'] 
	desc, value = list(map(lambda t: d_.find_all('div',  {'class' : t}), get_tags)) 
	all_atrs = {clean(d.text).strip(':') : clean(v.text) for d,v in zip(desc, value)}
	get_tidy_company_record = {**all_atrs, **{'name' : name, 'description' : description, 'url' : follow_up_url}}
	return get_tidy_company_record

def grabMeta(verifiedURL, headers = ''):
	
	""" Standalone function scraping pre-validated company landing page

	"""
	response = requests.get(verifiedURL, headers = headers)
	if response.status_code == 200:
		toSoup = BeautifulSoup(response.content,'html.parser')
		parseForMeta = get_meta_description(toSoup) #relies solely on meta
		if parseForMeta:
			return parseForMeta

def source(company, locationCues, head_params = {'User-Agent' : random.choice(browser_agent)}, test = False):

	"""
	In-domain search
	"""
	t_str = '_'.join(str(datetime.now()).split(' '))
	t_stamp = t_str[:t_str.index('.')]
	parseCandidateCo = re.sub('|'.join(dissallowed_char), '', company.lower()) 
	model_url = '<MASKED>' + t_stamp + '&keyword[]=' + parseCandidateCo
	response = requests.get(model_url, headers=head_params)
	if response.status_code == 200:
		compilePattern = re.compile('|'.join(locationCues), re.I)
		jsonResponse = response.json()
		if jsonResponse.get('listcount') != 0:
			make_soup = BeautifulSoup(jsonResponse.get('pagecontent'), 'html.parser')
			descriptors = make_soup.find_all('div', {'class' : 'row'})
			data = make_soup.find_all('div',  {'class' : 'company-des'})
			if test:
				print (data)
			candidateUrl = []
			placeHolderMetadsc = ''
			for dsc, hd in zip(descriptors, data): #pairing headers with data (classes are independent)
				if candidateUrl == []:
					detailedMeta = div_to_dict(dsc, hd)
					locationCandidate = re.split(r'([^\s\w]|_)+', detailedMeta.get('Location'))[0]
					if locationCandidate:
						if re.search(compilePattern, locationCandidate) and detailedMeta.get('url'):
							# print ('Found regional match for %s' % company)
							placeHolderMetadsc = detailedMeta.get('description')
							candidateUrl.append(detailedMeta.get('url'))
			if candidateUrl:
				makeStaticURL = candidateUrl[0] + '?_escaped_fragment_='
				getDecription = grabMeta(verifiedURL = makeStaticURL, headers = head_params)
				if getDecription:
					return getDecription
				else:
					pass
			if placeHolderMetadsc:
				return placeHolderMetadsc
			else:
				return None


