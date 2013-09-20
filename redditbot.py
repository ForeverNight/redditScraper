#!/usr/bin/python

###########################################
#
#   SCHEMA: SUBREDDIT (text)  LINK (text)  NSFW (boolean)  POSTER (text)  ID (text){PK} DONE (boolean)
#
############################################

import requests 			#do raw interaction with the internets
import praw					#python reddit api wrapper
import psycopg2				#postgres-python link
import mysql				#mysql-python link
import ConfigParser			#config loader for python
from multiprocessing import Process				#threading!
from time import sleep		#might just import all of time

headers = {
	'User-Agent': 'nightAgent by foreverNight v0.1'
	#'From': 'foreverNight@editmyconfigsbitch.net'
}

def getsubs(dbname, dbuser, dbpass, dbhost, dbport, subreddit):
	url = "http://www.reddit.com/r/" + subreddit +"/new/.json"
	conn = psycopg2.connect(
		database=dbname, 
		user=dbuser,
		password=dbpass,
		host=dbhost,
		port=dbport)
	cur = conn.cursor()
	latest = 0
	while True:

		r = requests.get(url, headers=headers)
		r.text
		rj = r.json()	

		for child in rj['data']['children']:
			
			if child['kind'] == "t3":
				if child['data']['latest'] > latest:
					try:
						arguments = (child['data']['subreddit'], child['data']['url'], child['data']['over_18'], child['data']['author'], child['data']['id'], False, )
						cur.execute("INSERT INTO scraper VALUES(%s, %s, %s, %s, %s, %s);", arguments)
						conn.commit()
					except psycopg2.IntegrityError, ie:
						conn.rollback()

		for child in rj['data']['children']:	#since cputime is cheap....
			if child['data']['created_utc'] > latest:	#this won't be optimized for a bit
				latest = child['data']['created_utc']		#and we can do this as inefficently as we can for now
		sleep(180)	#3 minute sleeper to let more people make and post content, maybe increase sleep time later

	cur.close()
	conn.close()

if __name__ == '__main__':
	cfgparser = ConfigParser.ConfigParser()
	cfgparser.optionxform = str;	
	cfgparser.read("scraper.conf")
	reddit_name = cfgparser.get('main', 'bot_login')
	reddit_pass = cfgparser.get('main', 'bot_password')
	subreddits = cfgparser.get('main', 'subreddits').split('\n')

	dbtype = cfgparser.get('main', 'database_type')
	dbname = cfgparser.get(dbtype, 'name')
	dbuser = cfgparser.get(dbtype, 'user')
	dbpass = cfgparser.get(dbtype, 'pass')
	dbhost = cfgparser.get(dbtype, 'host')
	dbport = cfgparser.get(dbtype, 'port')

	for sr in subreddits:
		if not sr:	#if it's not an empty string
			Process(target=getsubs, args=(dbname, dbuser, dbpass, dbhost, dbport, sr,)).start()
			sleep(2)	#so we don't scrape right away like crazy