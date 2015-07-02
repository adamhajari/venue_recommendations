import sqlite3 as sql3
import requests
import json
import pandas as pd
import time
import configs

db_path = configs.db_path
sk_api_key = configs.sk_api_key
offline_mode = False

class VenueRec(object):
	def __init__(self):
		con = sql3.connect(db_path)
		self.venues = pd.read_sql("select * from venues",con)

	def get_gigography(self, sk_artist_id, page):
		url = 'http://api.songkick.com/api/3.0/artists/{}/gigography.json?order=desc&apikey={}&page={}'
		try:
			response = requests.get(url.format(sk_artist_id,sk_api_key,page))
		except:
			time.sleep(10)  # wait to retry
			response = requests.get(url.format(sk_artist_id,sk_api_key,page))
		events = json.loads(response.text)
		time.sleep(1)  # rate limit
		return events

	def get_venue_data(self, venue_id):
		url = 'http://api.songkick.com/api/3.0/venues/{}.json?apikey={}'
		try:
			response = requests.get(url.format(venue_id,sk_api_key))
		except:
			time.sleep(10)  # wait to retry
			response = requests.get(url.format(venue_id,sk_api_key))
		venue = json.loads(response.text)
		time.sleep(1)  # rate limit
		return venue

	def crawl_songkick(self, sk_artists):
		con = sql3.connect(db_path)
		df_existing_artists = pd.read_sql("select * from artists",con,index_col='id')['name'].to_dict()
		for i in range(0,len(sk_artists)):
			artist_id = sk_artists[i]
			if df_existing_artists.has_key(int(artist_id)):
				continue # if artist is in existing_artists no need to recrawl, go on to next
			artist_ids = []
			venue_ids = []
			billing_index = []
			dates = []
			artist_name = "Unknown"
			page = 1
			MAX_PAGE = 5
			try:
				events = self.get_gigography(artist_id, page)
			except:
				print("ERROR: unable to find profile for %s"%artist_id)
				continue
			while events['resultsPage']['results'].has_key('event'):
				for event in events['resultsPage']['results']['event']:
					for artist in event['performance']:
						if int(artist['artist']['id'])==int(artist_id):
							billing_index_tmp = artist['billingIndex']
							artist_name = artist['displayName']
							break
					if event['start']['date']<'2012-01-01':
						break
					artist_ids.append(artist_id)
					venue_ids.append(event['venue']['id'])
					dates.append(event['start']['date'])
					billing_index_tmp = None
					billing_index.append(billing_index_tmp)
				try:
					if len(events['resultsPage']['results']['event'])<events['resultsPage']['perPage'] or page>=MAX_PAGE:
						print("%s: %s (%s)"%(i,artist_name,artist_id))
						break
					if event['start']['date']<'2012-01-01':
						print("%s: %s (%s) %s %s"%(i, artist_name, artist_id, event['start']['date'], page))
						break
				except:
					print("%s: %s"%(i, artist_id))
					break
				page+=1
				events = self.get_gigography(artist_id, page)
			df = pd.DataFrame({"artist_id":artist_ids,"venue_id":venue_ids,"billing_index":billing_index, "start":dates}, 
					 columns=["artist_id", "venue_id", "start", "billing_index"])
			unique_columns = ['artist_id','venue_id','start']
			try:
				df.dropna(subset=unique_columns).drop_duplicates(subset=unique_columns).to_sql("concerts",con,if_exists="append",index=False)
			except:
				pass
			try:
				c = con.cursor()
				c.execute("INSERT OR IGNORE INTO artists (id, name) VALUES (?,?);", (artist_id, artist_name))
				con.commit()
			except:
				pass
		con.close()
		return 1

	def get_similar(self, sk_artist_id):
		con = sql3.connect(db_path)
		query = "select * from similar_artists where artist_id ={}"
		df_similar_artists= pd.read_sql(query.format(sk_artist_id),con)
		con.close()
		if len(df_similar_artists)>0 or offline_mode:
			return df_similar_artists.loc[0,'similar_artists'].split(',')
		url = 'http://api.songkick.com/api/3.0/artists/{}/similar_artists.json?apikey={}'
		try:
			response = requests.get(url.format(sk_artist_id,sk_api_key))
		except:
			time.sleep(10)  # wait to retry
			response = requests.get(url.format(sk_artist_id,sk_api_key))
		sim_artists = json.loads(response.text)
		sim_artist_ids = []
		for artist in sim_artists['resultsPage']['results']['artist']:
			sim_artist_ids.append(artist['id'])
		# store similar artists
		insert_query = "INSERT OR IGNORE INTO similar_artists (artist_id, similar_artists, count) VALUES (?,?,?);"
		con = sql3.connect(db_path)
		c = con.cursor()
		c.execute(insert_query, (sk_artist_id, ",".join(map(str,sim_artist_ids)), len(sim_artist_ids)))
		con.commit()
		con.close()

		time.sleep(1)  # rate limit
		return sim_artist_ids

	def get_venue_matches(self, sk_artist_id, num=20):
		"""sk_artist_id is a songkick artist id. num is the number of artists to compare against"""
		similar_artists = self.get_similar(sk_artist_id)
		if not offline_mode:
			self.crawl_songkick(similar_artists[:num]) # make sure all of our artists are in db
		con = sql3.connect(db_path)
		query = """select * from concerts 
					where artist_id in ({})
					and start>'2012-01-01'"""
		df_similar_venues = pd.read_sql(query.format(",".join(map(str,similar_artists))),con)
		con.close()
		return df_similar_venues

	def get_top_venues(self, sk_artist_id, num=20):
		df_similar_venues = self.get_venue_matches(sk_artist_id, num=num)
		top_venues = df_similar_venues['venue_id'].value_counts()
		return top_venues

	def store_venue(self, venue_id):
		venue_json = self.get_venue_data(venue_id)
		try:
			name = venue_json['resultsPage']['results']['venue']['displayName']
		except:
			name = None
			
		try:
			zipcode = venue_json['resultsPage']['results']['venue']['zip']
		except:
			zipcode = None
			
		try:
			country = venue_json['resultsPage']['results']['venue']['city']['country']['displayName']
		except:
			country = None
		try:
			state = venue_json['resultsPage']['results']['venue']['city']['state']['displayName']
		except:
			state = None
			
		try:
			city = venue_json['resultsPage']['results']['venue']['city']['displayName']
		except:
			city = None
			
		try:
			lat = venue_json['resultsPage']['results']['venue']['lat']
			lng = venue_json['resultsPage']['results']['venue']['lng']
		except:
			lat = None
			lng = None
			
		try:
			capacity = venue_json['resultsPage']['results']['venue']['capacity']
		except:
			capacity = None
		
		insert_query = """INSERT OR IGNORE INTO venues 
			(id, name, zip, city, state, country, lat, lng, capacity) 
			VALUES (?,?,?,?,?,?,?,?,?);"""
		con = sql3.connect(db_path)
		c = con.cursor()
		c.execute(insert_query, (venue_id, name, zipcode, city, state, country, lat, lng, capacity))
		print (venue_id, name, zipcode, city, state, country, lat, lng, capacity)
		con.commit()
		con.close()

	def get_venues(self, top_venues, num=20, exclude_recent=False, sk_artist_id=None):
		"""top_venues is a dict where the key is venue id and the value is a score. 
		num is the number of venues to return
		returns venue metadata and stores results in the venues table if they don't already exist"""
		top_venues_df = pd.DataFrame.from_dict(top_venues).reset_index()
		top_venues_df.columns = ["id","score"]
		if exclude_recent and sk_artist_id is not None:
			recent_venues = self.get_recent_venues(sk_artist_id)
			top_venues_df = top_venues_df.loc[ ~top_venues_df['id'].isin(recent_venues['id']) ,:].reset_index(drop=True)
		con = sql3.connect(db_path)
		venue_ids = []
		count=1
		venues = pd.read_sql("select * from venues",con)
		for venue_id in top_venues_df['id']:
			if venue_id not in venues['id'].tolist() and not offline_mode:
				self.store_venue(venue_id)  # store the venue data
				venues = pd.read_sql("select * from venues",con)  # reload the venues dataframe
			if venues.loc[venues['id']==venue_id,'country'].tolist()[0]=="US":
				venue_ids.append(venue_id)
				count+=1
				if count>num:
					break
			
		query = "select id, name, city, state, lat, lng, capacity from venues where country='US' and id in ({})".format(",".join(map(str,venue_ids)))
		venues = pd.read_sql(query,con)
		return venues.merge(top_venues_df).sort("score",ascending=False)

	def get_recent_venues(self, sk_artist_id):
		con = sql3.connect(db_path)
		query = """select venue_id AS id from concerts where artist_id in ({}) and start>'2012-01-01'"""
		recent_venues = pd.read_sql(query.format(sk_artist_id),con)
		con.close()
		return recent_venues

	def get_artist_name(self, sk_artist_id):
		con = sql3.connect(db_path)
		query = """select name from artists where id={}"""
		name_df = pd.read_sql(query.format(sk_artist_id),con)
		if len(name_df)<1:
			self.crawl_songkick( [sk_artist_id] )
			name_df = pd.read_sql(query.format(sk_artist_id),con)
		con.close()
		return name_df.loc[0,'name']

	def get_artists_with_similar(self):
		con = sql3.connect(db_path)
		query = """select distinct a.id, a.name from artists a
					inner join similar_artists sa
					on a.id=sa.artist_id
					where sa.count>=20 order by a.id limit 30;"""
		artists = pd.read_sql(query,con)
		con.close()
		return artists




