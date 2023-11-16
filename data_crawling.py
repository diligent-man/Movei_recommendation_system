import numpy as np
import pandas as pd
import requests as rq
import multiprocessing as mp
import time
# https://developer.themoviedb.org/reference/intro/getting-started
# API key: d21a8139891f4454bb72c094df982311
# API read access key: eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU
def multiprocessing(target_function, file_name, lower_bound, upper_bound) -> None:
	# spawn: start a new Python process. -> slow but safe
	# fork: copy a Python process from an existing process. -> fast but unsafe
	# Default on each platform:
	# 	+ Windows (win32): spawn
	# 	+ macOS (darwin): spawn
	# 	+ Linux (unix): fork
	start_method = mp.get_start_method()
	
	# Num of processes in each interval
	num_of_processes = 10
	step = 10000

	process_lst = []
	for i in range((upper_bound // step)+1):
		start = lower_bound + step*i
		end = start + step
		
		if end > upper_bound:
			end = upper_bound
		interval = abs(start - end)
		pivot = int(interval // num_of_processes)

		for j in range(0, interval//pivot):
			if start + pivot*(j+2) > end:
				p = mp.Process(target=target_function, args=(start + pivot*j, end, file_name))
				print('Start: {}, end: {}, from {} to {}'.format(start + pivot*j, end, start, end))
			else:
				p = mp.Process(target=target_function, args=(start + pivot*j, start + pivot*(j+1), file_name))
				print('Start: {}, end: {}, from {} to {}'.format(start + pivot*j, start + pivot*(j+1), start, end))
			process_lst.append(p)

		if end == upper_bound:
			break
			
	for process in process_lst:
		process.start()
	
	for process in process_lst:
	 	process.join()
	return None


def get_total_pages(year):
		# Query total pages for each year
		language = "language=en-US&"
		year = "year="+str(year)+"&"
		# Query every page in each year
		# page should be range from 1 to 500
		url = "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&"+language+year
		headers = {"accept": "application/json",
	    		   "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"}

		total_pages = rq.get(url, headers=headers).json()['total_pages']
		total_pages = min(total_pages, 500)
		return total_pages


def metadata_crawling(start, end, file_name):
	for year in range(start, end):
		total_pages = get_total_pages(year)

		for page in range(1, total_pages+1):
			language = "language=en-US&"
			url = "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&"+language+"year="+str(year)+"&"+"page="+str(page)
			headers = {"accept": "application/json",
					   "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"}
			
			response = rq.get(url, headers=headers)
			result = response.json()['results']
			df = pd.DataFrame(result)
			df.to_csv(file_name, mode='a', index=False, header=False)
			print('Finished page ' + str(page) + ' of year ' + str(year) + ' in ' + str(start) + ' and ' + str(end))
	return None


def movie_detail_crawling(start, end, file_name):
	global df

	for index in range(start, end):
		url = "https://api.themoviedb.org/3/movie/" +  str(df['id'][index]) + "?language=en-US"
		headers = {"accept": "application/json",
	 	 		   "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"}
		response = rq.get(url, headers=headers)
		result = response.json()

		cols = ['adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'id', 'imdb_id', 'original_language', 'original_title', 'overview', 'popularity', 'poster_path', 'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'status', 'tagline', 'title', 'video', 'vote_average', 'vote_count']
		
		df = pd.json_normalize(result)
		adult
		backdrop_path
		belongs_to_collection
		budget
		genres
		homepage
		id
		imdb_id
		original_language
		original_title
		overview
		popularity
		poster_path
		production_companies
		production_countries
		release_date
		revenue
		runtime
		spoken_languages
		status
		tagline
		title
		video
		vote_average
		vote_count


		
	# 	df.to_csv(file_name, mode='a', index=False, header=False)
	# 	print('Finished line: ' + str(index) + ' from ' + str(start) + ' to ' + str(end))
	# 	time.sleep(0.5)
	return None




if __name__ == '__main__':
	# cols = ["adult","backdrop_path","genre_ids","id","original_language","original_title","overview","popularity","poster_path","release_date","title","video","vote_average","vote_count"]
	# file_name = 'metadata.csv'
	# df = pd.DataFrame(columns=cols)
	# df.to_csv(file_name, mode='w', index=False)
	
	# multiprocessing(metadata_crawling, file_name, 1882, 2023)
	# df = pd.read_csv('metadata.csv', lineterminator='\n')
	# df = df.drop_duplicates(subset=['id'])
	# df.to_excel('metadata.xlsx', index=False)



	cols = ['adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'id', 'imdb_id', 'original_language', 'original_title', 'overview', 'popularity', 'poster_path', 'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'status', 'tagline', 'title', 'video', 'vote_average', 'vote_count']
	file_name = 'movie_detail.csv'
	df = pd.DataFrame(columns=cols)
	df.to_csv(file_name, mode='w', index=False)

	df = pd.read_csv('metadata.csv', lineterminator='\n')
	multiprocessing(movie_detail_crawling, file_name, 0, len(df)-1)