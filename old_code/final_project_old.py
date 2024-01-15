import requests as rq

import pandas as pd
import numpy as np
from data_preping import prep_data
from movie_recommendation_system_data_preping import movie_recommendation_system_preping
from EDA import EDA

from PIL import Image
from io import BytesIO
from skimage import io
import cv2 as cv

import pickle
import streamlit as st

# https://www.kaggle.com/code/rounakbanik/movie-recommender-systems/notebook
# https://www.kaggle.com/code/rounakbanik/the-story-of-film/notebook
# https://developer.themoviedb.org/reference/discover-movie
# https://www.youtube.com/watch?v=1xtrIEwY_zY



# Using bytearray, np, opencv
def url_to_image_method_1(url):
	# Convert str to byte obj
	print(rq.get(url).content)
	img = bytearray(rq.get(url).content)
	#print(img) # check byte values

	# get 1D int arr for img
	img = np.asarray(img, dtype="uint8")
	
	# Decode img by opencv
	img = cv.imdecode(img, cv.IMREAD_COLOR)
	return img
# url = "https://image.tmdb.org/t/p/w500"+ "/r2J02Z2OpNTctfOSN1Ydgii51I3.jpg"
# img = url_to_image_method_1(url)
# cv.imshow('demo', img)
# cv.waitKey()
# cv.destroyAllWindows()


# Using PIL, io library for handling
def url_to_image_method_2(url):
	# Get binary from url
	img = rq.get(url).content
	# Convert string to byte object
	img = BytesIO(img)
	# print(img.getvalue()) # check byte values

	img = Image.open(img)
	img = np.asarray(img)
	img = cv.cvtColor(img, cv.COLOR_RGB2BGR)
	return img


def url_to_image_method_3(url):
	# Convert str to byte obj
	img = io.imread(url)
	# print(img) # check byte values
	# conver from rgb to bgr
	img = cv.cvtColor(img, cv.COLOR_RGB2BGR)
	return img


def dataset_intepretation() -> None:
	# This dataset was obtained through the TMDB API from 1882 to present day
	df = pd.read_csv('metadata_and_movie_detail.csv', lineterminator='\n')
	print(df.columns); print(); print()

	df.info(); print(); print()
	print("""Features
id: film id in tmdb
title: The Official Title of the movie.
genre_ids: id of genres
original_language: The language in which the movie was originally shot in.
release_date: Theatrical Release Date of the movie.
overview: A brief blurb of the movie.
popularity: The Popularity Score assigned by TMDB.
vote_average: The average rating of the movie.
vote_count: The number of votes by users, as counted by TMDB.
poster_path: The URL of the poster image.
backdrop_path: The URL of the backdrop image.
budget: The budget of the movie in dollars.
genres: A stringified list of dictionaries that list out all the genres associated with the movie.
homepage: The Official Homepage of the move.
revenue: The total revenue of the movie in dollars.
runtime: The runtime of the movie in minutes.
tagline: The tagline of the movie.
belongs_to_collection: A stringified dictionary that gives information on the movie series the particular film belongs to.
production_companies: A stringified list of production companies involved with the making of the movie.
production_countries: A stringified list of countries where the movie was shot/produced in."""); print()
	return None


def data_wrangling() -> None:
	df = pd.read_csv('metadata_and_movie_detail.csv', lineterminator='\n')
	'''
	We see that the majority of the movies have a recorded revenue of 0.
	This indicates that we do not have information about the total revenue for these movies.
	Although this forms the majority of the movies available to us,
	we will still use revenue as an extremely important feature going forward from the remaining movies.
	'''
	print('# of null in budget col ', df[df['revenue'].isnull()].shape[0])
	df['revenue'] = df['revenue'].replace(0, np.nan)
	

	'''
	The budget feature has some unclean values that makes Pandas assign it as a generic object.
	We proceed to convert this into a numeric variable and
	replace all the non-numeric values with NaN.
	Finally, as with budget, we will convert all the values of 0 with NaN
	to indicate the absence of information regarding budget.
	'''
	df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
	df['budget'] = df['budget'].replace(0, np.nan)
	print('# of null in budget col ', df[df['budget'].isnull()].shape[0])


	'''
	Construct return col for assessing an effectiveness of the movie
	return > 1 ->  profitable
	return = 1 -> break even

	value < 1 -> unprofitable
	'''
	df['return'] = df['revenue'] / df['budget']
	print('# of null in return col: ', df[df['return'].isnull()].shape[0])

	# Export
	df.to_csv('wrangled_metadata_and_movie_detail.csv', index=False)
	df.to_excel('wrangled_metadata_and_movie_detail.xlsx', index=False)
	return None



##############################################################
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def fetch_poster(movie_id):
	df = pd.read_csv('english_movie.csv', lineterminator='\n')
	url = "https://api.themoviedb.org/3/movie/{}?api_key=8265bd1679663a7ea12ac168da84d2e8&language=en-US".format(movie_id)
	headers = {"accept": "application/json",
			   "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"
			   }
	response = rq.get(url, headers=headers)
	json = response.json()
	poster_path = json['poster_path']
	full_path = "https://image.tmdb.org/t/p/w500/" + poster_path
	return full_path


def movie_recommendation_system(movie_name):
	df = pd.read_csv('preprocessed_movie_recommmendation_system.csv', lineterminator='\n').dropna().reset_index(drop=True).loc[:20000]
	
	cv = CountVectorizer(max_features=5000, stop_words='english')
	vectors = cv.fit_transform(df['tag']).toarray()
	# print(cv.get_feature_names_out())
	# print(vectors.shape) # 10516 different words

	# This movie recommendation sys is built on content-based approach which uses cosine similarity for calculation text similarity
	cos_similarity = cosine_similarity(vectors)
	# print(cos_similarity.shape) # 10516*10516

	index = df.loc[df['title'] == movie_name].index[0]
	
	distances = sorted(list(enumerate(cos_similarity[index])), reverse=True, key=lambda x: x[1])
	recommended_movie_names = []
	recommended_movie_posters = []


	for i in distances[1:6]:
		# fetch the movie poster
		movie_id = df.iloc[i[0]]['id']
		recommended_movie_posters.append(fetch_poster(movie_id))
		recommended_movie_names.append(df.iloc[i[0]]['title'])
	
	return recommended_movie_names, recommended_movie_posters









if __name__ == '__main__':
	# prep_data()
	# dataset_intepretation()
	# data_wrangling()
	# EDA()
	# movie_recommendation_system_preping()
	recommended_movie_names, recommended_movie_posters = movie_recommendation_system('Avatar')
	print(recommended_movie_names)
	print(recommended_movie_posters)


