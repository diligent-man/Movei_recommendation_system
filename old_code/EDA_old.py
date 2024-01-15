import pandas as pd
import seaborn as sns
from plotly import express as px
from matplotlib import pyplot as plt
from wordcloud import WordCloud, STOPWORDS




def title_wordcloud():
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')
	df['title'] = df['title'].astype('str')
	df['overview'] = df['overview'].astype('str')
	
	title_corpus = ' '.join(df['title'])
	overview_corpus = ' '.join(df['overview'])

	title_wordcloud = WordCloud(stopwords=STOPWORDS, background_color='white', height=1000, width=1500).generate(title_corpus)
	plt.figure(figsize=(16,8))
	plt.title('Word cloud for movie title')
	plt.imshow(title_wordcloud)
	plt.axis('off')
	plt.show()

	overview_wordcloud = WordCloud(stopwords=STOPWORDS, background_color='white', height=1000, width=1500).generate(overview_corpus)
	plt.figure(figsize=(16,8))
	plt.imshow(overview_wordcloud)
	plt.title('Word cloud for movie overview')
	plt.axis('off')
	plt.show()
	return None


def genre_production_countries_visualization(target_col):
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')
	if target_col == 'genres':
		df = df['genres'].dropna(axis=0, how='any').reset_index(drop=True)
	elif target_col == 'production_countries':
		df = df['production_countries'].dropna(axis=0, how='any').reset_index(drop=True)

	# Remove comma
	df = df[:].str.replace("'", "")
	
	# Counting each genre
	genre_dict = dict()
	for i in range(len(df)):
		genre_lst = df[i][1: -1].split(', ')
		for genre in genre_lst:
			if genre == '':
				genre = 'Unidentified'
			if genre not in genre_dict.keys():
				genre_dict[genre] = 1
			else:
				genre_dict[genre] += 1
	
	if target_col == 'genres':
		genre_dict = dict(sorted(genre_dict.items(), key=lambda x: x[1], reverse=False))
	elif target_col == 'production_countries':
		genre_dict = dict(sorted(genre_dict.items(), key=lambda x: x[1], reverse=False)[-11:])
	print(genre_dict)

	# Plotting
	keys = list(genre_dict.keys())[:-1]
	vals = [genre_dict[k] for k in keys]

	fig, ax = plt.subplots(figsize=(18,16))
	ax = sns.barplot(x=vals, y= keys, hue=keys, orient='h', dodge=False, ax=ax, palette='turbo')
	
	for bars_group in ax.containers:
		ax.bar_label(bars_group, padding=3, fontsize=13)

	ax.legend(bbox_to_anchor=(1, .5), loc='center left')
	sns.despine()
	if target_col == 'genres':
		plt.title('Distribution of genres')
	elif targe_col == 'production_countries':
		plt.title('Distribution of production countries')
	plt.show()
	return None


def original_language_visualization() -> None:
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')['original_language']
	df = df.groupby(df.values).count().sort_values(ascending=False).iloc[0:11]
	
	sns.barplot(x=df.index, y=df[:])
	plt.ylabel('Number of films')
	plt.title('Number of films in top 10 languages')
	plt.show()
	return None


def release_date_visualization(time_obj, normalization=False) -> None: 
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv',  lineterminator='\n')
	# convert string to timestamp
	df =  pd.to_datetime(df['release_date'])
	if time_obj == 'year':
		df = pd.DataFrame(df.dt.year.groupby(df.dt.year).sum())
	elif time_obj == 'month':
		df = pd.DataFrame(df.dt.month.groupby(df.dt.month).sum())
	elif time_obj == 'day':
		df = pd.DataFrame(df.dt.day.groupby(df.dt.day).sum())
	if normalization:
		time_obj = df.apply(lambda x: (x - df.min())/ (df.max() - df.min()))

	df.rename(columns={'release_date':'Number of films each '+ time_obj}, inplace=True)
	
	# Plotting
	fig, ax = plt.subplots(figsize=(8, 6))

	ax = sns.barplot(x=df.index, y=df[df.columns[0]], dodge=False, ax=ax, palette='turbo')
	for bars_group in ax.containers:
		ax.bar_label(bars_group, padding=3, fontsize=9)
	ax.legend(bbox_to_anchor=(1, .5), loc='center left')	

	sns.despine()
	plt.xlabel(time_obj)
	plt.ylabel('# of movies')
	plt.title('Number of films each ' + time_obj)
	plt.show()
	return None


def popularity_vote_average_vote_count_visualization(column) -> None:
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')
	# Top 10 lowest popularity films 
	target_column = df.sort_values(by=[column], ascending=False)[0:11].reset_index()

	fig, ax = plt.subplots(figsize=(8,6))
	
	# set the number of rows and cols for our table
	rows = 10
	cols = 3
	
	# adding a bit of padding on bottom (-1), top (1), right (0.5)
	ax.set_ylim(-1, rows + 1)
	ax.set_xlim(0, cols + .5)
	
	# Add table rows
	for row in range(10):
		ax.text(x=.85, y=10-row-1, s=target_column.loc[row, 'title'], va='center', ha='left')
		ax.text(x=1.655, y=10-row-1, s=target_column.loc[row, column], va='center', ha='left')
		ax.text(x=2.22, y=10-row-1, s=target_column.loc[row, 'original_language'], va='center', ha='left')
	
	# Add column name
	
	ax.text(1.05, 9.75, 'Movie', weight='bold', ha='left', color='cyan')
	if column == 'popularity':
		name = 'Popularity'	
	elif column == 'vote_average':
		name = 'Vote Average'
	elif column	== 'vote_count':
		name = 'Vote count'
	
	ax.text(1.63, 9.75, name, weight='bold', ha='left', color='cyan')		
	ax.text(2.15, 9.75, 'Language', weight='bold', ha='left', color='cyan')

	# Add gridlines
	ax.plot([0.85, 2.5], [9.5, 9.5], lw='.5', c='red')
	ax.plot([1.5, 1.5], [10, -.2], lw='.5', c='red')
	ax.plot([2, 2], [10, -.2], lw='.5', c='red')


	ax.axis('off')
	ax.set_title('Top 10 films with the highest ' + name.lower(),
				 loc='center',
				 fontsize=15,
				 weight='bold',
				 color='magenta')
	plt.show()
	return None


def budget_revenue_visualization(sort_by='budget'):
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')
	# Remove comma and square bracket
	df['genres'] = df['genres'].str.replace("'", "")
	df['genres'] = df['genres'].str.replace("[", "")
	df['genres'] = df['genres'].str.replace("]", "")
	# remove null rows
	df = df.loc[:, ['title', 'release_date',  'genres', 'original_language', 'budget', 'revenue']].dropna()
	df['return'] = df['revenue'] / df['budget']
	df['return'] =df['return'].round(decimals=3)
	df = df.sort_values(by=sort_by, ascending=False).reset_index(drop=True)
	
	# Create table
	fig, ax = plt.subplots(figsize=(18,16))
	# set the number of rows and cols for our table
	rows = 10
	cols = 6
	# adding a bit of padding on bottom (-1), top (1), right (0.5)
	ax.set_ylim(-1, rows + 1)
	ax.set_xlim(0, cols + .5)
	# Add table rows
	for row in range(10):
		ax.text(x=0, y=10-row-1, s=df.loc[row, 'title'], va='center', ha='left')
		ax.text(x=1.655, y=10-row-1, s=df.loc[row, 'release_date'], va='center', ha='left')
		ax.text(x=2.22, y=10-row-1, s=df.loc[row, 'genres'], va='center', ha='left')
		ax.text(x=3.6, y=10-row-1, s=df.loc[row, 'original_language'], va='center', ha='left')
		ax.text(x=3.9, y=10-row-1, s=df.loc[row, 'budget'], va='center', ha='left')
		ax.text(x=4.47, y=10-row-1, s=df.loc[row, 'revenue'], va='center', ha='left')
		ax.text(x=5.1, y=10-row-1, s=df.loc[row, 'return'], va='center', ha='left')

	# Add column name
	ax.text(0.65, 9.75, 'Title', weight='bold', ha='left', color='cyan')
	ax.text(1.59, 9.75, 'Release date', weight='bold', ha='left', color='cyan')
	ax.text(2.8, 9.75, 'Genre(s)', weight='bold', ha='left', color='cyan')
	ax.text(3.55, 9.75, 'Lang', weight='bold', ha='left', color='cyan')
	ax.text(3.93, 9.75, 'Budget', weight='bold', ha='left', color='cyan')
	ax.text(4.57, 9.75, 'Revenue', weight='bold', ha='left', color='cyan')
	ax.text(5.1, 9.75, 'Return', weight='bold', ha='left', color='cyan')
	
	# Add gridlines
	# horizontal line
	ax.plot([0, 5.5], [9.5, 9.5], lw='.5', c='red')
	# vertical line
	ax.plot([1.5, 1.5], [10, -.2], lw='.5', c='red')
	ax.plot([2.1, 2.1], [10, -.2], lw='.5', c='red')
	ax.plot([3.45, 3.45], [10, -.2], lw='.5', c='red')
	ax.plot([3.8, 3.8], [10, -.2], lw='.5', c='red')
	ax.plot([4.37, 4.37], [10, -.2], lw='.5', c='red')
	ax.plot([5.03, 5.03], [10, -.2], lw='.5', c='red')
	
	ax.axis('off')
	ax.set_title('Correlation between budget and revenue',
				 loc='center',
				 fontsize=15,
				 weight='bold',
				 color='magenta')
	plt.show()
	return None


def runtime_visualization() -> None:
	df = pd.read_csv('wrangled_metadata_and_movie_detail.csv', lineterminator='\n')['runtime']

	# Chart of the whole dataset
	# fig = px.box(df, y='runtime', points="all")
	# fig.show()
	# Chart after removing outlier
	df = df.where(df <= 244)
	fig = px.box(df, y='runtime')# points="all")
	fig.show()

	fig = px.histogram(df, title='Runtime distribution')
	fig.show()
	return None


def EDA() -> None:
	title_wordcloud()
	genre_production_countries_visualization(target_col='genres')
	original_language_visualization()
	release_date_visualization('day')
	popularity_vote_average_vote_count_visualization(target_col='popularity')
	budget_revenue_visualization(sort_by='budget')
	runtime_visualization()
	return None