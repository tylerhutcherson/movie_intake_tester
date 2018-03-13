import os
import sys
from movies.movie_fetch import MovieData
from movies.movie_info import MovieInfo
from movies.logger import get_logger
from skafossdk import *


# Grab some environment variables using os module
if 'MOVIE_DB' in os.environ:
  api_key = os.environ['MOVIE_DB']
else:
  sys.exit('Please save a movie database api key in your environment.')

# Any movie with a popularity score lower than 15 is likely irrelevant
if 'POPULARITY' in os.environ:
  pop = int(os.environ['POPULARITY'])
else:
  pop = 15

if 'BATCH_SIZE' in os.environ:
  n = int(os.environ['BATCH_SIZE'])
else:
  n = 10

if 'BACKFILLED_DAYS' in os.environ:
  bd = os.environ['BACKFILLED_DAYS']
else:
  bd = None

if 'FILE_DATE' in os.environ:
  fd = os.environ['FILE_DATE']
else:
  fd = None

# Initialize the skafos sdk
ska = Skafos()

# Fetch movie data and write to cassandra using the Skafos Data Engine
ingest_log = get_logger('movie-fetch')
daily_movie_update = MovieData(api_key, ingest_log, batch_size=n, backfilled_days=bd, file_date=fd).fetch(skafos=ska, filter_pop=pop)

# Fetch additional movie info data for all new movies and write to cassandra using the Skafos Data Engine
info_log = get_logger('movie-info')
movie_info = MovieInfo(api_key, info_log, batch_size=n).fetch(skafos=ska)
