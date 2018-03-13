import json
from time import sleep
import numpy as np
import requests
from datetime import datetime
from movies.constants import *
from skafossdk import DataSourceType

class MovieInfo(object):

  def __init__(self, api_key, log, retry=3, batch_size=50):
    """Class constructor for movie info ingester.
    :param api_key: str. API KEY retrieved from the MOVIE DATABASE.
    :param log: logger. Records user-defined messages to the Skafos logs.
    :param retry: int, default 3. Number of retries allowed for each GET request.
    :param batch_size: int, default 50. Number of rows written to the database at a time.
    """
    self.api_key = "?api_key=" + str(api_key)
    self.log = log
    self.retry = retry
    self.base_url = "https://api.themoviedb.org/3/movie/"
    self.lan = "&language=en-US"
    self.batch_size = batch_size

  def _get_movie_list(self, skafos):
    # Create a list of the most recent movies that don't have additional data yet
    self.log.info('Setting up view and querying movie list')
    skafos.engine.save(INFO_SCHEMA, []).result()
    res = skafos.engine.create_view(
      "list", {"table": "movie_list_pop_sorted"}, DataSourceType.Cassandra).result()
    res = skafos.engine.create_view(
      "info", {"table": "movie_info"}, DataSourceType.Cassandra).result()
    info_query = "SELECT DISTINCT(movie_id) from info"
    info_result = skafos.engine.query(info_query).result()
    if info_result["data"]:
      info = np.array([i.get('movie_id') for i in info_result["data"]])
    else:
      info = np.array([])

    movies_query = "SELECT DISTINCT(movie_id) from list"
    movies = np.array([key.get('movie_id') for key in skafos.engine.query(movies_query).result()['data']])
    self.movies = np.setdiff1d(movies, info, assume_unique=True)
    self.log.info("Ingested {} movies that haven't been ingested".format(len(self.movies)))

  def _build_request_url(self, movie_data):
    self.id = movie_data
    self.url = self.base_url + self.id + self.api_key + self.lan

  def _make_movie_api_request(self, movie_id, movie_url, retry):
    # GET request to retrieve movie data
    retries = 0
    while retries <= retry:
      try:
        self.log.info('Requesting movie info for movie {}'.format(movie_id))
        resp = requests.get(movie_url)
        resp.raise_for_status()
        break
      # Catch HTTP errors and return none
      except requests.exceptions.HTTPError as e:
        self.log.debug("{}".format(e))
        return None
      except Exception as e:
        self.log.debug("%s:\tFailed to make TMDB request for movie %s on try %s." %
                  (e, movie_id, retries))
        retries += 1
        if retries <= retry:
          self.log.info("\tTrying again")
          continue
        else:
          self.log.info("Max retries reached")
          return None
    return resp

  def _parse_response(self, raw):
    # Parse the response from the GET request
    json_data = json.loads(raw)
    genres = json_data.get('genres')
    if genres:
      genres = [str(g.get('id')) for g in genres]
    return {
      'movie_id': self.id,
      'imdb_id': json_data.get('imdb_id'),
      'movie_title': json_data.get('original_title'),
      'release_date': json_data.get('release_date'),
      'language': json_data.get('original_language'),
      'length': json_data.get('runtime'),
      'poster_path': json_data.get('poster_path'),
      'adult': json_data.get('adult'),
      'genres_id': genres or None,
      'description': json_data.get('overview')
    }

  def _convert_empty_values(self, row):
    # Convert empty or missing values in a row to NONE and check date format
    for key in row:
      if key == 'release_date':
        # Validate that the date string is correct format
        if self._validate_date(row.get(key)):
          continue
        else:
          row[key] = None
      elif row.get(key) == '':
        row[key] = None
      else:
        continue
    return row

  def _validate_date(self, date_text, date_format=None):
    # Validate a date string meets format expectations
    if not date_format:
      date_format = '%Y-%m-%d'
    try:
      datetime.strptime(date_text, date_format)
      return True
    except ValueError:
      return None

  def fetch(self, skafos):
    # Fetch additional data for movies
    self._get_movie_list(skafos)
    if len(self.movies) == 0:
      self.log.info('No new movies to go get data for.')
      return self
    self.log.info('Making requests to TMDB for movie info on {} movies'.format(len(self.movies)))
    self.info = []
    for movie in self.movies:
      self._build_request_url(movie)
      response = self._make_movie_api_request(self.id, self.url, self.retry)
      if not response:
        # Sleep to avoid getting banned!
        sleep(2)
        continue
      else:
        row = self._parse_response(response.content)
        cleaned_row = self._convert_empty_values(row)
        if cleaned_row:
          self.info.append(cleaned_row)
        sleep(0.35)
    self._write_data(skafos)
    return self

  def _write_batches(self, engine, logger, schema, data, batch_size):
    # Write batches of data to Skafos Data Engine
    for rows in self._batches(data, batch_size):
      res = engine.save(schema, list(rows)).result()
      logger.debug(res)

  def _write_data(self, skafos):
    # Save data out using the Skafos Data Engine
    if len(self.info) == 0:
      self.log.info('No data to write to cassandra. Try again when votes have been recorded')
      return self
    # Save out using the data engine
    self.log.info('Saving {} new movie info records to the data engine'.format(len(self.info)))
    self._write_batches(skafos.engine, self.log,
                        INFO_SCHEMA, self.info, self.batch_size)

  @staticmethod
  def _batches(iterable, n):
    """Divide a single list into a list of lists of size n.
    :param iterable: list or array-like. Object to be divided into n parts.
    :param n: int. Number of parts to divide iterable into.
    """
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
      yield list(iterable[ndx:min(ndx + n, batchLen)])
