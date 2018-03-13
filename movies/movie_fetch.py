import os
import sys
import json
import gzip
import requests
from datetime import datetime, timedelta
from pytz import timezone
from movies.constants import *
from skafossdk import *

# Movie data class
class MovieData(object):
  """Fetch movie data from the MOVIE DATABASE: https://developers.themoviedb.org/3/getting-started/introduction"""
  def __init__(self, api_key, log, retry=3, backfilled_days=None, file_date=None, batch_size=50):
    """Class constructor for movie data ingester.
    :param api_key: str. API KEY retrieved from the MOVIE DATABASE.
    :param log: logger. Records user-defined messages to the Skafos logs.
    :param retry: int, default 3. Number of retries allowed for each GET request.
    :param backfilled_days: int, optional. Number of consecutive days in the past for which to fetch movie data.
    :param file_date: str ('%Y-%m-%d' format), optional. Single date for which to fetch movie data
    :param batch_size: int, default 50. Number of rows written to the database at a time.
    """
    self.api_key = api_key
    self.log = log
    self.retry = retry
    self.base_url = "https://api.themoviedb.org/3/movie/"
    self.tz = timezone('EST')
    self.today = datetime.now(self.tz)
    self.batch_size = batch_size
    if not backfilled_days:
      if not file_date:
        sys.exit("You must supply either backfilled_days or a file_date")
      else:
        fdate = datetime.strptime(file_date, "%Y-%m-%d")
        self.filenames = [self._create_filename(fdate.day, fdate.month, fdate.year)]
    elif not isinstance(int(backfilled_days), int):
      sys.exit("Backfilled days must be an integer >= 0")
    elif int(backfilled_days) < 0:
      sys.exit("Backfilled days must be >= 0")
    elif int(backfilled_days) == 0:
      # Create the url to make the request
      self.filenames = [self._create_filename(self.today.day, self.today.month, self.today.year)]
      print(self.filenames)
    else:
      self.backfilled_days = int(backfilled_days)
      self.filenames = self._create_filenames()

  def _create_filenames(self):
    # Create filenames over a range of dates
    for day in range(self.backfilled_days+1):
      prior_date = self.today - timedelta(days=day)
      yield self._create_filename(prior_date.day, prior_date.month, prior_date.year)

  def _create_filename(self, day, month, year):
    # Create filename from date parts
    day = str(day)
    month = str(month)
    year = str(year)
    if len(day) == 1:
      day = '0' + day
    if len(month) == 1:
      month = '0' + month
    date_obj = month + '_' + day + '_' + year
    filename = 'movie_ids_' + date_obj + '.json.gz'
    return filename

  def _make_movie_file_request(self, filename, retry):
    # GET request to retrieve movie data and store in file
    retries = 0
    while retries <= retry:
      try:
        url = "http://files.tmdb.org/p/exports/{}".format(filename)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, "wb") as f:
          for chunk in response.iter_content(chunk_size=128):
            if chunk:  # filter out keep-alive new chunks
              f.write(chunk)
        break
      # Catch HTTP errors and break the loop - indicative of some cnx problem
      except requests.exceptions.HTTPError as e:
        self.log.debug("{}".format(e))
        break
      # Otherwise retry on all other exceptions
      except Exception as e:
        self.log.debug("{}: Failed to make TMDB request on try {}".format(e, retries))
        retries += 1
        if retries <= retry:
          self.log.info("Trying again!")
          continue
        else:
          sys.exit("Max retries reached")

  def _parse_movie_file(self, json_record, filename):
    # Parse a single line from the json file
    data = json.loads(json_record)
    file_date = self._date_from_filename(filename)
    return {'movie_id': str(data['id']),
            'movie_title': data['original_title'].strip(),
            'popularity': data['popularity'],
            'ingest_date': str(file_date[2]) + "-" + str(file_date[0]) + "-" + str(file_date[1]),
            'adult': data.get('adult'),
            'video': data.get('video')}

  def _open_movie_file(self, filename):
    # Open the downloaded gzip file and map rows to a curated list
    if not os.path.isfile(filename):
      self.movies = []
    else:
      with gzip.open(filename, 'rb') as f:
        d = f.readlines()
        # For each movie, parse the record and store in a list
        self.movies = [self._parse_movie_file(line, filename) for line in d]
        self.log.info('Data found for {} movies!'.format(len(self.movies)))

  def _filter_popularity(self, pop):
    # Filter down movie list by popularity score
    self.movies = list(filter(lambda x: x['popularity'] >= pop, self.movies))

  def _remove_file(self, filename):
    try:
      os.remove(filename)
    except OSError:
      self.log.debug("Unable to remove file {}".format(filename))

  def fetch(self, skafos, filter_pop=None):
    """Fetch the daily movie export list from movie database, parse the data, filter on popularity,
       write out the data, and remove the file.
    :param skafos: Skafos. Instantiated Skafos object from the sdk
    :param filter_pop: int, optional. Lower threshold on popularity score for a movie to be written to database
    """
    self.log.info('Making request to TMDB for daily movie list export')
    for f in self.filenames:
      self.log.info('Retrieving movie file {}'.format(f))
      self._make_movie_file_request(f, self.retry)
      self._open_movie_file(f)
      # If a filter value is provided - use it
      if filter_pop:
        self._filter_popularity(filter_pop)
      # Write the data
      self._write_data(skafos)
      # Remove the file after processing rows to reduce memory needs
      self._remove_file(f)
    return self

  def _write_batches(self, engine, logger, schema, data, batch_size):
    # Write batches of data to Skafos Data Engine
    for rows in self._batches(data, batch_size):
      res = engine.save(schema, list(rows)).result()
      logger.debug(res)

  def _write_data(self, skafos):
    # Save data out using the Skafos Data Engine
    movie_count = len(self.movies)
    self.log.info('Saving {} movie records with the data engine'.format(movie_count))
    if movie_count == 0:
      pass
    else:
      self._write_batches(skafos.engine, self.log, MOVIE_SCHEMA, self.movies, self.batch_size)

  @staticmethod
  def _date_from_filename(filename):
    """Extract the date from a filename.
    :param filename: str ('movie_ids_YYYY_MM_DD.json.gz')
    """
    return filename.split("movie_ids_")[1].split(".json.gz")[0].split("_")

  @staticmethod
  def _batches(iterable, n):
    """Divide a single list into a list of lists of size n.
    :param iterable: list or array-like. Object to be divided into n parts.
    :param n: int. Number of parts to divide iterable into.
    """
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
      yield list(iterable[ndx:min(ndx + n, batchLen)])
