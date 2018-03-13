# moviedb.template
This template shows example usage of the Metis Machine platform for the purpose of data ingestion and curation. Fundamentally, the task is to go out each morning and fetch a list of valid movie IDs from www.themoviebd.org (TMDb) and then retrieve additional data about each film (genres, release date, length, etc).

## Dependencies
1. User must sign up and aqcuire a free API key from TMDb.
  - Register Here --> https://www.themoviedb.org/account/signup
2. Set the API key as an environment variable with the skafos CLI.
  - Run from the terminal (in your project directory): `skafos env MOVIE_DB --set <API KEY>`
3. User must have git installed and a github account created --> https://git-scm.com/

## Project Structure
- *movies*
  - `__init__.py`
  - `constants.py`
  - `logger.py`
  - `movie_fetch.py`
  - `movie_info.py`
- *metis.config.yml*
- *environment.yml*
- *README.md*
- *main.py*

## Flow
- `movie_fetch.py` and `movie_info.py` contain the classes that handle the ingestion using TMDb API calls. Any methods can be expanded to retrieve more or less data as desired.
- `main.py` script is the primary driver for this task. At the end, a list of valid movie ID's and associated information will be written to a project keyspace using the Skafos Data Engine.
- `metis.config.yml` allows the user to configure project specific or runtime specific requirements (schedule, resources, run count, etc). See for more information here --> https://metismachine.readme.io/docs/deploying-tasks.
- Once the user is ready to fire it off (after following the dependency steps above):
  - CREATE: a github repository and attach it to the skafos app here --> https://github.com/apps/skafos
  - OPEN: one of the files and make some sort of change (add a comment, add new functionality, change config file, etc)
  - From Project Directory RUN:
  ```
    $ git init
    $ git add .
    $ git commit -am "<message>"
    $ git remote add origin git@github.com:<organization>/<repository-name>.git
   ```

## Options
These are the environment variables that could be set from the terminal in the project directory using the Skafos CLI.
`MOVIE_DB` and either `BACKFILLED_DAYS` or `FILE_DATE` are required and the others are optional. See below for details.

**required**
- `MOVIE_DB`: API key from TMDb. See [above](#Dependencies).

**at least one of these required**
- `BACKFILLED_DAYS`: Number of consecutive days in the past for which to fetch movie data.
- `FILE_DATE`, format=`%Y-%m-%d`: Single date for which to fetch movie data.

**optional**
- `POPULARITY`, default=15: Lower threshold on popularity score for a particular movie. Note: most films fall above 15.
- `BATCH_SIZE`, default=10:  Number of rows written to the database at a time.

## Deployment
Once the steps are completed above, the user has deployed their movie data ingester. To check the status of the build or job run `skafos logs --tail` to see realtime updates.

