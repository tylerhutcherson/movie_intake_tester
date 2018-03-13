INFO_SCHEMA = {
    "table_name": "movie_info",
    "options": {
        "primary_key": ["movie_id"],
    },
    "columns": {
        "movie_id": "text",
        "imdb_id": "text",
        "movie_title": "text",
        "release_date": "date",
        "language": "text",
        "length": "double",
        "poster_path": "text",
        "adult": "boolean",
        "genres_id": "set<text>",
        "description": "text"
    }
}

MOVIE_SCHEMA = {
    "table_name": "movie_list_pop_sorted",
    "options": {
        "primary_key": ["ingest_date", "popularity", "movie_id"],
        "order_by": ["popularity desc"]
    },
    "columns": {
        "movie_id": "text",
        "ingest_date": "date",
        "popularity": "float",
        "movie_title": "text",
        "adult": "boolean",
        "video": "boolean"
    }
}
