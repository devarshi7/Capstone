# SpringBoard Capstone Project

## User created playlist/dynamic genre classification



### Dataset

I have used my own playlists from spotify to collect track information. There are 13 playlists, falling under genres of progressive house, house, some electronic and trance. 

### Features

Spotify/EchoNest provides [audio features](https://developer.spotify.com/documentation/web-api/reference/tracks/get-audio-features/) and [audio analysis](https://developer.spotify.com/documentation/web-api/reference/tracks/get-audio-analysis/) of tracks, where audio features are Spotify's custom designed measures like danceability, acousticness, speechiness etc and audio analysis includes pitch and timbre vectors.

#### Organization of code

* Cap_package : Helper functions to -
    *request user playlists and tracks using spotipy library 
    *requesting audio analysis and audio features of tracks using spotipy
    *extracting and filtering pandas dataframes converted from json objects
    *saving dataset locally
    
* Collect and Data Wrangling : Jupyter notebooks for -
    *Requesting and saving audio analysis and audio features of the dataset
    *EDA
   
* Model : Jupyter notebooks for modeling
