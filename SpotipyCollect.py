import datetime as dt
from dotenv import load_dotenv
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import numpy as np
import os
import pandas as pd
from pandas.io.json import json_normalize
from pathlib import Path
import re
import requests
import spotipy
import spotipy.util as util


load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
USERNAME = os.getenv('USERNAME')
SCOPE = 'playlist-read-private'

spotify = spotipy.Spotify(auth_manager=spotipy.SpotifyOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, scope=SCOPE, username=USERNAME))
# me = spotify.me()
# me.keys()


def spotipy_userauth2(username):
    '''
    Creates authorization token and returns spotipy object.
    util prompt does not refresh token and maybe deprecated in future.
    '''
    token = util.prompt_for_user_token(username=username, scope=SCOPE, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI)
    spotify = spotipy.Spotify(auth=token)

    return spotify


def spotipy_userauth(username):
    '''
    Creates authorization token and returns spotipy object.
    Token automatically refreshes.
    '''
    username = username
    spotify = spotipy.Spotify(auth_manager=spotipy.SpotifyOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, scope=SCOPE, username=username))

    return spotify


def extract_playlists(spotipyUserAuth, username):
    '''
    Extract user's playlists' details

    spotipyUserAuth : spotipy object from 'spotipy_userauth' function.
    username : username(string)

    Returns a list of dictionary containing details of individual playlists.
    '''
    playlists = spotipyUserAuth.user_playlists(username)
    playlistsdetails = playlists['items']

    return playlistsdetails


def playlists_id_url(playlistsdetails):
    '''
    Collects and returns lists of playlist names, IDs, URLs, 
    and number of tracks present in a playlist

    playlistsdetails : list of dictionary containing details of individual
                       playlist. Obtained from spotipy.user_playlists.

    Returns :  list of playlists' total tracks, url, and ids.
    '''

    pl_name = []  # Initiate playlist name list
    pl_id = []   # Initiate playlist id list
    pl_url = []  # Initiate playlist url list
    pltot_tracks = []  # Initiate playlist track count list

    playlistsURL = 'https://api.spotify.com/v1/playlists/'

    for i in range(len(playlistsdetails)):

        current_list = playlistsdetails[i]

        pl_name.append(current_list['name'])
        pl_id.append(current_list['id'])
        url = playlistsURL + current_list['id'] + '/tracks'
        pl_url.append(url)
        pltot_tracks.append(current_list['tracks']['total'])

    return pl_name, pl_id, pl_url, pltot_tracks


def get_pl_details(username):
    '''
    Get playlist details such as name, id, url and total tracks
    username : username (string)

    returns : lists of - all playlist names, all ids, all urls,
              and list of number of tracks in a playlist
    '''
    pl_details = extract_playlists(spotipyUserAuth, username)

    pl_name, pl_id, pl_url, pltot_tracks = playlists_id_url(pl_details)

    return pl_name, pl_id, pl_url, pltot_tracks


def extract_tracks(spotipyUserAuth, playlist_id, allCol=False, showkeys=False):
    '''
    Extract track info of all tracks in a playlist.

    spotipyUserAuth : spotipy object from 'spotipy_userauth' function.
    playlist_id : playlist id can be obtained from  'extract_playlists'
                  or 'filtersort_playlists' function.
    allCol : Default False - Returns a dataframe with only track name and id.
             True - Returns a complete dataframe of track details
                    with all columns.
    showkeys : Prints all column names/keys of the complete dataframe

    Returns: Dataframe with track info (Default - name and id)
    '''
    track_lim = 100

    tracks = spotipyUserAuth.playlist_tracks(playlist_id)

    tracks_json = [tracks['items'][j]['track'] for j in range(len(tracks['items']))]
    tracks_df = json_normalize(tracks_json, sep='_')

    if tracks['total'] > track_lim:
        offset = track_lim
        tracks_dflist = [tracks_df]

        for i in range(int(tracks['total'] / track_lim)):

            tracks = spotipyUserAuth.playlist_tracks(playlist_id, offset=(i + 1) * offset)
            tracks_json = [tracks['items'][j]['track'] for j in range(len(tracks['items']))]
            tracks_df_ = json_normalize(tracks_json, sep='_')
            tracks_dflist.append(tracks_df_)

        tracks_df = pd.concat(tracks_dflist, ignore_index=True)

    if allCol is False:
        df = tracks_df[['name', 'id']]
    else:
        df = tracks_df

    if showkeys is True:
        print('Info keys are :', tracks_df.columns)

    return df


def track_genre(spotipyUserAuth, album_ids):
    '''
    NOTE : Spotify API does not return ANY Genre information, just empty lists.
    spotipyUserAuth : spotipy object from 'spotipy_userauth' function.
    album_ids : list of album ids. If a single album id is provided, it needs to be wrapped
                in a list.
    Returns : List of tuples of Name and Genre of albums
    '''
    # To remove any repeating album ids
    album_ids = list(set(album_ids))
    album_ids.remove(None)

    tot_albums = len(album_ids)

    # Limit of number of albums spotipy takes in its method 'albums'
    album_lim = 20

    if tot_albums < album_lim:
        # Switch assignment so the next loop runs just once
        album_lim = tot_albums

    album_genre = []
    end_idx = 0

    for i in range(int(tot_albums / album_lim)):

        start_idx = end_idx
        end_idx = start_idx + album_lim

        album_details = spotipyUserAuth.albums(album_ids[start_idx: end_idx])['albums']
        [album_genre.append((album_details[j]['name'], album_details[j]['genres']))
         for j in range(album_lim) if len(album_details[j]['genres']) != 0]

    if tot_albums % album_lim != 0:

        album_details = spotipyUserAuth.albums(album_ids[end_idx:])['albums']
        [album_genre.append((album_details[j]['name'], album_details[j]['genres']))
         for j in range(len(album_ids[end_idx:])) if len(album_details[j]['genres']) != 0]

    return album_genre


def extract_tracks_analysis(spotipyUserAuth, tracksid, showkeys=False):
    '''
    spotipyUserAuth : spotipy object from 'spotipy_userauth' function.
    trackids : list of track ids.
    showkeys : Default False - prints dictionary keys
    returns : list of dictionaries containing track analysis
    '''
    tracks_analysis = [spotipyUserAuth.audio_analysis(tracksid[j])
                       for j in range(len(tracksid))]

    if showkeys is True:
        print(tracks_analysis[0].keys())

    return tracks_analysis


def track_anlaysis_to_df(trackid=None, spotipyUserAuth=None,
                         track_analysis=None):
    '''
    Convert track analysis dictionaries into dateframes -
    beats, bars, segments and sections.

    trackid : Spotify track id
    spotipyUserAuth : Spotipy auth object. Required if using track id
    track_analysis : Track analysis dictionary of a single track if trackid is not provided

    Returns : track overview (dictionary) and dataframes of
              beats, bars, segments and sections
    '''

    if trackid is not None:
        if spotipyUserAuth is None:
            raise TypeError('Need spotipy authorized object')

        track_analysis = extract_tracks_analysis(spotipyUserAuth, [trackid])[0]

    trackoverview = track_analysis['track']

    # We don't need tatums currently
    beats_df = json_normalize(track_analysis['beats'], sep='_')
    bars_df = json_normalize(track_analysis['bars'], sep='_')
    segments_df = json_normalize(track_analysis['segments'], sep='_')
    sections_df = json_normalize(track_analysis['sections'], sep='_')

    return trackoverview, beats_df, bars_df, segments_df, sections_df


def convert_time(secs):
    ''' COnverts seconds to mins. Format mm:ss '''

    if pd.isna(secs):
        return float('NaN')
    else:
        int_secs = int(secs)
        if int_secs is not 0:
            milisecs = int(round(secs % int_secs, 2) * 100)
        else:
            milisecs = int(round(secs, 2) * 100)

        minutes = int(int_secs / 60)
        seconds = int_secs % 60

        return '{:0>2d}:{:0>2d}:{:0>2d}'.format(minutes, seconds, milisecs)


def tracks_analysis_(spotipyUserAuth, playlist_id):
    '''
    spotipyUserAuth : Spotipy auth object.
    playlist_id : playlist id
    *user functions extract_tracks and extract_track_analysis used here.

    Returns : a list of tuples : (name of the track (string), tuple containing trackoverview (dictionary),
                            beats_df, bars_df, segments_df, sections_df)
    '''
    # extract_tracks returns a dataframe
    tracks_df = extract_tracks(spotipyUserAuth, playlist_id)
    tracks_name = list(tracks_df['name'])
    tracks_id = list(tracks_df['id'])

    # track_analysis returns a list of dictionary
    tracks_analysis = extract_tracks_analysis(spotipyUserAuth, tracks_id)
    analysis_dict = {}

    for name_, track_analysis in zip(tracks_name, tracks_analysis):

        # trackanalysis = track_analysis_to_df(track_analysis = track_analysis)
        analysis_dict[name_] = track_analysis

    return analysis_dict


def get_segments(track_analysis, segments=True, min_conf=0.5, min_dur=0.25, tempo=True,
                 sections=False, beats=False, bars=False):
    '''
    Get segments of tracks on a playlist with conditions on  minimum confidence
    and minimum duration of a segment. Since we are currently interested in tempo
    of a track we will be returning that value as well.

    trackanalysis: track analysis (dict) of a track (obtained from tracks_analysis dict)
    segments: Default True. False if segments dataframe is not needed
    min_conf: minimum confidence to include a segment (range 0-1)
    min_dur : minimum duration/length in secs to include a segment
    tempo: Default True. False if tempo value is not needed
    sections: Default False. True if sections dataframe needs to be returned
    beats: Default False. True if beats dataframe needs to be returned
    bars: Default False. True if bars dataframe needs to be returned

    Returns: (in this order) tempo and segments dataframe (sections_df, beats_df, bars_df  as asked)
              of a single track
    '''

    trackoverview, beats_df, bars_df, segments_df, sections_df = track_anlaysis_to_df(track_analysis=track_analysis)

    if tempo:
        tempo_df = pd.DataFrame({'tempo': [trackoverview['tempo']]})

    # Introducing start_minute column for more readability of start time in min:sec format
    start_minute = segments_df['start'].map(convert_time)
    segments_df.insert(1, 'start_minute', start_minute)
    segments_df_ = segments_df[(segments_df['confidence'] > min_conf) & (segments_df['duration'] > min_dur)]

    while len(segments_df_) < 100:
        min_conf = min_conf - 0.05
        min_dur = min_dur - 0.05
        segments_df_ = segments_df[(segments_df['confidence'] > min_conf) & (segments_df['duration'] > min_dur)]

    segments_df_ = segments_df_[['start', 'start_minute', 'duration', 'confidence', 'pitches', 'timbre']]

    # iterating over a boolean mask to collect what to output/return
    output = [b for a, b in zip(
              [tempo, segments, sections, beats, bars], [tempo_df, segments_df_, sections_df, beats_df, bars_df])
              if a]

    return output


def get_playlist_analysis(spotipyUserAuth, playlist_id, segments=True, min_conf=0.5,
                          min_dur=0.25, tempo=True, sections=False, beats=False, bars=False):
    '''
    spotipyUserAuth : Spotipy auth object.
    playlist_id : playlist id
    segments and tempo: Default True. False if not needed
    min_conf: minimum confidence to include a segment (range 0-1)
    min_dur : minimum duration/length in secs to include a segment
    sections/beats/bars: Default False. True if needs to be returned

    Returns : a dict with key/value pairs for all tracks in the playlist
                Keys: name of track
                Value: list containing tempo and segment dataframe of the track
                       (and sections/beats/bars if asked)
    '''

    tracks_df = extract_tracks(spotipyUserAuth, playlist_id)
    tracks_name = list(tracks_df['name'])
    tracks_id = list(tracks_df['id'])
    # track_analysis returns a list of dictionary
    tracks_analysis = extract_tracks_analysis(spotipyUserAuth, tracks_id)
    playlist_analysis = {}

    for name_, track_analysis in zip(tracks_name, tracks_analysis):

        # remove any special characters from name (they may cause issues in filenaming)
        name_ = re.sub(r'[*|><:"?/]|\\', "", name_)
        playlist_analysis[name_] = get_segments(track_analysis, segments=segments,
                                                min_conf=min_conf, min_dur=min_dur, tempo=tempo,
                                                sections=sections, beats=beats, bars=bars)
    return playlist_analysis


def get_folder_analysis(spotipyUserAuth, filsort_pl, segments=True, min_conf=0.5,
                        min_dur=0.25, tempo=True, sections=False, beats=False, bars=False):
    '''
    Here, we will be using filtered and sorted output. Future edit should take user
    playlist names and id.
    Returns: a dict with key/value pairs for all playlists in the folder.
             Key : Name of the playlist (string)
             Value : a dict of track analysis of all tracks from the playlist
             (Values are returned from get_playlist_analysis)
    '''

    folder_analysis = {}

    for p in filsort_pl:

        # remove any special characters from name (they may cause issues in filenaming)
        pl_name = re.sub(r'[*|><:"?/]|\\', "", p[1])
        folder_analysis[pl_name] = get_playlist_analysis(spotipyUserAuth, playlist_id=p[2],
                                                         segments=segments, tempo=tempo,
                                                         min_conf=min_conf, min_dur=min_dur,
                                                         sections=sections, beats=beats, bars=bars)

    return folder_analysis
