from cap_package import SpotipyCollect as sc
import demoji
import numpy as np
import pandas as pd
from pandas import json_normalize
import random
import re
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


demoji.download_codes()


def spotipy_client_cred(client_id, client_secret):
    '''
    Creates server-to-server authentication token and returns spotipy object.
    Token automatically refreshes.
    '''
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    return sp


def filterby_keyword(keywords, ex_list, playlists):
    '''
    filter playlists based on keywords
    keywords : list of words to match
    ex_list  : list of words to exlcude/ not match
    playlists : list of users' playlists-
                list of tuples - (id, playlists name, description, ...(if other features exist))
    returns : filtered list of tuples
    '''
    kw_string = '\\W* |'.join(map(str, keywords))  # include non word characters and space to
    # match these as separate words that may contain special characters.

    ex_string = '|'.join(map(str, ex_list))

    filtered = []

    for user in range(len(playlists)):

        user_filpl = []

        for pl in playlists[user]:

            name = re.findall(kw_string, pl[1], flags=re.IGNORECASE)
            desc = re.findall(kw_string, pl[2], flags=re.IGNORECASE)

            if name or desc:
                ex_name = re.findall(ex_string, pl[1], flags=re.IGNORECASE)
                ex_desc = re.findall(ex_string, pl[2], flags=re.IGNORECASE)

                if not(ex_name or ex_desc):
                    user_filpl.append(pl)

        filtered.append(user_filpl)

    return filtered


def get_artist_name(pl_full_df):
    '''
    Inserts artists_name column containg
    names of artists in one string
    '''

    artists_list = []

    for a in pl_full_df.artists:

        track_artists = []
        for i in a:

            track_artists.append(i['name'])

        ta = ', '.join(map(str, track_artists))
        artists_list.append(ta)

    return artists_list


def get_df_analysis(spotipyUserAuth, tracks_df, segments=True, min_conf=0.5,
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

    tracks_name = list(tracks_df['name'])
    tracks_id = list(tracks_df['id'])
    artists_name = list(tracks_df['artists_name'])
    # track_analysis returns a list of dictionary
    tracks_analysis = sc.get_tracks_analysis(spotipyUserAuth, tracks_id)
    df_analysis = {}

    for name_, artists_name_, track_analysis in zip(tracks_name, artists_name, tracks_analysis):

        # remove any special characters from name (they may cause issues in filenaming)
        name_ = re.sub(r'[*|><:"?/]|\\', "", name_)
        name_ = demoji.replace(name_)
        artists_name_ = re.sub(r'[*|><:"?/]|\\', "", artists_name_)
        name_ = name_ + '-' + artists_name_[:3]
        df_analysis[name_] = sc.get_segments(track_analysis, segments=segments,
                                             min_conf=min_conf, min_dur=min_dur, tempo=tempo,
                                             sections=sections, beats=beats, bars=bars)
    return df_analysis


def get_public_playlists(sp, usernames, keys=('id', 'name', 'description')):
    '''
    Get public playlists of given users as a list of tuples of details
    defined by keys.

    sp : spotipy auth object
    usernames : List of usernames
    keys : information items to be fetched.
           Default - ('id', 'name', 'description')

    returns : list of users' playlist
    '''
    userpl_list = []
    for username in usernames:
        user_pl = []
        playlists = sp.user_playlists(username)

        while playlists:

            for i, playlist in enumerate(playlists['items']):
                user_pl.append(tuple([playlist[k] for k in keys]))
            if playlists['next']:
                playlists = sp.next(playlists)
            else:
                playlists = None
        userpl_list.append(user_pl)

    return userpl_list


def get_tracks(spotipyUserAuth, playlist_id, allCol=False, showkeys=False):
    '''
    Extract track info of all tracks in a playlist.

    spotipyUserAuth : spotipy object from 'spotipy_userauth' function.
    playlist_id : playlist id can be obtained from  'get_playlists'
                  or 'filtersort_playlists' function.
    allCol : Default False - Returns a dataframe with only track name and id.
             True - Returns a complete dataframe of track details
                    with all columns.
    showkeys : Prints all column names/keys of the complete dataframe

    Returns: Dataframe with track info (Default - name and id)
    '''
    track_lim = 100

    tracks = spotipyUserAuth.playlist_tracks(playlist_id)

    tracks_json = [
        tracks['items'][j]['track']
        for j in range(len(tracks['items']))
        if tracks['items'][j]['track']]

    tracks_df = json_normalize(tracks_json, sep='_')

    if tracks['total'] > track_lim:
        offset = track_lim
        tracks_dflist = [tracks_df]

        for i in range(int(tracks['total'] / track_lim)):

            tracks = spotipyUserAuth.playlist_tracks(playlist_id, offset=(i + 1) * offset)

            tracks_json = [
                tracks['items'][j]['track']
                for j in range(len(tracks['items']))
                if tracks['items'][j]['track']]

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


def get_tracks_df(sp, user_playlistIDs, rem_dup=True, allCol=False):
    '''
    Gets tracks from spotipfy API of the listed playlists and returns
    a single dataframe of tracks from all playlists

    playlistIDs : list of tuples - (user, playlist IDs)
    rem_dup : Default True. Remove duplicate entries of tracks
              if track name and artists' names match

    returns : dataframe fo tracks df
    '''
    tracks_df = []

    for u in user_playlistIDs:

        tr_df = get_tracks(sp, u[1], allCol=True)

        user_col = [u[0]] * len(tr_df)
        tr_df.insert(loc=len(tr_df.columns), column='user', value=user_col)

        tracks_df.append(tr_df)

    pl_full_df = pd.concat(tracks_df, ignore_index=True)

    artists_list = get_artist_name(pl_full_df)
    pl_full_df.insert(loc=0, column='artists_name', value=artists_list)

    if rem_dup:
        pl_full_df = pl_full_df.drop_duplicates(subset=['name', 'artists_name'], keep='first', ignore_index=True)

    if not allCol:
        pl_full_df = pl_full_df[['name', 'id', 'artists_name', 'user']]

    return pl_full_df


def uri_to_id(uri_list):
    '''
    Parses ID from playlist URI and returns list of playlists' ID.
    Playlist IDs get fed into get_tracks or tracks_analysis

    URI_list : list of playlist URIs
    returns : playlists' ID list
    '''
    id_list = [uri_list[i].split(':')[-1] for i in range(len(uri_list))]

    return id_list


def user_analysis(spotipyUserAuth, user, df, save=True, path=None, fn=0):
    '''
    Gets tracksanalysis for tracks under one user and saves them as parquet files.

    spotipyUserAuth : Spotipy auth object.
    '''
    if save:
        # Create user folder
        path_ = path.joinpath('{}'.format(user))
        path_.mkdir(exist_ok=True)
        # create sub folders for every chunk or n number of tracks in the user folder
        p = path_.joinpath('{}_{}'.format(user, fn))
        p.mkdir(exist_ok=True)

    # list of dataframe names in output
    df_names = ['tempo', 'segments', 'sections', 'beats', 'bars']

    df_analysis = get_df_analysis(spotipyUserAuth, df)

    for track, a in df_analysis.items():

        for k in range(len(a)):

            a[k].to_parquet(p.joinpath('{}_{}.parquet'.format(track, df_names[k])), engine='pyarrow')


def user_plid_pair(user_ids, playlists):
    '''
    Creates a list of username/user id and playlist id tuples.
    This will be used as the input for get_tracks_df

    user_ids : list of user ids
    playlists : list of tuples where 1st position has the playlist id
    '''
    paired = [x for x in zip(user_ids, playlists)]
    user_plid = [(u[0], x[0]) for u in paired for x in u[1]]

    return user_plid


# --------------------------------------------------------------------------------
#  Functions for reading and transforming dataset
# --------------------------------------------------------------------------------

def read_dataset(path_, segments=True, tempo=False):
    '''
    Read analysis(dataframes) dataset stored as parquet files.

    path_ : path to dataset directory
    segments : Boolean - True to read segments files
    tempo : Boolean - True to read tempo files. Default False
    return : a dict with key/value pairs for all playlists in the folder.
             Key : Name of the playlist (string)
             Value : a dict of track analysis dataframes of all tracks from the playlist
    '''
    dataset = []
    for pl in path_.iterdir():

        pl_info = [pl.name]

        if tempo:
            tempo_list = [(re.sub('_tempo.parquet', '', t.name), pd.read_parquet(t)) for t in pl.glob('*_tempo.parquet')]
            pl_info.append(tempo_list)

        if segments:
            segments_list = [(re.sub('_segments.parquet', '', s.name), pd.read_parquet(s)) for s in pl.glob('*_segments.parquet')]
            pl_info.append(segments_list)

        dataset.append(pl_info)

    return dataset


def split_columns(df, pitch_cols, timbre_cols):
    '''
    df : dataframe of track segments with columns of pitch vector and timbre vector
    pitch_cols : List of pitch column names to split in
    timbre_cols : List of timbre column names to split in

    return : Updated dataframe with only pitches and timbre elements columns
    '''

    new_seg = pd.DataFrame()
    new_seg[pitch_cols] = pd.DataFrame(df.pitches.tolist(), index=df.index)
    new_seg[timbre_cols] = pd.DataFrame(df.timbre.tolist(), index=df.index)

    return new_seg


def timbre_minmax_tr(track_seg):
    '''
    Get the min and max of timbre values for a track

    track_segs : dataframe of track segments containing columns - timbre_1 to timbre_12
    return : min and max values of timbre elements as lists
    '''

    timbre_ = ['timbre_{}'.format(i + 1) for i in range(12)]
    timbre_min = [track_seg[timb].min() for timb in timbre_]
    timbre_max = [track_seg[timb].max() for timb in timbre_]

    return timbre_min, timbre_max


def pop_timbre_minmax(timbre_mins, timbre_maxs):
    '''
    Evaluates population minimum and maximum values of all timbre elements.

    timbre_mins : list of minimum values of timbre elements, for all tracks
    timbre_maxs : list of maximum values of timbre elements, for all tracks

    return: minimum and maximum values of timbre elements over the whole dataset
    '''
    timbre_ = ['timbre_{}'.format(i + 1) for i in range(12)]
    pop_timbre_min = []
    pop_timbre_max = []
    for i in range(len(timbre_)):
        pop_timbre_min.append(np.array([t[i] for t in timbre_mins]).min())
        pop_timbre_max.append(np.array([t[i] for t in timbre_maxs]).max())

    return pop_timbre_min, pop_timbre_max


def minmax_scale(x, mins, maxs, a=-1, b=1):
    '''
    Scale vector x between -1 and 1.
    x : vector/ array to be scaled
    mins : vector/ array of min values
    maxs : vector/ array of max values
    a : lower bound of scaling. Default -1
    b : upper bound of scaling. Default 1
    '''
    a_ = a * (np.ones(len(x)))
    b_ = b * (np.ones(len(x)))
    rescaled_x = a_ + ((x - mins) * (b_ - a_)) / (maxs - mins)

    return rescaled_x


def transform_dataset(dataset, timbre_min, timbre_max, num_seg=50):
    '''
    Create input arrays to be fed into a model.

    dataset : list of track segmentsdataframes
    timbre_min : List or numpy array of minimums of timbre values over the whole dataset
    timbre_max : List or numpy array of maximums of timbre values over the whole dataset
    num_seg : Default : 50 - Number of segments to be taken for input.

    returns : data - list of tuples of input data arrays and (one-hot)encoded labels.
              categories - array  of unique playlist name/labels in the data.
              num_tracks -  number of tracks in each category/playlist
              Currently only using segment arrays consisting of sequences pitch array and timbre values flattened for input.
              Future edit should account for other features, i.e tempo and/or audio features from spotify.
    '''

    timbre_ = ['timbre_{}'.format(i + 1) for i in range(12)]

    timbre_min = np.array(timbre_min)
    timbre_max = np.array(timbre_max)

    data_arrays = []
    for df in dataset:

        # Scale timbre values
        df[timbre_] = df[timbre_].apply(minmax_scale, args=(timbre_min, timbre_max), axis=1)

        # filter index of n -> num_seg randomly chosen segments
        indx = random.sample(list(df.index), num_seg)
        df_ = df.loc[indx]

        # Convert dataframe to numpy array
        segments = df_.to_numpy()
        segments = np.hstack(segments)
        data_arrays.append(segments)

    return data_arrays
