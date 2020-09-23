
'''
 Future edit should remove track_genre and tracks_analysis

 Function definitions : spotipy_userauth, get_playlists, playlists_id_url
                        get_pl_details, get_tracks, track_genre,
                        get_tracks_analysis, track_anlaysis_to_df,
                        convert_time, tracks_analysis, get_segments,
                        get_playlist_analysis, get_folder_analysis,
                        create_dataset, uri_to_id

Hierachy:
- spotipy_userauth
- create_dataset > arg(get_folder_analysis)
                 > get_playlist_analysis
                 > get_segments > arg(get_tracks_analysis > arg(get_tracks))

  - get_segments > arg(track_analysis), track_analysis_to_df, convert_time

    - track_analysis_to_df > arg(track_analysis) or > get_track_analysis(arf=g(track_id))

  -get_track_analysis > arg(tracksid), spotipy.audio_analysis
  -get_tracks > arg(playlist_id), spotipy.playlist_tracks

Using USER's playlist: get_pl_details >  playlist_id_url > arg(get_playlists,)

Reduntant - tracks_analysis, track_genre

'''
import demoji
import pandas as pd
from pandas import json_normalize
import re
import spotipy
import spotipy.util as util

demoji.download_codes()


def spotipy_userauth2(username, scope, client_id, client_secret, redirect_uri):
    '''
    Implements Authorization Code Flow for Spotify’s OAuth implementation.

    Util prompt does not refresh token and maybe deprecated in future.

    Parameters
    ----------
    username : str
        username of current client
    scope : str
        the desired scope of the request
    client_id : str
        client id of the app
    client_secret : str
        client secret of the app
    redirect_uri : str
        redirect URI of the app


    Returns
    -------
    sp : spotipy object
        spotipy object with access to all Spotify Web API endpoints
    '''
    token = util.prompt_for_user_token(username=username, scope=scope, client_id=client_id,
                                       client_secret=client_secret, redirect_uri=redirect_uri)
    sp = spotipy.Spotify(auth=token)

    return sp


def spotipy_userauth(username, scope, client_id, client_secret, redirect_uri):
    '''
    Implements Authorization Code Flow for Spotify’s OAuth implementation.

    Token automatically refreshes.

    Parameters
    ----------
    username : str
        username of current client
    scope : str
        the desired scope of the request
    client_id : str
        client id of the app
    client_secret : str
        client secret of the app
    redirect_uri : str
        redirect URI of the app

    Returns
    -------
    sp : spotipy object
        spotipy object with access to all Spotify Web API endpoints
    '''
    username = username
    spotify = spotipy.Spotify(
        auth_manager=spotipy.SpotifyOAuth(
            username=username, scope=scope, client_id=client_id,
            client_secret=client_secret, redirect_uri=redirect_uri))
    return spotify


def get_playlists(spotipyUserAuth, username):
    '''
    Extract user's playlists' details

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    username : str
        username of the user

    Returns
    -------
    playlistdetails : List[Dict]
        lists of dictionary containing details of individual playlists.
    '''
    playlists = spotipyUserAuth.user_playlists(username)
    playlistsdetails = playlists['items']

    return playlistsdetails


def playlists_id_url(playlistsdetails):
    '''
    Collects and returns lists of playlist names, IDs, URLs,
    and number of tracks present in a playlist.

    Parameters
    ----------
    playlistsdetails : List[Dict]
        details of individual playlist obtained from spotipy.user_playlists endpoint.

    Returns
    -------
    pl_name, pl_id, pl_url, pltot_tracks : List
        list of playlists' name, ids, url, and total number of tracks in a playlist.
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


def get_pl_details(spotipyUserAuth, username):
    '''
    Get playlist details such as name, id, url and total tracks.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    username : str
        username of the user

    Returns
    -------
    pl_name, pl_id, pl_url, pltot_tracks : List
        list of playlists' name, ids, url and total number of tracks in a playlist.
    '''
    pl_details = get_playlists(spotipyUserAuth, username)

    pl_name, pl_id, pl_url, pltot_tracks = playlists_id_url(pl_details)

    return pl_name, pl_id, pl_url, pltot_tracks


def filtersort_playlists(pl_name, pl_id, pl_url, pltot_tracks, key_words=None, start=0, pl_range=10):
    '''
    Filters playlists.

    Filters based on provided keywords present in the name of the playlists or the
    first n playlists present in the range provided.
    Sorts ascending by total number of tracks in a playlist.
    Playlist name, id, url and total tracks to be passed are
    returned from the ******'playlist_id_url'***** function.

    pl_name, pl_id, pl_url : str
        playlist name, id and url respectively
    pltot_tracks : int
        total number of tracks
    Key_words : List[str]
        List of genre/terms to filter. Default None.
    pl_range : int, optional
        First 'n' number of playlists. Default 10

    Returns
    -------
    sorted_pl: List[tuple]
        list of filtered and sorted tuples (playlist name, id, url and # of tracks).
    '''
    fil_pl_name = []  # Initiate filtered playlist name list
    fil_pltot_tracks = []  # Initiate filtered total tracks list
    fil_pl_id = []  # Initiate filtered playlist ID list
    fil_pl_url = []  # Initiate filtered playlist URL list

    if key_words is not None:

        for i in range(len(pl_name)):

            name = pl_name[i]
            if any(word in name for word in key_words):
                fil_pl_name.append(name)
                fil_pltot_tracks.append(pltot_tracks[i])
                fil_pl_id.append(pl_id[i])
                fil_pl_url.append(pl_url[i])
    else:

        for i in range(start, pl_range):

            fil_pl_name.append(pl_name[i])
            fil_pltot_tracks.append(pltot_tracks[i])
            fil_pl_id.append(pl_id[i])
            fil_pl_url.append(pl_url[i])

    sorted_pl = sorted(zip(fil_pltot_tracks, fil_pl_name, fil_pl_id, fil_pl_url), reverse=True)

    return sorted_pl


def get_artist_name(tracks_df):
    '''
    Inserts artists_name column containg names of artists in one string.

    Parameters
    ----------
    tracks_df : pandas.DataFrame
        Index : RangeIndex
        Columns :
            Name : artists, dtype : str
            Name : name, dtype : str
        track_df should atleast contain the columns 'name' and 'artists'.

    Returns
    -------
    artists_list : List[str]
        List of concatenated artists' names for a track
    '''

    artists_list = []

    for a in tracks_df.artists:

        track_artists = []
        for i in a:

            track_artists.append(i['name'])

        ta = ', '.join(map(str, track_artists))
        artists_list.append(ta)

    return artists_list


def get_tracks(spotipyUserAuth, playlist_id, allCol=False, showkeys=False):
    '''
    Extract track info of all tracks in a playlist.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    playlist_id : str
        playlist id can be obtained from  'get_playlists' or 'filtersort_playlists' function.
    allCol : bool, optional
        Default False - Returns a dataframe with only track name, artists names and id.
                True - Returns a complete dataframe of track details
                        with all columns.
    showkeys : bool, optional
        True - Prints all column names/keys of the complete dataframe
        Default False

    Returns
    -------
    df : pandas.Dataframe
        Index : RangeIndex
        (Relevant) Columns :
                    Name : artists_name, dtype : str
                    Name : name, dtype : str
                    Name : id, dtype : str
    See https://developer.spotify.com/documentation/web-api/reference/playlists/get-playlists-tracks/ 
    for more information on returned track object columns/keys
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

    artists_list = get_artist_name(tracks_df)
    tracks_df.insert(loc=0, column='artists_name', value=artists_list)

    if allCol is False:
        df = tracks_df[['name', 'id', 'artists_name']]
    else:
        df = tracks_df

    if showkeys is True:
        print('Info keys are :', tracks_df.columns)

    return df


def track_genre(spotipyUserAuth, album_ids):
    '''
    Get track genre info from parent album genre

    NOTE : Spotify API does not return ANY Genre information in most cases, just empty lists.
           Use this function only for checking and experimenting.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    album_ids : List[str]
        list of album ids. If a single album id is provided, it needs to be wrapped in a list.

    Returns
    -------
    album_genre : List[tuple(str,str)]
        List of tuples of Name and Genre of albums
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

# --------------------------------------------------------------------------------
#  Functions for retrieving track analysis of the dataset
# --------------------------------------------------------------------------------


def get_tracks_analysis(spotipyUserAuth, tracksid, showkeys=False):
    '''
    Fetches track analysis of tracks

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    trackids : List[str]
        list of track ids.
    showkeys : bool, optional
        Default False. True - prints dictionary keys

    Returns
    -------
    tracks_analysis : List[Dict]
        list of dictionaries containing track analysis
    '''
    tracks_analysis = [spotipyUserAuth.audio_analysis(tracksid[j])
                       for j in range(len(tracksid))]

    if showkeys is True:
        print(tracks_analysis[0].keys())

    return tracks_analysis


def tracks_analysis(spotipyUserAuth, playlist_id):
    '''Fetches track analysis for tracks in a playlist

    User functions get_tracks and get_track_analysis used here.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    playlist_id : str

    Returns
    -------
    analysis_dict : Dict
         key - track name (str) : value - track analysis(json object)
    '''
    # get_tracks returns a dataframe
    tracks_df = get_tracks(spotipyUserAuth, playlist_id)
    tracks_name = list(tracks_df['name'])
    tracks_id = list(tracks_df['id'])

    # track_analysis returns a list of dictionary
    tracks_analysis_ = get_tracks_analysis(spotipyUserAuth, tracks_id)
    analysis_dict = {}

    for name_, track_analysis in zip(tracks_name, tracks_analysis_):

        analysis_dict[name_] = track_analysis

    return analysis_dict


def track_anlaysis_to_df(trackid=None, spotipyUserAuth=None, track_analysis=None):
    '''
    Convert track analysis dictionaries into dateframes -
    beats, bars, segments and sections.

    Parameters
    ----------
    spotipyUserAuth : spotipy object, optional
        Default - None. Or returned by 'spotipy_userauth' function.
    trackid : str, optional
        Spotify track id
    track_analysis : Dict, optional
        Default - None. Or track analysis dictionary of a single track if trackid is not provided

    Either need to provide track ids or track analysis for a single track

    Returns
    -------
    trackoverview : Dict
        track info.

    beats_df, bars_df, segment_df, sections_df : pandas.DataFrame
        Audio Analysis returned from spotify is converted from json to a dataframe for more readability
        and for ease of transformation.

    Note
    ----
    See https://developer.spotify.com/documentation/web-api/reference/tracks/get-audio-analysis/
    '''

    if trackid is not None:
        if spotipyUserAuth is None:
            raise TypeError('Need spotipy authorized object')

        track_analysis = get_tracks_analysis(spotipyUserAuth, [trackid])[0]

    trackoverview = track_analysis['track']

    # We don't need tatums currently
    beats_df = json_normalize(track_analysis['beats'], sep='_')
    bars_df = json_normalize(track_analysis['bars'], sep='_')
    segments_df = json_normalize(track_analysis['segments'], sep='_')
    sections_df = json_normalize(track_analysis['sections'], sep='_')

    return trackoverview, beats_df, bars_df, segments_df, sections_df


def convert_time(secs):
    ''' Converts seconds to mins. Format mm:ss

    Parameters
    ----------
    secs : int or Null

    Returns
    -------
    mm : ss : ms : str
        time as a string in the above format
    '''

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


def get_segments(track_analysis, segments=True, min_conf=0.5, min_dur=0.25, tempo=True,
                 sections=False, beats=False, bars=False):
    '''
    Get segments of tracks on a playlist with conditions.

    Restrictions on  minimum confidence and minimum duration of a segment can be set.

    Parameters
    ----------
    track_analysis: Dict
        track analysis of a track (obtained from tracks_analysis dict)
    segments: bool, optional
        Default True. False if segments dataframe is not needed
    min_conf: Float
        minimum confidence to include a segment (range 0-1)
    min_dur : Float
        minimum duration/length in secs to include a segment.
        Segments tend to be of very small time intervals, most under a second.
    tempo: bool, optional
        Default True. False if tempo value needs to be returned
    sections: bool, optional
        Default False. True if sections dataframe needs to be returned
    beats: bool, optional
        Default False. True if beats dataframe needs to be returned
    bars: bool, optional
        Default False. True if bars dataframe needs to be returned

    Returns
    -------
    output : List[pandas.DataFrame]
    For a single track (in this order) - tempo and segments dataframe
    sections_df, beats_df, bars_df  as required
    '''

    trackoverview, beats_df, bars_df, segments_df, sections_df = track_anlaysis_to_df(track_analysis=track_analysis)

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
    Gets audio analysis for all tracks in a playlist.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    playlist_id : str
        Spotify playlist id
    segments: bool, optional
        Default True. False if segments dataframe is not needed
    min_conf: Float
        minimum confidence to include a segment (range 0-1)
    min_dur : Float
        minimum duration/length in secs to include a segment.
        Segments tend to be of very small time intervals, most under a second.
    tempo: bool, optional
        Default True. False if tempo value needs to be returned
    sections: bool, optional
        Default False. True if sections dataframe needs to be returned
    beats: bool, optional
        Default False. True if beats dataframe needs to be returned
    bars: bool, optional
        Default False. True if bars dataframe needs to be returned

    Returns
    -------
    playlist_analysis : Dict
        Keys: name of track from playlist (str)
        Value: List containing tempo and segment dataframe
               (and sections/beats/bars if asked)of the track
               Values here are returned from get_segments
    '''
    playlist_analysis = {}
    tracks_df = get_tracks(spotipyUserAuth, playlist_id)
    tracks_name = list(tracks_df['name'])
    tracks_id = list(tracks_df['id'])
    tracks_artist = list(tracks_df['artists_name'])
    # track_analysis returns a list of dictionary
    tracks_analysis = get_tracks_analysis(spotipyUserAuth, tracks_id)

    for name_, track_artist, track_analysis in zip(tracks_name, tracks_artist, tracks_analysis):

        # remove any special characters from name (they may cause issues in filenaming)
        track_artist = re.sub(r'[*|><:"?/]|\\', "", track_artist)
        track_artist = demoji.replace(track_artist)
        name_ = re.sub(r'[*|><:"?/]|\\', "", name_)
        name_ = demoji.replace(name_)
        # rename track name to make it unique by adding first 3 characters from
        # the artist's name
        name_ = name_ + '_' + track_artist[:3]
        playlist_analysis[name_] = get_segments(track_analysis, segments=segments,
                                                min_conf=min_conf, min_dur=min_dur, tempo=tempo,
                                                sections=sections, beats=beats, bars=bars)
    return playlist_analysis


def get_folder_analysis(spotipyUserAuth, filsort_pl=None, pl_name_id=None, segments=True, min_conf=0.5,
                        min_dur=0.25, sections=True, tempo=False, beats=False, bars=False):
    '''
    Gets audio analysis for all tracks in a playlist, for all playlists.
    Here, we will be using either a filtered and sorted list of playlists
    or a list of user playlist name and id tuples.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    filsort_pl : List[tuple], optional
        Default None. Uses 4-tuple output from filtersort_playlist function.
    pl_name_id : List[tuple], optional
        Dafault None. In the case filsort_pl is not available,
        provide a list of playlist name and id tuples
    segments:  bool, optional
        Default True. False if segments dataframe is not needed
    min_conf: Float
        minimum confidence to include a segment (range 0-1)
    min_dur : Float
        minimum duration/length in secs to include a segment.
        Segments tend to be of very small time intervals, most under a second.
    tempo: bool, optional
        Default True. False if tempo value needs to be returned
    sections: bool, optional
        Default False. True if sections dataframe needs to be returned
    beats: bool, optional
        Default False. True if beats dataframe needs to be returned
    bars: bool, optional
        Default False. True if bars dataframe needs to be returned

    Returns
    -------
    folder_analysis : Dict
         Key : Name of the playlist (string)
         Value : a dict of track analysis of all tracks from the playlist
                 Values here are returned from get_playlist_analysis
    '''

    folder_analysis = {}

    if filsort_pl is not None:
        for p in filsort_pl:

            # remove any special characters from name (they may cause issues in filenaming)
            pl_name = re.sub(r'[*|><:"?/]|\\', "", p[1])
            pl_name = demoji.replace(pl_name)
            folder_analysis[pl_name] = get_playlist_analysis(spotipyUserAuth, playlist_id=p[2],
                                                             segments=segments, tempo=tempo,
                                                             min_conf=min_conf, min_dur=min_dur,
                                                             sections=sections, beats=beats, bars=bars)
    else:
        for p in pl_name_id:

            # remove any special characters from name (they may cause issues in filenaming)

            pl_name = re.sub(r'[*|><:"?/]|\\', "", p[0])
            pl_name = demoji.replace(pl_name)

            folder_analysis[pl_name] = get_playlist_analysis(spotipyUserAuth, playlist_id=p[1],
                                                             segments=segments, tempo=tempo,
                                                             min_conf=min_conf, min_dur=min_dur,
                                                             sections=sections, beats=beats, bars=bars)
    return folder_analysis


def create_dataset(folder_analysis, path):
    '''
    Creates dataset as folders for each playlist, subfolders for all tracks in a playlist folder
    and track analysis dataframes as parquet files.


    Parameters
    ----------
    folder_analysis : Dict
        audio analysis for all tracks in all playlists
        dict returned by get_folder_analysis
    path : str
        path to store the dataset
    '''
    # Path to 'Dataset' dir
    p = path
    # list of dataframe names in output
    df_names = ['tempo', 'segments', 'sections', 'beats', 'bars']

    for fn, i in folder_analysis.items():

        path_ = p.joinpath('{}'.format(fn.strip()))
        path_.mkdir(exist_ok=True)

        for track, j in i.items():

            for k in range(len(j)):

                j[k].to_parquet(path_.joinpath('{}_{}.parquet'.format(track, df_names[k])), engine='pyarrow')


# --------------------------------------------------------------------------------
#  Functions for retrieving track features of the dataset
# --------------------------------------------------------------------------------


def get_tracks_features(spotipyUserAuth, tracksid, showkeys=False):
    '''
    Gets track features for multiple tracks.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    trackids : List[str]
        list of track ids. Wrap a track id in a list if only a single track is present.
    showkeys : bool, optional
        Default False - prints dictionary keys

    Returns
    -------
    track_features : List[Dict]
        list of dictionaries containing track features of all tracks
        in the track id list
    '''
    tracks_features = spotipyUserAuth.audio_features(tracksid)

    if showkeys is True:
        print(tracks_features[0].keys())

    return tracks_features


def tracks_features_to_df(spotipyUserAuth, tracksid):
    '''
    Convert track feature dictionary into dateframe.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    trackids : List[str]
        list of track ids. If a single trackid is present, wrap it in a list.

    Returns
    -------
    features_df : pandas.DataFrame
    '''

    tracks_features = get_tracks_features(spotipyUserAuth, tracksid)
    features_df = json_normalize(tracks_features)
    return features_df


def get_playlist_features(spotipyUserAuth, playlist_id):
    '''Gets features for all tracks in a playlist.

    Parameters
    ----------
    spotipyUserAuth : spotipy object
        returned by 'spotipy_userauth' function.
    playlist_id : str

    Returns : pandas.DataFrame
              Rows represent tracks and colummns represent features
    '''

    tracks_df = get_tracks(spotipyUserAuth, playlist_id)
    features_df = tracks_features_to_df(spotipyUserAuth, tracks_df['id'])
    pl_features_df = pd.concat([tracks_df[['name', 'artists_name']], features_df], axis=1)

    return pl_features_df


def get_folder_features(spotipyUserAuth, filsort_pl=None, pl_name_id=None):
    '''
    Here, we will be using filtered and sorted output. Future edit should take user
    playlist names and id.

    spotipyUserAuth : Spotipy auth object.

    filsort_pl : Default None. Uses 4-tuple output from filtersort_playlist function.
    pl_name_id : Dafault None. In the case filsort_pl is not available,
                 provide list of playlist name and id tuples

    Returns: a dict with key/value pairs for all playlists in the folder.
             Key : Name of the playlist (string)
             Value : pandas.DataFrame returned from get_playlist_features
    '''

    folder_features = {}

    if filsort_pl is not None:
        for p in filsort_pl:

            # remove any special characters from name (they may cause issues in filenaming)
            pl_name = re.sub(r'[*|><:"?/]|\\', "", p[1])
            pl_name = demoji.replace(pl_name)
            folder_features[pl_name] = get_playlist_features(spotipyUserAuth, playlist_id=p[2])
    else:
        for p in pl_name_id:

            # remove any special characters from name (they may cause issues in filenaming)

            pl_name = re.sub(r'[*|><:"?/]|\\', "", p[0])
            pl_name = demoji.replace(pl_name)
            folder_features[pl_name] = get_playlist_features(spotipyUserAuth, playlist_id=p[1])

    return folder_features
