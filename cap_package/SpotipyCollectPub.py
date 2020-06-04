import pandas as pd
from pandas import json_normalize
import re
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


def spotipy_client_cred(client_id, client_secret):
    '''
    Creates server-to-server authentication token and returns spotipy object.
    Token automatically refreshes.
    '''
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    return sp


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


def get_tracks_df(sp, playlistIDs, rem_dup=True, allCol=False):
    '''
    Gets tracks from spotipfy API of the listed playlists and returns
    a single dataframe of tracks from all playlists

    sp : spotipy auth object
    playlistIDs : list of playlist IDs
    rem_dup : Default True. Remove duplicate entries of tracks
              if track name and artists' names match

    returns : dataframe fo tracks df
    '''

    tracks_df = [get_tracks(sp, playlistIDs[i], allCol=True) for i in range(len(playlistIDs))]
    pl_full_df = pd.concat(tracks_df, ignore_index=True)

    artists_list = get_artist_name(pl_full_df)
    pl_full_df.insert(loc=0, column='artists_name', value=artists_list)

    if rem_dup:
        subdf = pl_full_df[pl_full_df.duplicated(subset=['name', 'artists_name'], keep='first')]
        pl_full_df = pl_full_df.drop(subdf.index)
        pl_full_df.reset_index(drop=True)

    if not allCol:
        pl_full_df = pl_full_df[['name', 'id', 'artists_name']]

    return pl_full_df


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


def uri_to_id(uri_list):
    '''
    Parses ID from playlist URI and returns list of playlists' ID.
    Playlist IDs get fed into get_tracks or tracks_analysis

    URI_list : list of playlist URIs
    returns : playlists' ID list
    '''
    id_list = [uri_list[i].split(':')[-1] for i in range(len(uri_list))]

    return id_list
