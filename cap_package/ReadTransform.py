import numpy as np
import pandas as pd
import random
import re
from sklearn.preprocessing import OneHotEncoder

# --------------------------------------------------------------------------------
#  Functions for reading and transforming dataset
# --------------------------------------------------------------------------------


def read_dataset(path_, segments=True, sections=False, tempo=False):
    '''
    Read analysis(dataframes) dataset stored as parquet files.
    Use this when only track info needs to be retained and not playlist labels

    path_ : path to dataset directory
    segments : Boolean - True to read segments files
    tempo : Boolean - True to read tempo files. Default False
    return : a tuple for all playlists in the folder -
             (name : Name of the playlist (string),
              segments : if true, a list of track analysis dataframes of all tracks from the playlist
              tempo : if true, a list of tempo values (as pandas dataframes/series) of all tracks from the playlist)
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

        if sections:
            sections_list = [(re.sub('_sections.parquet', '', s.name), pd.read_parquet(s)) for s in pl.glob('*_sections.parquet')]
            pl_info.append(sections_list)

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


def transform_dataset(dataset, timbre_min, timbre_max, num_seg=50, bin_num=5):
    '''
    Create input arrays to be fed into a model.
    A fixed number(num_seg) of segments are randomly chosen from each track(dataframe) and
    timbre columns of these segments are scaled. The resulting dataframe is then converted
    to a numpy array

    Note - Segments dataframe will have missing indices since they have been passed through
    a filter constricting them to minimum duration and confidence.
    See get_segments in SpotifyCollect module for details.

    dataset : list of track segments dataframes
    timbre_min : List or numpy array of minimums of timbre values over the whole dataset
    timbre_max : List or numpy array of maximums of timbre values over the whole dataset
    num_seg : Default : 50 - Number of segments to be taken for input.
    bin_num : Number of bins for rows of segments dataframes are to be divided in

    returns : data - list of track input arrays.
    '''
    if num_seg % bin_num != 0:
        print('Make sure num_seg is divisible by bin_num to ensure equal number of segments are chosen from each bin')

    bin_seg = int(num_seg / bin_num)
    timbre_ = ['timbre_{}'.format(i + 1) for i in range(12)]

    timbre_min = np.array(timbre_min)
    timbre_max = np.array(timbre_max)

    data_arrays = []

    for df in dataset:

        bin_size = int(len(df) / bin_num)
        idx = list(df.index)

        # make a list of randomly chosen 'bin_seg' number of segments from each bin
        idx_list = []
        for b in range(bin_num):

            idx_list += random.sample(idx[b * bin_size: (b + 1) * bin_size], bin_seg)

        idx_list.sort()

        # filter index of n -> num_seg randomly chosen segments
        df_ = df.loc[idx_list]

        # Scale timbre values
        df_[timbre_] = df_[timbre_].apply(minmax_scale, args=(timbre_min, timbre_max), axis=1)

        # Convert dataframe to numpy array
        segments = df_.to_numpy()
        segments = np.hstack(segments)
        data_arrays.append(segments)

    return data_arrays


def get_segsec_stats(tracks_seg, tracks_sec):
    '''
    Create input arrays to be fed into a model.
    A fixed number(num_seg) of segments are randomly chosen from each track(dataframe) and
    timbre columns of these segments are scaled. The resulting dataframe is then converted
    to a numpy array

    Note - Segments dataframe will have missing indices since they have been passed through
    a filter constricting them to minimum duration and confidence.
    See get_segments in SpotifyCollect module for details.

    dataset : list of track segments dataframes
    timbre_min : List or numpy array of minimums of timbre values over the whole dataset
    timbre_max : List or numpy array of maximums of timbre values over the whole dataset
    num_seg : Default : 50 - Number of segments to be taken for input.
    bin_num : Number of bins for rows of segments dataframes are to be divided in

    returns : data - list of track input arrays.
    '''

    sec_stat = []
    seg_stat = []

    keys = [x for x in range(12)]
    keys_column = ['key_{:0>2d}'.format(i + 1) for i in range(12)]
    enc = OneHotEncoder(categories=[keys])

    for t in zip(tracks_seg, tracks_sec):

        kl = {}
        tsec_stat = {}
        tseg_stat = {}
        seg = t[0]
        sec = t[1]

        top5 = sec.duration.sort_values(ascending=False)[:5].index.sort_values().array

        for i in top5:

            tsec_stat['sec_{}'.format(i)] = \
                seg[(seg['start'] >= sec.start[i]) & (seg['start'] < sec.start[i] + sec.duration[i])] \
                .loc[:, 'timbre_01': 'timbre_12'] \
                .mean()
            kl['sec_{}'.format(i)] = sec.iloc[i][['key', 'loudness']]

        tsec_stat = pd.DataFrame.from_dict(tsec_stat, orient='index')
        kl = pd.DataFrame.from_dict(kl, orient='index')
        kl = kl.astype({'key': 'int32'})
        X = np.array(kl.key).reshape(-1, 1)
        enc_keys = enc.fit_transform(X).toarray()
        kl[keys_column] = pd.DataFrame(enc_keys, index=kl.index)
        kl = kl.drop(columns=['key'])

        nulls = [i for i, x in tsec_stat.iterrows() if x.isnull().sum() > 0]
        if nulls:
            indices = list(set(tsec_stat.index[:len(top5)]) - set(nulls))
            val = tsec_stat.loc[indices].mean()
            for idx in nulls:
                tsec_stat.loc[idx] = val
                tsec_stat.rename(index={idx: '{}_upd'.format(idx)}, inplace=True)
                kl.rename(index={idx: '{}_upd'.format(idx)}, inplace=True)

        if len(top5) < 5:
            index = ['sec_avg{}'.format(j) for j in range(5 - len(top5))]

            top_avg = tsec_stat.iloc[:len(top5)].mean()
            values = [top_avg for j in range(5 - len(top5))]
            lines = pd.DataFrame(values, index=index)
            tsec_stat = pd.concat(
                [tsec_stat.iloc[:int(len(top5) / 2)], lines, tsec_stat.iloc[int(len(top5) / 2):]])

            filler = kl.iloc[int(len(top5) / 2)]
            values_ = [filler for j in range(5 - len(top5))]
            lines_ = pd.DataFrame(values_, index=index)
            kl = pd.concat(
                [kl.iloc[:int(len(top5) / 2)], lines_, kl.iloc[int(len(top5) / 2):]])

        tsec = pd.concat([kl, tsec_stat], axis=1)
        sec_stat.append(tsec)

        tseg_stat['min'] = seg.loc[:, 'timbre_01': 'timbre_12'].min()
        tseg_stat['max'] = seg.loc[:, 'timbre_01': 'timbre_12'].max()
        tseg_stat['mean'] = seg.loc[:, 'timbre_01': 'timbre_12'].mean()
        tseg_stat['std'] = seg.loc[:, 'timbre_01': 'timbre_12'].std()
        tseg_stat['skewness'] = seg.loc[:, 'timbre_01': 'timbre_12'].skew()
        tseg_stat['kurtosis'] = seg.loc[:, 'timbre_01': 'timbre_12'].kurt()

        tseg_stat = pd.DataFrame.from_dict(tseg_stat, orient='index')
        seg_stat.append(tseg_stat)

    return seg_stat, sec_stat

def encode_label(data_labels):

    X = np.array(data_labels).reshape(-1, 1)
    data_encode = OneHotEncoder().fit(X)
    categories = data_encode.categories_
    data_encoded = data_encode.transform(X).toarray()

    return data_encoded, categories
