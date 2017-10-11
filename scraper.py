# -*- codiong: utf-8 -*-

import json
import requests
import time

from bs4 import BeautifulSoup
from instagram_scraper import InstagramScraper
import pandas as pd


class Scraper:

    # ----- Constants ----- #

    QUERY_HASHTAG = """https://www.instagram.com/graphql/query/?query_id=17882293912014529&tag_name={0}&first={1}&after={2}"""
    COLUMNS_MEDIA = ('id', 
                     'user_id', 
                     'location', 
                     'caption', 
                     'url', 
                     'url_thumbnail',
                     'im_height', 
                     'im_width', 
                     'n_comments', 
                     'n_likes', 
                     'timestamp_media', 
                     'timestamp_db',
                     'is_ad',
                     'shortcode')
    COLUMNS_USERS = ('id', 
                     'nickname', 
                     'full_name',
                     'n_followers', 
                     'n_follows', 
                     'bio', 
                     'external_url',
                     'fb_page',
                     'is_verified',
                     'profile_pic_url',
                     'timestamp_db')
    HEADERS = {'user-agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)'}


    # ----- Attributes ----- #

    # DataFrame attributes
    df_media = pd.DataFrame()
    df_users = pd.DataFrame()
    path_df_media = 'df_media.csv'
    path_df_users = 'df_users.csv'
    # Web attributes
    session_hashtag_api = None
    session_media = None
    session_user = None
    idx_first = 500
    # Private attributes
    _ec_hashtag = ''  # end cursor


    # ----- Constructor ----- #

    def __init__(self, path_df_media=None, path_df_users=None, idx_first=500):
        """
        Args:
            path_df_media (str): path to media DataFrame file
                path to the CSV file where media results are stored
            path_df_users (str): path to users DataFrame file
                path to the CSV file where users results are stored
            idx_first (int): index required by hashtag-based search query
        """
        # Check databases
        if path_df_media is None:
            self.df_media = pd.DataFrame(columns=self.COLUMNS_MEDIA)
        else:
            self.path_df_media = path_df_media
            self.df_media = pd.read_csv(path_df_media, delimiter='\t')
        if path_df_users is None:
            self.df_users = pd.DataFrame(columns=self.COLUMNS_USERS)
        else:
            self.path_df_users = path_df_users
            self.df_users = pd.read_csv(path_df_users, delimiter='\t')
        
        # Sessions
        self.session_hashtag_api = requests.Session()
        self.session_hashtag_api.headers = self.HEADERS
        self.session_media = requests.Session()
        self.session_media.headers = self.HEADERS
        self.session_user = requests.Session()
        self.session_user.headers = self.HEADERS

        # Attributes
        self.idx_first = idx_first


    # ----- Public Functions ----- #

    def insert_in_db(self, hashtag='food', n_iterations=10):
        """
        Update database containing information related to new media and users.

        Args:
            hashtag (str): search hashtag
            n_iterations (int): number of times the search is operated

        Returns:
            None
        """
        for i_iter in xrange(n_iterations):
            print "Iteration:", str(i_iter + 1)
            # Get list of media information
            print "Get media list for hashtag:", hashtag
            if i_iter == 0:
                reset_ec = True
            else:
                reset_ec = False
            media_list = \
              self.get_media_metadata_by_hashtag_api(hashtag=hashtag, 
                                                     reset_ec=reset_ec)
            # Process each media item
            print "Process each item"
            for (i_media, media) in enumerate(media_list):
                print i_media, '/', len(media_list)
                try:
                    # Get metadata
                    metadata = self.get_metadata(media)
                    # Update media and users DataFrame
                    self._update_media_df(metadata)
                    self._update_user_df(metadata)
                except Exception, e:
                    print e


    def get_media_metadata_by_hashtag_api(self, hashtag='food', reset_ec=True):
        """
        Get list of media information associated to input hashtag.

        Args:
            hashtag (str): search keyword
            reset_ec (bool): end cursor reset
                if True, end cursor is reset; if False, the algorithm follows 
                the current order

        Returns:
            media_list (list): dict objects containing media information
        """
        # Check input
        if reset_ec is True:
            self._ec_hashtag = ''
        # Initialize output variable
        media_list = []  # output variable
        # Send request
        url = self.QUERY_HASHTAG.format(hashtag, self.idx_first, self._ec_hashtag)
        resp = self.session_hashtag_api.get(url)
        # Check response json
        if resp.ok is True:
            # Get general information
            json_resp = resp.json()
            json_media = json_resp['data']['hashtag']['edge_hashtag_to_media']
            # Update end cursor information
            self._ec_hashtag = json_media['page_info']['end_cursor']
            # Get information for each media element
            json_edges = json_media['edges']
            for edge in json_edges:
                node = edge['node']
                if node['is_video'] is False:
                    # Get picture information
                    id_media = node['id']
                    shortcode = node['shortcode']
                    im_url = node['display_url']
                    im_thumbnail_url = node['thumbnail_src']
                    im_dim_h = node['dimensions']['height']
                    im_dim_w = node['dimensions']['width']
                    n_likes = node['edge_liked_by']['count']
                    text_edges = node['edge_media_to_caption']['edges']
                    text = ''
                    if len(text_edges) > 0:
                        text = node['edge_media_to_caption']['edges'][0]['node']['text']
                    n_comments = node['edge_media_to_comment']['count']
                    timestamp = node['taken_at_timestamp']
                    id_user = node['owner']['id']
                    # Create a row with the extracted info and update list
                    row = {'id_media': id_media,
                           'shortcode_media': shortcode,
                           'im_url': im_url,
                           'im_thumbnail_url': im_thumbnail_url,
                           'im_dim_h': im_dim_h,
                           'im_dim_w': im_dim_w,
                           'n_likes': n_likes,
                           'text': text,
                           'n_comments': n_comments,
                           'timestamp_media': timestamp,
                           'id_user': id_user}
                    media_list.append(row)
        return media_list


    def get_metadata(self, media):
        """
        Given media metadata, obtain the Web metadata using the function
        _get_metadata_from_shortcode and merge all information.

        Args:
            media (dict): item of media_list
                media_list is returned by get_media_metadata_by_hashtag_api

        Returns:
            metadata (dict)
        """
        # Get metadata from shortcode
        shortcode = media['shortcode_media']
        metadata_shortcode = self._get_metadata_from_shortcode(shortcode)
        # Merge media metadata
        metadata = media
        for k in metadata_shortcode.keys():
            metadata[k] = metadata_shortcode[k]
        return metadata


    def write_db_on_file(self):
        """
        Write database on files.

        Args:
            None

        Returns:
            None
        """
        # Media
        if self.path_df_media is not None and len(self.path_df_media) > 0:
            self.df_media.to_csv(self.path_df_media, sep='\t')
        # Users
        if self.path_df_users is not None and len(self.path_df_users) > 0:
            self.df_users.to_csv(self.path_df_users, sep='\t')


    # ----- Private Functions ----- #

    def _get_metadata_from_shortcode(self, shortcode):
        """
        Get metadata from Web page, accessed using the media shortcode.

        Args:
            shortcode (str): media shortcode

        Returns:
            metadata (dict): media and user metadata, merged
        """
        metadata = {}  # output variable
        # Get media JSON from media URL
        url_media = 'https://www.instagram.com/p/' + shortcode
        curr_json_media = self._get_json_from_url(url_media)
        # Check JSON media
        if curr_json_media is not None:
            # Get media data
            media_data = self._get_data_from_media_json(curr_json_media)
            # Get user JSON
            username = media_data['user_nickname']
            url_user = 'https://www.instagram.com/' + username
            curr_json_user = self._get_json_from_url(url_user)
            # Get user data
            user_data = self._get_data_from_user_json(curr_json_user)
            # Merge data
            metadata = media_data
            for k in user_data.keys():
                metadata[k] = user_data[k]
        return metadata


    def _get_json_from_url(self, url):
        """
        Return JSON contained in Web page.

        Args:
            url (str): URL

        Returns:
            curr_json (str): JSON extracted from input URL
        """
        # Constants
        JSON_PREFIX = 'window._sharedData = '
        #
        resp = self.session_media.get(url)
        # Check response
        curr_json = None
        if resp.ok is True:
            # Extract json with relevant information
            soup = BeautifulSoup(resp.content, 'html.parser')
            element_script = soup.find_all('script', {'type': 'text/javascript'})[1]
            element_script_content = element_script.contents[0]
            curr_json_txt = \
              element_script_content[element_script_content.index(JSON_PREFIX) +\
                                     len(JSON_PREFIX) : -1]
            curr_json = json.loads(curr_json_txt)
        return curr_json


    def _get_data_from_media_json(self, curr_json):
        """
        Given the JSON of a single media item, select relevant information.

        Args:
            curr_json (dict): media JSON

        Returns:
            json_data (dict): media data
        """
        # Get data from json
        shortcode_media = \
          curr_json['entry_data']['PostPage'][0]['graphql']['shortcode_media']
        is_ad = shortcode_media['is_ad']
        location = shortcode_media['location']
        owner = shortcode_media['owner']
        user_full_name = owner['full_name']
        user_is_verified = owner['is_verified']
        user_profile_pic_url = owner['profile_pic_url']
        user_nickname = owner['username']
        # Return json data
        json_data = {'is_ad': is_ad,
                     'location': location,
                     'user_full_name': user_full_name,
                     'user_is_verified': user_is_verified,
                     'user_profile_pic_url': user_profile_pic_url,
                     'user_nickname': user_nickname}
        return json_data


    def _get_data_from_user_json(self, curr_json):
        """
        Given the JSON of a single user, select relevant information.

        Args:
            curr_json (dict): user JSON

        Returns:
            json_data (dict): user data
        """
        # Get data from json
        user = curr_json['entry_data']['ProfilePage'][0]['user']
        biography = user['biography']
        fb_page = user['connected_fb_page']
        external_url = user['external_url']
        n_followers = user['followed_by']['count']
        n_follows = user['follows']['count']
        n_media = user['media']['count']
        # Return json data
        json_data = {'biography': biography,
                     'fb_page': fb_page,
                     'external_url': external_url,
                     'n_followers': n_followers,
                     'n_follows': n_follows,
                     'n_media': n_media}
        return json_data


    def _update_media_df(self, metadata):
        """
        Given metadata associated to a media item, append it to the media 
        DataFrame.

        Args:
            metadata (dict): information returned by get_metadata

        Returns:
            None
        """
        # Get current time
        current_time = str(int(time.time()))
        # Check if media is contained in DataFrame
        id_media = metadata['id_media']
        if id_media not in self.df_media['id'].values:  # not contained => create new
            # Create new item
            item = [id_media,
                    metadata['id_user'],
                    metadata['location'],
                    metadata['text'],
                    metadata['im_url'],
                    metadata['im_thumbnail_url'],
                    metadata['im_dim_h'],
                    metadata['im_dim_w'],
                    metadata['n_comments'],
                    metadata['n_likes'],
                    metadata['timestamp_media'],
                    current_time,
                    metadata['is_ad'],
                    metadata['shortcode_media']]
            # Convert item to DataFrame
            df_item = pd.DataFrame(data=[item], columns=self.COLUMNS_MEDIA)
            # Add item to DataFrame
            self.df_media = pd.concat([self.df_media, df_item], 
                                      ignore_index=True)
        else:  # contained => update
            self.df_media[self.df_media['id'] == id_media]['n_comments'] = \
              metadata['n_comments']
            self.df_media[self.df_media['id'] == id_media]['n_likes'] = \
              metadata['n_likes']
            self.df_media[self.df_media['id'] == id_media]['timestamp_db'] = \
              dtz_string


    def _update_user_df(self, metadata):
        """
        Given metadata associated to a user, append it to the media DataFrame.

        Args:
            metadata (dict): information returned by get_metadata

        Returns:
            None
        """
        # Get current time
        current_time = str(int(time.time()))
        # Check if user is contained in DataFrame
        id_user = metadata['id_user']
        if id_user not in self.df_users['id'].values:
            # Create new item
            item = [id_user,
                    metadata['user_nickname'],
                    metadata['user_full_name'],
                    metadata['n_followers'],
                    metadata['n_follows'],
                    metadata['biography'],
                    metadata['external_url'],
                    metadata['fb_page'],
                    metadata['user_is_verified'],
                    metadata['user_profile_pic_url'],
                    current_time]
            # Convert item to DataFrame
            df_item = pd.DataFrame(data=[item], columns=self.COLUMNS_USERS)
            # Add item to DataFrame
            self.df_users = pd.concat([self.df_users, df_item], 
                                      ignore_index=True)
        else:
            self.df_users[self.df_users['id'] == id_user]['n_followers'] = \
              metadata['n_followers']
            self.df_users[self.df_users['id'] == id_user]['n_follows'] = \
              metadata['n_follows']
            self.df_users[self.df_users['id'] == id_user]['timestamp_db'] = \
              dtz_string
