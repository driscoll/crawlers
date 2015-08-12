# -*- coding: utf-8 -*-
"""
Crawl a two-step friends network for a Twitter user

Minimally tested in August 2015 so it might be totally broken now.

This script may take a very long time to run 
and will probably use lots of disk space.

A seed user with 2000 friends will take several days 
to crawl and result in about 3GB of data.

You need to register an app with Twitter to get OAuth credentials.
Let me know if you get stuck.

Kevin Driscoll, 2015

"""

import argparse
import numpy as np
import OpenSSL
import os
import cPickle as pickle
import re
import requests
import time
from requests_oauthlib import OAuth1Session


class TwitterAPI(OAuth1Session):

    def get(self, url, **args):
        """Kludge to catch a strange empty OpenSSL error
        that gets thrown when we don't use the session for 
        a few minutes.
        """
        try:
            response = super(self.__class__, self).get(url, **args)
        except OpenSSL.SSL.Error as e:
            print "OpenSSL error:", e
            print "Trying again in 30s..."
            time.sleep(30.0)
            response = super(self.__class__, self).get(url, **args)
        return response

def check_rate_limit(api, url, zzz=180.0):
    """Returns a dict with remaining requests and 
    seconds until the window closes
        api: TwitterAPI object,
        url: Valid URL endpoint at the Twitter REST API
    """
    pattern = 'https:\/\/api.twitter.com\/.*(\/([a-z_]*)\/.*)\.json'
    endpoint, family = re.match(pattern, url).groups()
    url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
    params = {"resources": [family]}
    response = api.get(url, params=params)
    response.close()
    try:
        return response.json()["resources"][family][endpoint]
    except KeyError:
        try:
            return response.json()["resources"][family][endpoint + '/:id']
        except KeyError:
            print "Error checking rate limit status:"
            print response.json()
            print "Sleeping {:,}s and trying again...".format(zzz)
            # DEBUG
            # Weirdly we get an OpenSSL error everytime
            # we go to sleep
            time.sleep(zzz)
            return check_rate_limit(api, url, zzz=zzz*2)
    
def get_one_profile(api, user_id=None, screen_name=None):
    """Returns dictionary with profile data. 
    Must specify user_id OR screen_name.
    """
    url = u'https://api.twitter.com/1.1/users/show.json'
    if user_id:
        params = {'user_id': user_id}
    elif screen_name:
        params = {'screen_name': screen_name}
    else:
        return {}
    params['include_entities'] = True
    rate_status = check_rate_limit(api, url)
    if not rate_status['remaining']:
        delay = rate_status['reset'] - time.time()
        if delay > 0:
            print "Sleeping {0}...".format(delay)
            time.sleep(delay)    
    response = api.get(url, params=params)
    return response.json()

def get_my_profile(api):
    """Returns profile data for the user 
    verified by this TwitterAPI object.
    """ 
    url = "https://api.twitter.com/1.1/account/verify_credentials.json"
    rate_status = check_rate_limit(api, url)
    if not rate_status["remaining"]:
        delay = rate_status['reset'] - time.time()
        if delay > 0:
            print "Sleeping {0}...".format(delay)
            time.sleep(delay)
    response = api.get(url)
    response.close()
    return response.json()

def get_friends_ids(api, user_id):
    """Returns the user IDs for the friends of user_id.
    """
    url = "https://api.twitter.com/1.1/friends/ids.json"
    rate_status = check_rate_limit(api, url)
    remaining_requests = rate_status["remaining"]
    if not remaining_requests:
        delay = rate_status['reset'] - time.time()
        if delay > 0:
            print "Sleeping {0}...".format(delay)
            time.sleep(delay)        
            rate_status = check_rate_limit(api, url)
            remaining_requests = rate_status["remaining"]

    friends_ids = []
    params = {"user_id": user_id, "counter": 0, 
              "count": 5000, "stringify_ids": True}
    response = api.get(url, params=params)
    friends_ids.extend(response.json().get("ids", []))
    response.close()
    remaining_requests -= 1

    while response.json().get('next_cursor'):
        if not remaining_requests:
            delay = rate_status['reset'] - time.time()
            if delay > 0:
                print "Sleeping {0:,.4} s...".format(delay)
                time.sleep(delay)        
                rate_status = check_rate_limit(api, url)
                remaining_requests = rate_status["remaining"]
        params["cursor"] = response.json().get('next_cursor_str')
        response = api.get(url, params=params)
        friends_ids.extend(response.json().get("ids", []))
        response.close()
        remaining_requests -= 1
    return friends_ids

def users_lookup(api, user_ids):
    """Returns a dict with profile information 
    for a list of user IDs.
        user_ids: list of user IDs
    """
    url = "https://api.twitter.com/1.1/users/lookup.json"
    i = 0
    rate_status = check_rate_limit(api, url)
    remaining_requests = rate_status["remaining"]
    if not remaining_requests:
        delay = rate_status['reset'] - time.time()
        if delay > 0:
            print "Sleeping {0}...".format(delay)
            time.sleep(delay)
        rate_status = check_rate_limit(api, url)
        remaining_requests = rate_status["remaining"]

    users = {}
    for i in range(0, len(user_ids), 100):
        interval = 100
        user_id_param = [long(uid) for uid in user_ids[i:i+interval]]
        params = {"user_id": user_id_param, "include_entities": True}
        response = api.get(url, params=params)
        if 'errors' in response.json():
            for error in response.json().get('errors', []):
                print 'Error code:', error.get('code', 'NO CODE')
                print 'Error message:', error.get('message', 'NO MESSAGE')
        else:
            for user in response.json():
                id_str = user["id_str"]
                users[id_str] = user
        response.close()

        remaining_requests -= 1
        if not remaining_requests:
            delay = rate_status['reset'] - time.time()
            if delay > 0:
                print "Sleeping {0}...".format(delay)
                time.sleep(delay)
            rate_status = check_rate_limit(api, url)
            remaining_requests = rate_status["remaining"]
    return users

def crawl_friends(api, friends_ids=[]):
    """Returns a dict with profile data for each of 
    the users in friends_ids.
        friends_ids: list of valid Twitter user IDs
    """
    req_count = len(friends_ids) / 100.0
    hours = req_count / 12.0 / 60.0
    print "Estimated time to crawl profiles: {:,} hours".format(hours)
    print
    friends = users_lookup(api, friends_ids)
    return friends

def crawl_friends_ids(api, friends={}):
    """Retrieves the user IDs for the friends of your friends
        friends: dict of friend profiles (as returned by crawl_friends)
    """ 
    req_count = 0
    for f_id in friends:
        if not "friends_ids" in friends[f_id]:
            friends_count = friends[f_id]["friends_count"]
            req_count += np.int(np.ceil(friends_count / 5000.0))
    hours = req_count / 60.0
    print "Estimated time to crawl: {:,.4} hours".format(hours)
    print

    # Retrieve friends of friends IDs
    for f_id in friends:
        if not "friends_ids" in friends[f_id]:
            new_friends_ids = get_friends_ids(api, f_id)
            friends[f_id]["friends_ids"] = new_friends_ids
            print "{:>16} {:,}".format(friends[f_id]["screen_name"],
                                        len(new_friends_ids))
    return friends

def flatten_friends_ids(users):
    """Returns a list of unique user IDs for 
    the friends of a group of users 
        users: dict of user profiles (as returned by crawl_friends)
    """
    friends_ids = []
    for user_id in users:
        friends_ids.extend(users[user_id]["friends_ids"])       
    return list(set(friends_ids))


if __name__=="__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--clientkey', 
                            type=str,
                            default=u"",
                            help="Consumer Key")
    parser.add_argument('--clientsecret', 
                            type=str,
                            default=u"",
                            help="Consumer Secret")
    parser.add_argument('--resourcekey', 
                            type=str,
                            default=u"",
                            help="Access Token")
    parser.add_argument('--resourcesecret', 
                            type=str,
                            default=u"",
                            help="Access Token Secret")
    args = parser.parse_args()

    consumer_key = args.clientkey
    consumer_secret = args.clientsecret 
    access_token = args.resourcekey 
    access_token_secret = args.resourcesecret

    print "Creating a Twitter API session object..."
    twitter = TwitterAPI(client_key=consumer_key, 
                                client_secret=consumer_secret, 
                                resource_owner_key=access_token, 
                                resource_owner_secret=access_token_secret)

    my_profile = get_my_profile(twitter)
    my_friends_ids = get_friends_ids(twitter, my_profile['id_str'])
    my_profile['friends_ids'] = my_friends_ids

    for label, value in (("User ID", my_profile['id_str']),
                        ("Screen name", my_profile['screen_name']),
                        ("Friend count", len(my_profile['friends_ids']))):
        print "{:>12}: {}".format(label, value)
    print

    datapath = my_profile['screen_name']
    i = 0
    while os.path.exists(datapath):
        template = '{}.{}'
        datapath = template.format(my_profile['screen_name'], i)
        i += 1
    print "Creating data directory: ./{}".format(datapath)
    os.mkdir(datapath)

    fn = os.path.join(datapath, my_profile['screen_name'] + '.pickle')
    print "Saving my profile data to:", fn
    with open(fn, 'wb') as f:
        pickle.dump(my_profile, f)
    print

    print "Crawling my friends..."
    friends = crawl_friends(twitter, my_friends_ids)
    print "Crawled my {:,} friends.".format(len(friends))
    
    print "Crawling their friends' IDs..."
    friends = crawl_friends_ids(twitter, friends)
    print

    fn = os.path.join(datapath, my_profile['screen_name'] + '.friends.pickle')
    print "Saving my friends' profile data to:", fn
    with open(fn, 'wb') as f:
        pickle.dump(friends, f)
    print

    foaf_ids = flatten_friends_ids(friends)
    print "Crawling my friends' {:,} friends...".format(len(foaf_ids))
    foaf_profiles = crawl_friends(twitter, foaf_ids)
    print "{:,} friends of friends crawled.".format(len(foaf_profiles))
    print 

    fn = os.path.join(datapath, my_profile['screen_name'] + '.foaf.pickle')
    print "Saving friends of friends profile data to:", fn
    with open(fn, 'wb') as f:
        pickle.dump(foaf_profiles, f)