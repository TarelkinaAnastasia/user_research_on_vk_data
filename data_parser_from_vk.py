import pandas as pd
import requests
import time
import random
import numpy as np

fields = "can_see_audio,bdate,can_send_friend_request,can_write_private_message,career,counters, common_count,connections,contacts,city,country,crop_photo,domain,education,exports,followers_count,friend_status,has_photo,has_mobile,home_town,sex,site,schools,screen_name,status,verified,games,interests,is_favorite,is_friend,is_hidden_from_feed,last_seen,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_id,photo_max,photo_max_orig,schools,quotes, sex, status, wall_default, verified about,relation,relatives,timezone,tv, universities"
token = "<your_token>"
method1 = 'users.get'
method2 = 'friends.get'
method3 = 'groups.get'


def get_vk_data(method, params, max_attempts=5):
    url = f'https://api.vk.com/method/{method}'
    attempt = 0
    while attempt < max_attempts:
        resp = requests.get(url, params=params)
        data = resp

        if 'error' in data.json() and (data.json()['error'].get('error_code') == 6 or data.json()['error'].get('error_code') == 6):
            time.sleep(4)
            attempt += 1
            continue

        break

    return data


def get_data(id, data):
    paramusers = {'access_token': token, 'user_id': id, 'v': 5.131,
                  'fields': fields}
    try:
        rec2users = get_vk_data(method1, paramusers)
        #print(rec2users.json())
        data = pd.concat([data, (pd.json_normalize(rec2users.json()['response'][0], sep='_'))])
        #groups = get_groups(id, groups)
    except Exception as e:
        #print(e, rec2users.json())
        return data
    return data


# def get_groups(id, groups):
#     paramgroups = {'access_token': token, 'user_id': id, 'v': 5.131, 'extended': 1,
#                   'fields': 'id, name, description, type, deactivated, activity, age_limits'}
#     try:
#         rec2groups = get_vk_data(method3, paramgroups)          #requests.get(url=f'https://api.vk.com/method/{method3}', params=paramgroups)
#
#         df = pd.concat([pd.Series(id * np.ones(rec2groups.json()['response'].get('count'))).astype(int), (pd.json_normalize(rec2groups.json()['response'].get('items'), sep='_'))], axis = 1)
#         #print(rec2groups)
#         groups = pd.concat([groups, df], ignore_index = True)
#     except Exception as e:
#         #print (e, rec2groups.json())
#         return groups
#     return groups
#

def main():
    for end in range(44, 47, 1):
        start = end - 1
        data = pd.DataFrame()
        for j in range(10000):
            i = random.randrange(start * 10000000, end * 10000000)
            print(j, i)
            data = get_data(i, data)

        for j in ['interests', 'tv', 'quotes', 'games', 'movies', 'status', 'university_name','faculty_name','occupation_name']:
            try:
                data[j] = data[j].str.replace('\n', '')
                data[j] = data[j].str.replace('\r', '')
            except(Exception):
                continue

        filename = 'data' + str(start) + '_' + str(end) + '.csv'
        print(filename)
        data.to_csv(filename)


if __name__ == '__main__':
    main()