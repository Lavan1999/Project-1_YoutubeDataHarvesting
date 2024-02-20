#Youtube Data Harvesting and Warehousing Project:
    
    '''In this project we need to extract the data from the resourse Youtube. So that 
we need to connect with api key. By using Api key we can easily extract the datas from
the resorce. By python build in function called 'build' can connect with the api key.'''


from googleapiclient.discovery import build
#1 Connecting youtube api

def connecting_api():
    api_key = 'AIzaSyB2UG9mxzeVrqqI916PGYnCI8JLKrLnrxQ'
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey = api_key)
    return youtube
youtube = connecting_api()



#2 Getting youtube channel details

def channel_details(channel_id):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id= channel_id).execute()

  for i in request['items']:
    channel_information = dict(
    channel_id = i['id'],
    channel_name = i['snippet']['title'],
    channel_type = i['kind'],
    channel_description = i['snippet']['description'],
    playlists_id = i['contentDetails']['relatedPlaylists']['uploads'],
    channel_views = i['statistics']['viewCount'])
  return channel_information



#3 getting playlist id and details

def playlist_details(channel_id):
    playlist_datas = []
    nextpage = None
    while True:
        request1 = youtube.playlists().list(part = 'snippet,contentDetails',
                                            channelId = channel_id,
                                            maxResults = 50,
                                            pageToken = nextpage).execute()

        for i in request1['items']:
            playlists_info = dict(
                playlist_id = i['id'],
                channel_id = i['snippet']['channelId'],
                playList_name = i['snippet']['title'],
                channel_name = i['snippet']['channelTitle'],
                video_count = i['contentDetails']['itemCount'])
            playlist_datas.append(playlists_info)
        if nextpage is None:
            break
    return playlist_datas



#4 getting video ids 

def video_ids(channel_id):
    video_ids = []
    request2 = youtube.channels().list(
                                  id= channel_id,
                                  part='contentDetails').execute()
    playlists_id =  request2['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    nextpage = None
    while True:
        request3 = youtube.playlistItems().list(part = 'snippet',
                                                  playlistId = playlists_id,
                                                  maxResults = 50,
                                                  pageToken = nextpage).execute()
        
        for i in range(len(request3['items'])):
            video_ids.append(request3['items'][i]['snippet']['resourceId']['videoId'])
        nextpage = request3.get('nextPageToken')
        if nextpage is None:
            break
    return video_ids


#Now we need to get the video ids from channel, by using channel_plalist_id.
#5 Getting video details

def video_details(video_ids):
      video_datas = []
      try:
            for video_id in video_ids:
                        request4 = youtube.videos().list(part = "snippet,contentDetails,statistics",
                                                            id = video_id).execute()
                        for i in request4['items']:
                              video_info = dict(
                                    video_name = i['snippet']['title'],
                                    video_id = i['id'],
                                    channel_id = i['snippet']['channelId'],
                                    channel_name = i['snippet']['channelTitle'],
                                    video_description = i['snippet'].get('description'),
                                    published_date = i['snippet']['publishedAt'],
                                    view_count = i['statistics'].get('viewCount'),
                                    like_count = i['statistics'].get('likeCount'),
                                    dislike_count = i['statistics'].get('dislikeCount'),
                                    favorite_count = i['statistics'].get('favoriteCount'),
                                    comment_count = i['statistics'].get('commentCount'),
                                    duration = i['contentDetails']['duration'],
                                    thumbnail = i['snippet']['thumbnails']['default']['url'],
                                    caption_status = i['contentDetails'].get('caption'))
                              video_datas.append(video_info)
      except:
            pass
      return video_datas
  
  
  
#6  Collecting comment id


def comment_details(video_ids):
    comment_datas = []
    try:
        for video_id in video_ids:
            request5 = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId= video_id, maxResults = 20).execute()

            for i in request5['items']:
                comment_info = dict(
                    comment_id = i['snippet']['topLevelComment']['id'],
                    comment_video_id = i['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published_date = i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_datas.append(comment_info)
    except:
        pass

    return comment_datas
