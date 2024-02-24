import googleapiclient
from googleapiclient.discovery import build

import pymongo
import pandas as pd 
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer,Date, PrimaryKeyConstraint
from sqlalchemy.exc import SQLAlchemyError
import psycopg2


#1 Connecting youtube api------------

def connecting_api():
    api_key = 'AIzaSyB2UG9mxzeVrqqI916PGYnCI8JLKrLnrxQ'
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey = api_key)
    return youtube
youtube = connecting_api()



#2 Getting youtube channel details------------

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



#3 getting playlist id and details---------------

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



#4 getting video ids and details---------------

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



#5 Getting video details------------

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
  
  
  
#6  Collecting comment id-------------

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

    
    
#connecting mongodb (nosql) ----------------

client = pymongo.MongoClient('mongodb+srv://lavanya:Lavan123@guvilavan.5pjwpvl.mongodb.net/?retryWrites=true&w=majority')
db = client['youtubeproject']
collection = db['Youtube_details']



# To get all the data of the given channel

def extracting_Data(channel_id):
    channel_data = channel_details(channel_id)
    playlist_data = playlist_details(channel_id)
    videoIds = video_ids(channel_id)
    video_data = video_details(videoIds)
    comment_data = comment_details(videoIds)
        
    Data = {'channel_details': channel_data, 
                'playlist_details': playlist_data,
                'video_details': video_data, 
                'comment_details': comment_data}
        
    collection.insert_one(Data)
        
    return 'Succesfully inserted!!!'



#Channel DataFrame
# converting DataFrame for channels

def channeldf():
    channel = []
    for i in collection.find({},{'_id':0,'channel_details':1}):
        channel.append(i['channel_details'])
    dfc = pd.DataFrame(channel)
    
    #Changing the datatype of channel_views as int
    dfc['channel_views'] = dfc['channel_views'].astype(int)
    
    #Replace empty strings in the 'description' column with 'No description given'
    dfc['channel_description'] = dfc['channel_description'].replace('', 'No description given')
    
    return dfc




# Playlist Dataframe
# converting DataFrame for playlists

def playlistdf():
    playlist = []
    for i in collection.find({},{'_id':0, 'playlist_details':1}):
        playlist.append(i['playlist_details'])
        
    playlist_data = [i for sublist in playlist for i in sublist]
    dfp = pd.DataFrame(playlist_data)
    
    #Changing the dtype of video_count as integer
    dfp['video_count'] = dfp['video_count'].astype(int)
    return dfp




# Video Dataframe
# dfv  Concerting DataFrame for Videos
def videodf():
    video = []
    for i in collection.find({},{'_id':0, 'video_details':1}):
        video.append(i['video_details'])
    video_data = [i for sublist in video for i in sublist]
    dfv = pd.DataFrame(video_data)
    dfv.fillna(0,inplace=True)
    
    #Changing the dtype str to integer
    dfv['view_count'] = dfv['view_count'].astype(int)
    dfv['dislike_count'] = dfv['dislike_count'].astype(int)
    dfv['like_count'] = dfv['like_count'].astype(int)
    dfv['favorite_count'] = dfv['favorite_count'].astype(int)
    dfv['comment_count'] = dfv['comment_count'].astype(int)
    
    
    # Convert 'published_date' column to datetime objects
    dfv['published_date'] = pd.to_datetime(dfv['published_date'])

    # Format 'published_date' as 'ddmmyyyy'
    dfv['published_date'] = dfv['published_date'].dt.strftime('%d-%m-%Y')
    
    return dfv




# Comment dataframe
# dfm  Converting DataFrame for comments
def commentdf():   
    comment = []
    for i in collection.find({},{'_id':0, 'comment_details': 1}):
        comment.append(i['comment_details'])
    comment_data = [i for sublist in comment for i in sublist]
    dfm = pd.DataFrame(comment_data)
    
    # Convert 'published_date' column to datetime objects
    dfm['published_date'] = pd.to_datetime(dfm['published_date'])

    # Format 'published_date' as 'ddmmyyyy'
    dfm['published_date'] = dfm['published_date'].dt.strftime('%d-%m-%Y')
    
    return dfm




dfc = channeldf()
dfp = playlistdf()
dfv = videodf()
dfm = commentdf()
    



# SQL CHANNEL TABLE
def channeltable():
    try:
        # Define database connection
        engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubedb')
        
        # Create SQLAlchemy metadata
        metadata = MetaData()
        
        # Define table schema
        channel_table = Table('channel_table', metadata,
                              Column('channel_id', String, primary_key=True),
                              Column('channel_name', String),
                              Column('channel_type', String),
                              Column('channel_description', String),
                              Column('playlists_id', String),
                              Column('channel_views', Integer)
                             )
        
        # Create table in the database
        metadata.create_all(engine)

        
        # Convert DataFrame to SQL table
        dfc_sql=dfc.to_sql('channel_table', engine, if_exists='replace', index=False)
    
        
    except SQLAlchemyError as e:
        print('An error occurred:', e)
        
    return dfc_sql



#PLAYLIST TABLE
def playlisttable():
    try:
        # Define database connection
        engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubedb')
        
        # Create SQLAlchemy metadata
        metadata = MetaData()
        
        # Define table schema
        playlist_table = Table('playlist_table', metadata,
                               Column('playlist_id', String, primary_key=True),
                               Column('channel_id', String),
                               Column('playlist_name', String),
                               Column('playlist_description', String),
                               Column('video_count', Integer)
                              )
        
        # Create table in the database
        metadata.create_all(engine)
        print('Table created successfully')
        
        # Convert DataFrame to SQL table
        dfp_sql =dfp.to_sql('playlist_table', engine, if_exists='replace', index=False)
        print('Data inserted into the table')
        
    except SQLAlchemyError as e:
        print('An error occurred:', e)
    return dfp_sql



# VIDEO TABLE
from sqlalchemy import create_engine, String, Integer,Date
def videotable():
    try:
        # Define database connection
        engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubedb')
        
        # Create SQLAlchemy metadata
        metadata = MetaData()
        
        # Define table schema
        video_table = Table('video_table', metadata,
                            Column('video_name', String),
                            Column('video_id', String, primary_key=True),
                            Column('channel_id', String),
                            Column('channel_name', String),
                            Column('video_description', String),
                            Column('published_date', Date),
                            Column('view_count', Integer),
                            Column('dislike_count', Integer),
                            Column('like_count', Integer),
                            Column('favorite_count', Integer),
                            Column('comment_count', Integer),
                            Column('duration', String),
                            Column('thumbnail', String),
                            Column('caption_status', String)
                           )
        
        # Create table in the database
        metadata.create_all(engine)
        print('Table created successfully')
        
        # Convert DataFrame to SQL table
        dfv_sql = dfv.to_sql('video_table', engine, if_exists='replace', index=False)
        print('Data inserted into the table')
        
    except SQLAlchemyError as e:
        print('An error occurred:', e)
    return dfv_sql


#COMMENT TABLE
from sqlalchemy import create_engine, String, Integer,Date
def commenttable():
    try:
        # Define database connection
        engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubedb')
        
        # Create SQLAlchemy metadata
        metadata = MetaData()
        
        # Define table schema
        comment_table = Table('comment_table', metadata,
                              Column('comment_id', String, primary_key=True),
                              Column('comment_video_id', String),
                              Column('comment_text', String),
                              Column('comment_author', String),
                              Column('published_date', Date)
                             )
        
        # Create table in the database
        metadata.create_all(engine)
        
        # Convert DataFrame to SQL table
        dfm_sql = dfm.to_sql('comment_table', engine, if_exists='replace', index=False)

    except SQLAlchemyError as e:
        print('An error occurred:', e)
        
    return dfm_sql


# To convert all the dataframe to tables
def alltables():
    channeltable()
    playlisttable()
    videotable()
    commenttable()
    return 'Tables created succesfully'


#Streamlit UI codes------------------

import streamlit as st
import time


st.header(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')   
st.title(':green[Data Scrab]')


st.toast(':rainbow[Happy to diploy my project, Tq for watching!!!]',icon ='🎉') 
time.sleep(5)

with st.sidebar:
    if st.header(':blue[Process of the project]'):
        st.write('1.Data Collection')
        st.write('2.The filtered data inserted into mongodb')
        st.write('3.Create DataFrame by using pandas from mongodb')
        st.write('4.Cleaned Dataframe coverted into sql table')
    
    
    if st.button(':blue[My Greetings!!!]'):
        st.toast('Hello sir/mam...', icon='😍')
        time.sleep(.5)
        st.toast('I/m Lavanya, Welcome to my streamlit app', icon= "🔥")
        time.sleep(.6)
        st.toast('I/m happy to diploy my project', icon='🎉')
        
        
st.text_area("About Data harvesting",
              'Data harvesting is a process that copies datasets and their metadata between two or more data' 
              'catalogs—a critical step in making data useful. It’s similar to the techniques that search engines use to'
              'look for, catalog, and index content from different websites to make it searchable in a single location.' 
              'Application programming interfaces (APIs) act as lines of communication between different databases.')


st.header(":violet[About the workflow of the project]")
st.write('To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.') 
st.write('The application should have the following features: Ability to input a YouTube channel ID and retrieve all the relevant data')
st.write('(Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video)')
st.write('Using Google API. Option to store the data in a MongoDB database as a data lake.') 
st.write('Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button. ') 
st.write('Option to select a channel name and migrate its data from the data lake to a SQL database as tables. ') 
st.write('Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.')




with st.sidebar:
    if st.button("Home"):
        st.switch_page("your_app.py")
    if st.button("DataFrame"):
        st.switch_page("pages/DataFrame_page.py")
    if st.button("Query"):
        st.switch_page("pages/Query_page.py")
    if st.button("DataScrub"):
        st.switch_page("pages/Data Scrab.py")
    if st.button("Code_page")
    

        



















