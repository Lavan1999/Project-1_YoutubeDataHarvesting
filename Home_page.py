import googleapiclient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pymongo
import pandas as pd 
from sqlalchemy import create_engine, Text, String, Interval,Integer,Date, PrimaryKeyConstraint
from sqlalchemy.exc import SQLAlchemyError
import psycopg2



#1 Connecting youtube api------------

api_key = 'AIzaSyB2UG9mxzeVrqqI916PGYnCI8JLKrLnrxQ'
api_service_name = "youtube"
api_version = "v3"

youtube = build(api_service_name, api_version, developerKey = api_key)




#2 Getting youtube channel details------------

def channel_details(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id= channel_id).execute()

    for i in request['items']:
        channel_information = dict(
        channel_id = i['id'],
        channel_name = i['snippet']['title'],
        subscriber_count = i['statistics'].get('subscriberCount'),
        playlists_id = i['contentDetails']['relatedPlaylists']['uploads'],
        )
    return channel_information


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
    # Log the error for debugging purposes
    except HttpError as e:
        if e.resp.status == 403:
            print(f"Comment are diabled :{video_id}.")
        else:
            print(f"An error occured while fetching comments {video_id}:{e}")
        


    return comment_datas

    
    
#connecting mongodb (nosql) ----------------

client = pymongo.MongoClient('mongodb+srv://lavanya:Lavan123@guvilavan.5pjwpvl.mongodb.net/?retryWrites=true&w=majority')
db = client['youtubeproject']
collection = db['Youtube_details']



# To get all the data of the given channel

def extracting_Data(channel_id):
    channel_data = channel_details(channel_id)
    videoIds = video_ids(channel_id)
    video_data = video_details(videoIds)
    comment_data = comment_details(videoIds)
        
    Data = {'channel_details': channel_data,
                'video_details': video_data, 
                'comment_details': comment_data}
        
    collection.insert_one(Data)
        
    return 'Succesfully inserted!!!'


#Channel DataFrame
# converting DataFrame for channels

def channeltable(option):
    channel = []
    ch = collection.find_one({"channel_details.channel_name":option},{'_id':0,'channel_details':1})
    channel.append(ch['channel_details'])
    dfc = pd.DataFrame(channel)
    dfc['subscriber_count'] = dfc['subscriber_count'].astype(int)

    # Define database connection
    engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubepro')
        

    # Define table schema
    channel_table = {
                    'channel_id':String,
                    'channel_name': String,
                    'subscriber_count': Integer,
                    'playlists_id': String
    }
        
    # Convert DataFrame to SQL table
    dfct = dfc.to_sql('channel_table', engine, if_exists='append', index=False, dtype = channel_table)
    return dfct

 


# Video Dataframe
# Converting DataFrame for Videos

def videotable(option):
    video = []
    for i in collection.find({"channel_details.channel_name":option},{'_id':0, 'video_details':1}):
        video.append(i['video_details'])
    video_data = [i for sublist in video for i in sublist] 
    dfv = pd.DataFrame(video_data)
    dfv.fillna(0,inplace=True)


    # Convert 'published_date' column to datetime objects
    dfv['published_date'] = pd.to_datetime(dfv['published_date'])
    dfv['video_description'] = dfv['video_description'].replace('', "No description")

    # Format 'published_date' as 'ddmmyyyy'
    dfv['published_date'] = dfv['published_date'].dt.strftime('%d-%m-%Y')

        
    # Define database connection
    engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubepro')

    # Define table schema
    video_table = {
                    'video_name': String,
                    'video_id': String,
                    'channel_id': String,
                    'channel_name': String,
                    'video_description': String,
                    'published_date': Date,
                    'view_count': Integer,
                    'dislike_count': Integer,
                    'like_count': Integer,
                    'favorite_count': Integer,
                    'comment_count': Integer,
                    'duration': Interval,
                    'thumbnail': String,
                    'caption_status': String
    }

    # Convert DataFrame to SQL table
    dfvt = dfv.to_sql('video_table', engine, if_exists='append', index=False, dtype = video_table)
    print('Data inserted into the table')
    return dfvt


#Comment dataframe, table

def commenttable(option):    
    
    comment = []
    for i in collection.find({'channel_details.channel_name':option},{'_id':0, 'comment_details': 1}):
        comment.append(i['comment_details'])
    comment_data = [i for sublist in comment for i in sublist]
    dfm = pd.DataFrame(comment_data)

    # Convert 'published_date' column to datetime objects
    dfm['published_date'] = pd.to_datetime(dfm['published_date'])

    # Format 'published_date' as 'ddmmyyyy'
    dfm['published_date'] = dfm['published_date'].dt.strftime('%d-%m-%Y')

    # Define database connection
    engine = create_engine('postgresql://postgres:Lavan123@localhost:5432/youtubepro')

    # Define table schema
    comment_table = {
                    'comment_id':String,
                    'comment_video_id': String,
                    'comment_text': Text,
                    'comment_author': String,
                    'published_date': Date
    }
    # Convert DataFrame to SQL table
    dfmt = dfm.to_sql('comment_table', engine, if_exists='append', index=False, dtype= comment_table)
    return dfmt



# To convert all the dataframe to tables

def alltables(option):
    channeltable(option)
    videotable(option)
    commenttable(option)
    return 'Tables created succesfully'



#Streamlit UI codes------------------

import streamlit as st
import time


st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')   
st.toast(':rainbow[Welcome to my youtube project UI]',icon ='🎉') 
time.sleep(5)



with st.sidebar:
    st.header(':violet[Process of the project]')
    st.write('1.Data Collection')
    st.write('2.Insert data to NOsql')
    st.write('3.Converting DataFrame & cleaning')
    st.write('5.Converting into sql table')
    st.write('6.Steamlit outlook')
    
   
   
#Data Scrap and table
#Inserting new channel to scrap

chl_id = st.text_input('## :green[Enter the channel ID]')

if st.button(':violet[Upload to mongodb]'):
    ch_ids = []
    client = pymongo.MongoClient('mongodb+srv://lavanya:Lavan123@guvilavan.5pjwpvl.mongodb.net/?retryWrites=true&w=majority')
    db = client['youtubeproject']
    collection = db['Youtube_details']
    for ch_data in collection.find({},{'_id':0,'channel_details':1}):
       ch_ids.append(ch_data['channel_details']['channel_id'])
    
    if chl_id in ch_ids:
        st.success('Channel Details already exist, give other channel id.')
    else:
        insert = extracting_Data(chl_id)
        st.success(insert)
        


#Selected channel to migrate in SQL
ch_nms = []
client = pymongo.MongoClient('mongodb+srv://lavanya:Lavan123@guvilavan.5pjwpvl.mongodb.net/?retryWrites=true&w=majority')
db = client['youtubeproject']
collection = db['Youtube_details']
for ch_data in collection.find({},{'_id':0,'channel_details':1}):
    ch_nms.append(ch_data['channel_details']['channel_name'])
option = st.selectbox(":green[Choose channel to migrate]", ch_nms, placeholder='Choose channel name')
if st.button(':violet[Covert to Table]'):
    
    mydb = psycopg2.connect(
                        host = 'localhost',
                        user = 'postgres',
                        password = 'Lavan123',
                        database = 'youtubepro',
                        port = 5432)
    
    mycursor = mydb.cursor()
    ch_nms = []
    query = "SELECT channel_name FROM channel_table"
    try:
        mycursor.execute(query)
        result = mycursor.fetchall()
        for i in result:
            ch_nms.append(i[0])
    except Exception as e:
        st.error(f"An error occurred: {e}")

    if option in ch_nms:
        st.error("Given channel already exists")
    else:
        alltables(option)
        st.success("Channel information added successfully")



#Setting switch page
        
if st.button("Home"):
    st.switch_page("Home_page.py")
if st.button("Query"):
    st.switch_page("pages/Query_page.py")
if st.button('Workflow'):
    st.switch_page('pages/Project Workflow.py') 














