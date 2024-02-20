
'''Collected Youtube datas stored in mongodb.So that we need to connect the mongodb
in python. Created database and collection for to store the datas. By INSERT ONE method
I can inserted the datas by using the channel Id'''
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

'''Now the inserted datas are ready to convert DataFrame. To get required datas for 
the dataframe using query of mongodb. I used pandas for converting the dtype,
dataframe, data cleaning process. Here is the 4 dataframe for channel, playlist,
video, comment'''

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




#Playlist Dataframe
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




#Comment dataframe
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
