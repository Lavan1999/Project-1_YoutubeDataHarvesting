
import streamlit as st 
from Main_page import extracting_Data,alltables,channel_details
import pymongo
import pandas as pd


#Mongodb connection

chl_id = st.text_input('Enter the channel ID')

if st.button(':yellow[Store data]'):
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
        
if st.button(':violet[Add to Table]'):
    Tables=alltables()
    st.success(Tables)

if chl_id:    
    ch_name = []
    ch_view = []
    for i in collection.find({},{'_id':0,'channel_details':1}):
        ch_name.append(i['channel_details']['channel_name'])
        ch_view.append(i['channel_details']['channel_views'])
        dfc = pd.DataFrame(ch_name)

    

    