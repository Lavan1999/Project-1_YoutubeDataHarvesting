   
import streamlit as st
import psycopg2
import pandas as pd
import time
from PIL import Image
import plotly.express as px
        

st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]')

st.header(":green[Query Data]")

questions = st.selectbox("Choose your question",
            ("1. Names of all the videos and their channels",
            "2. Channels with most number of videos, and count",
            "3. Top 5 most viewed videos and their channels",
            "4. Comments with each video, and video names",
            "5. Videos with highest likes, and their channel names",
            "6. Total number of likes and dislikes for each video, and video names",
            "7. Total number of views for each channel, and channel names",
            "8. Published videos in the year 2022",
            "9. Average views of all videos in each channel, and channel names",
            "10. videos with highest number of comments, and channel names"))



mydb = psycopg2.connect(
                        host = 'localhost',
                        user = 'postgres',
                        password = 'Lavan123',
                        database = 'youtubepro',
                        port = 5432)
mycursor = mydb.cursor()


if questions == "1. Names of all the videos and their channels":
    query = """
                SELECT video_name, channel_name FROM video_table
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df1 = pd.DataFrame(table, columns=["Name of video", "Name of channel"])
    st.header(':violet[Table]')
    st.table(df1)
    

elif questions == "2. Channels with most number of videos, and count":
    query = """    
                select channel_name, count(video_id) as videocount from video_table
                        group by channel_name order by videocount desc limit 1 
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df2 = pd.DataFrame(table, columns=["channel name", "video counts"])
    st.header(':violet[Table]')
    st.table(df2)
    

elif questions ==  "3. Top 5 most viewed videos and their channels":
    query = """
                select channel_name, sum(view_count) as views_count from video_table
                group by channel_name order by views_count desc limit 5
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df3 = pd.DataFrame(table, columns=["channel name", "view counts"])
    st.header(':violet[Table and visulization]')
    st.table(df3)
    st.header(':violet[Visualization: Top 5 most viewed videos]')
    fig = px.bar(df3,
        y = 'channel name',
        x = 'view counts',
        orientation = 'h'
    )
    st.plotly_chart(fig,use_container_width=True)

    
    

elif questions == "4. Comments with each video, and video names":
    query = """
                select video_table.video_name, sum(comment_count) as comments_count from video_table join comment_table
                    on (video_id = comment_video_id) group by video_name order by comments_count desc
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df4 = pd.DataFrame(table, columns=["video name", "comment counts"])
    st.header(':violet[Table]')
    st.table(df4)
    

elif questions == "5. Videos with highest likes, and their channel names":
    query = """
                select channel_name,video_name, like_count as likes_count from video_table
                    order by likes_count desc
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df5 = pd.DataFrame(table, columns= ["Name of channel","Video name","like counts"])
    st.header(':violet[Visualization: Channels names with highest likes]')
    fig = px.bar(df5,
           y = 'Name of channel',
           x = 'like counts',
           orientation = 'h')
    st.plotly_chart(fig,use_container_width=True)
    st.header(':violet[Table]')
    st.table(df5)
    

elif questions == "6. Total number of likes and dislikes for each video, and video names" :
    query = """
                select video_name, sum(like_count)as likes_count, sum(dislike_count) as dislikes_count from video_table
                    group by video_name order by likes_count desc
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df6 = pd.DataFrame(table, columns=["Name of video", "Likes count","Dislike count"])
    st.header(':violet[Table]')
    st.table(df6)
    

elif questions == "7. Total number of views for each channel, and channel names":
    query = """
                select vt.channel_name, sum(vt.view_count) as views_count from video_table as vt
                group by vt.channel_name order by views_count
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df7 = pd.DataFrame(table, columns=["Name of Channel", "View Counts"])
    st.header(':violet[Table]')
    st.table(df7)

    
elif questions == "8. Published videos in the year 2022":
    query = """
                select distinct(channel_name) from video_table
                    where published_date like '%2020%' 

            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df8 = pd.DataFrame(table, columns=["Channel published videos in the year 2022"])
    st.header(':violet[Table]')
    st.table(df8)
    
    
elif questions == "9. Average views of all videos in each channel, and channel names":
    query = """
                select channel_name, round(avg(view_count)) as avg_views from video_table
                    group by channel_name order by avg_views desc
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df9 = pd.DataFrame(table, columns=["Channel names", "Average views"])
    st.header(':violet[Table and Visulization]')
    st.table(df9)
    st.header(':violet[Visualization: Average views of channels]')
    fig = px.bar(df9,
        y = 'Channel names',
        x = 'Average views',
        orientation = 'h'
    )
    st.plotly_chart(fig,use_container_width=True)
    
    
elif questions == "10. videos with highest number of comments, and channel names":
    query = """
                select channel_name, sum(comment_count) as comments_count from video_table 
                        group by channel_name order by comments_count desc
            """
    mycursor.execute(query)
    table = mycursor.fetchall()
    df10 = pd.DataFrame(table, columns=["channel name", "comment counts"])
    st.header(':violet[Table and visualization]')
    st.table(df10)
    st.header(':violet[Visualization: Channels comment count]')
    fig = px.bar(df10,
        y = 'channel name',
        x = 'comment counts',
        orientation = 'h'
    )
    st.plotly_chart(fig,use_container_width=True)
