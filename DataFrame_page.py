import streamlit as st
import time
from Main_page import dfc, dfp, dfv, dfm
  

st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
st.header(':green[DataFrame]')

st.toast(':white[Happy to diploy my project, Tq for watching!!!]',icon ='🎉') 
time.sleep(5)

show_tables = st.radio('Choose the Table for view..',
                    ("Channel Table", "Playlist Table", "Video Table", "Comment Table"))



if show_tables == "Channel Table":
    st.dataframe(dfc)

elif show_tables == "Playlist Table":
    st.dataframe(dfp)

elif show_tables == "Video Table":
    st.dataframe(dfv)
    
elif show_tables == "Comment Table":
    st.dataframe(dfm)
