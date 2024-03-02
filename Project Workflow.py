import streamlit as st

st.title(':blue[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')

st.text_area("## :green[About Data harvesting]",
              'data harvesting is the process of extracting data from a given source.' 
              'This data can be obtained from a variety of sources, such as: Websites ,online surveys'
              'args customer feedback forms,Social media posts, Data sets.'
              'Application programming interfaces (APIs) act as lines of communication between different databases.')


st.header(":green[Workflow of the project]")
st.header(':violet[Data Collection:]')

st.write('1.The first step is to collect data from the YouTube using the YouTube Data API.')
st.write('2.The API and the Channel ID is used to retrieve channel details, ')
st.write('videos details and comment details. ')

    
st.header(':violet[Inserting(Storing) the Collected Data to MongoDB:]')

st.write('1.Once the Data Collection is done, stored it in MongoDB, which is a NoSQL Database great choice for ')
st.write('handling unstructured and semi-structured data.')

st.header(':violet[Data cleaning and converting to DataFrame]')

st.write('1.After Loading all the data into MongoDB, I extracted the data to converting Dataframe by python library of pandas.')
st.write('2.Then I used pandas for cleaning the Data.')
  
st.header(':violet[Data Migration to SQL:]')

st.write('1. After cleaning process, it is then migrated to a structured Postgres data warehouse.')
st.write('2.Then used SQL queries to join the tables and retrieve data for specific channels.')
  
st.header(':violet[Data Analysis and Data Visualization:]')

st.write('2.Finally, the data retrieved from the SQL is displayed using the Streamlit Web app.')
st.write('4.Also used Plotly for the Data Visualization to create charts and graphs to analyze the data.')