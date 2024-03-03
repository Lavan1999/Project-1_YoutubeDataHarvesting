# YouTube-Project
YouTube Data Harvesting and Warehousing using MongoDB, SQL and Streamlit

**WORKFLOW**

Data Collection:

  1.The first step is to collect data from the YouTube using the YouTube Data API. 
  2.The API and the Channel ID (Extracted from the Channel Page) is used to retrieve channel details, 
    videos details and comment details. 
  3.I have used the Google API client library for Python to make requests to the API and the responses 
    are Collected as a Dictionary (Data Collection)
    
Inserting(Storing) the Collected Data to MongoDB:

  1.Once the Data Collection is done, store it in MongoDB, which is a NoSQL Database great choice for 
  handling unstructured and semi-structured data.
  
Data Migration to SQL:

  1.After Loading all the data into MongoDB, it is then migrated to a structured postgres data warehouse.
  2.Then used SQL queries to join the tables and retrieve data for specific channels.
  
Data Analysis and Data Visualization:

  1.With the help of SQL query, I have got many interesting insights about the youtube channels.
  2.Finally, the data retrieved from the SQL is displayed using the Streamlit Web app.
  3.Streamlit is used to create dashboard that allows users to visualize and analyze the data. 
  4.Also used Plotly for the Data Visualization to create charts and graphs to analyze the data.


**HOME PAGE**
To scrap the YouTube channel details:


![Screenshot (159)](https://github.com/Lavan1999/YouTube-Project/assets/152668558/ddc4ffd6-0fa5-4260-8b2a-483af17dfca1)


**SELECT CHANNEL TO CONVERT INTO TABLE**

![Screenshot (160)](https://github.com/Lavan1999/YouTube-Project/assets/152668558/c424e8d8-9052-43b0-b7f1-f075f67e7a98)


**QUERY PAGE**

![Screenshot (161)](https://github.com/Lavan1999/YouTube-Project/assets/152668558/ba94d51e-417d-453b-83e0-c0cfa5afd76d)


**WORKFLOW OF THE PROJECT**

![Screenshot (162)](https://github.com/Lavan1999/YouTube-Project/assets/152668558/89d0a7a9-f359-49d6-9a9c-0c2f09527c51)



