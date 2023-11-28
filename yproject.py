from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime,timedelta,timezone
import re



# api connection

def Api_connection():
    Api_id="AIzaSyCU7Xl7X8-OfTa6NTXf3VQbZIP7x2eCBjQ"

    api_service_name="youtube"
    api_version="v3"
   
    youtube=build(api_service_name,api_version,developerKey=Api_id)

    return youtube

youtube=Api_connection()


def channel_info(channel_id):
    request=youtube.channels().list(
                part="snippet,ContentDetails,statistics",
                id=channel_id

    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def convert_date_time(DT):
     youtueb_format="%Y-%m-%dT%H:%M:%SZ"
     Published_Date ="%Y-%m-%d %H:%M:%S"

     youtube_datetime=datetime.strptime(DT,youtueb_format).replace(tzinfo=timezone.utc)
     # Convert to the local timezone (you can adjust this based on your application's timezone)
     local_timezone = timezone(timedelta(hours=5, minutes=30))  # Adjust to your desired timezone
     local_datetime = youtube_datetime.astimezone(local_timezone)

     return local_datetime.strftime(Published_Date)

def get_min(duration):
    hour=re.search(r"(\d+)H",duration)
    hour= hour.group(1)if hour else 0
    minutes=re.search(r"(\d+)M",duration)
    minutes= minutes.group(1) if minutes else 0

    return int(hour)*60 + int(minutes)

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)
        response = request.execute()
        for item in response["items"]:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags') if item['snippet'].get('tags') is None else ','.join([str(elem) for elem in item['snippet'].get('tags')]),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=convert_date_time(item['snippet']['publishedAt']),
                        Dutation=get_min(item['contentDetails']['duration']),
                        View_Count=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)

    return video_data

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50)
            
            resonse=request.execute()

            for item in resonse['items']:
                data=dict(Comment_Id=item ['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item ['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                                part='snippet,contentDetails',
                                channelId=channel_id,
                                maxResults=50,
                                pageToken=next_page_token)

                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                        Title=item['snippet']['title'],
                                        Channel_Id=item ['snippet']['channelId'],
                                        Channel_Name=item ['snippet']['channelTitle'],
                                        PublishedAt=item['snippet']['publishedAt'],
                                        Video_count=item['contentDetails'] )
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


client=pymongo.MongoClient('mongodb://localhost:27017/')
db=client['Youtube_data']

def channel_details(channel_id):
    ch_details=channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    
    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'playlist_info':pl_details,
    'video_information':vi_details,'comment_information':com_details})

    return "upload successfully"

def channel():

    mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='janak123',
                            database='youtubeproject'   )
            
    mycursor= mydb.cursor()
  

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()


    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers int,
                                                            Views int,
                                                            Total_videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        mycursor.execute(create_query)
        mydb.commit()

    except:
        print('Table already created')



    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)


    for index, row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers ,
                                            Views,
                                            Total_videos ,
                                            Channel_Description,
                                            Playlist_Id )
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        
        except:
            print('Values already inserted')

def playlist_table():

        mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='janak123',
                            database='youtubeproject'   )
            
        mycursor= mydb.cursor()
        

        drop_query='''drop table if exists playlists'''
        mycursor.execute(drop_query)
        mydb.commit()



        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt varchar(100),
                                                        Video_count int
                                                        )'''
        mycursor.execute(create_query)
        mydb.commit()

        pl_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for pl_data in coll1.find({},{'_id':0,'playlist_info':1}):
                for i in range(len(pl_data['playlist_info'])):
                        pl_list.append(pl_data['playlist_info'][i])
        df1=pd.DataFrame(pl_list)


        for index, row in df1.iterrows():
                insert_query ='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id ,
                                                Channel_Name ,
                                                PublishedAt,
                                                Video_count)
                                                values(%s,%s,%s,%s,%s,%s)'''
                                                
                values = (row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_count']['itemCount']
                        )
                
                mycursor.execute(insert_query,values)
                mydb.commit( )

def videos():
    mydb=mysql.connector.connect(host='localhost',
                                    user='root',
                                    password='janak123',
                                    database='youtubeproject' )
    mycursor= mydb.cursor()

    drop_query='''drop table if exists video1'''
    mycursor.execute(drop_query)
    mydb.commit()

   

    create_query='''create table if not exists videos(Channel_Name varchar(1000),
                        Channel_Id varchar(100),
                        Video_Id varchar(500),
                        Title varchar(2000),
                        Description text,
                        Published_Date varchar(100),
                        Dutation varchar(100),
                        View_count int,
                        Likes int,
                        Comments int
                            )'''
    mycursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df3=pd.DataFrame(vi_list)
    
   

        
    for index, row in df3.iterrows():
                insert_query ='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Description,
                                                    Published_Date,
                                                    Dutation,
                                                    View_count,
                                                    Likes,
                                                    Comments
                                                    )
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                    
                values = (row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Description'],
                        row['Published_Date'],
                        row['Dutation'],
                        row['View_Count'],
                        row['Likes'],
                        row['Comments'])
                
                mycursor.execute(insert_query,values)
                
                mydb.commit( )


def comments():
    mydb=mysql.connector.connect(host='localhost',
                                user='root',
                                password='janak123',
                                database='youtubeproject'   )
                
    mycursor= mydb.cursor()

    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()



    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                        Video_Id varchar(50),
                                                        Comment_text text,
                                                        Comment_Author varchar(200),
                                                        Comment_Published varchar(50) 
                                                    )'''
    mycursor.execute(create_query)
    mydb.commit()


    comment_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for comment_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df2=pd.DataFrame(comment_list)


    for index, row in df2.iterrows():
            insert_query ='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_text,
                                                    Comment_Author,
                                                    Comment_Published  
                                                    )
                                            values(%s,%s,%s,%s,%s)'''


            values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )
            
            mycursor.execute(insert_query,values)
            mydb.commit( )


def tables():
    channel()
    playlist_table()
    videos()
    comments()

    return "Tables created"

def channel_table():
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df

def display_playlist_table():
        pl_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for pl_data in coll1.find({},{'_id':0,'playlist_info':1}):
                for i in range(len(pl_data['playlist_info'])):
                        pl_list.append(pl_data['playlist_info'][i])
        df1=st.dataframe(pl_list)

        return df1

def display_video_table():
    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df3=st.dataframe(vi_list)

    return df3


def display_comments_table():
    comment_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for comment_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(comment_data['comment_information'])):
            comment_list.append(comment_data['comment_information'][i])
    df2=st.dataframe(comment_list)

    return df2


# stremelit

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Take aways of projetc")
    st.header("Youtube API connection")
    st.caption("python code")
    st.caption("Data collection from youtube")
    st.caption("pushing data from MongoDB to SQL")
    st.caption("creation of stremlit page")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if  channel_id in ch_ids:
        st.success("channel id is already stored")  

    else:
        insert= channel_details(channel_id)
        st.success(insert)

if st.button("store to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select the variable to view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    channel_table()

elif show_table=="VIDEOS":
    display_video_table()

elif show_table=="COMMENTS":
    display_comments_table()

elif show_table=="PLAYLISTS":
    display_playlist_table()


mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='janak123',
                            database='youtubeproject'   )
            
mycursor= mydb.cursor()


Questions=st.selectbox("select your question",("1.What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos do they have?",
                                               "3. What are the top 10 most viewed videos and their respective channels?",
                                               "4. How many comments were made on each video, and what are their corresponding video names?",
                                               "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8. What are the names of all the channels that have published videos in the year 2022?",
                                               "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                                               ))



if Questions=="1.What are the names of all the videos and their corresponding channels?":
    question1='''select title as videos,Channel_Name as channelname from videos'''
    mycursor.execute(question1)
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos title","channel name"])
    mydb.commit()
    st.write(df)

elif Questions=="2.Which channels have the most number of videos, and how many videos do they have?":
    question2='''select Channel_Name as channelname,Total_videos as no_videos from channels 
                    order by Total_videos desc'''
    mycursor.execute(question2)
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel","Total no of videos"])
    mydb.commit()
    st.write(df2)

elif Questions=="3. What are the top 10 most viewed videos and their respective channels?":

    question3='''select View_count as Views,Channel_Name as channelname,Title as videotitle from videos
                    where View_count is not null order by View_count desc limit 10'''
    mycursor.execute(question3)
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
    mydb.commit()

    st.write(df3)

elif Questions=="4. How many comments were made on each video, and what are their corresponding video names?":

    question4='''select Comments as Comments,Title as video_title from videos where Comments is not null'''
    mycursor.execute(question4)
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of Comments","video title"])
    mydb.commit()

    st.write(df4)

elif Questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":

    question5='''select Title as Title,Channel_Name as Channelname,Likes as Likes from videos where Likes is not null order by Likes desc'''
    mycursor.execute(question5)
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Title","Channel","Likes"])
    mydb.commit()

    st.write(df5)


elif Questions=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

    question6='''select Likes as Likes,Title as VideoTitle from videos '''
    mycursor.execute(question6)
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Total likes","Video Title"])
    mydb.commit()

    st.write(df6)

elif Questions=="7. What is the total number of views for each channel, and what are their corresponding channel names?":

    question7='''select Views as Total_Views,Channel_Name as ChannelName from channels '''
    mycursor.execute(question7)
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Total views","Channel Name"])
    mydb.commit()

    st.write(df7)


elif Questions=="8. What are the names of all the channels that have published videos in the year 2022?":
    question8='''select Title as Title,Published_Date as video_uploded,Channel_Name as ChannelName from videos where extract(year from Published_Date)=2022 '''
    mycursor.execute(question8)
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Title","video uploded","Channel Name"])
    mydb.commit()

    st.write(df8)


elif Questions=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    question9='''select Channel_Name as Channelname,AVG(Dutation) as AverageDutation from videos group by Channel_Name'''
    mycursor.execute(question9)
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channelname","AverageDutation"])
    mydb.commit()

    T9=[]
    for index, row in df9.iterrows():
        Channel=row["Channelname"]
        average_duration=row["AverageDutation"]
        average_duration_str=str(average_duration)
        T9.append(dict(Channelname=Channel,avgd=average_duration_str))
    dfd=pd.DataFrame(T9)
    st.write(dfd)

elif Questions=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":

    question10='''select Title as Title,Channel_Name as Channelname,Comments as Comments from videos where Comments is not null order by Comments desc '''
    mycursor.execute(question10)
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Title","Channelname","Comments"])
    mydb.commit()

    st.write(df10)