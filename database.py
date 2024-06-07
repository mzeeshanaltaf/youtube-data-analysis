import pymongo
import psycopg2
import pandas as pd
from yt_detail import *
import streamlit as st


def sql_db_connection():
    mydb = psycopg2.connect(host="localhost", user="postgres", password="mUhammad", database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()
    return mydb, cursor


def mongodb_connection():
    # Establish Connection with MongoDB
    client = pymongo.MongoClient(
        "mongodb+srv://altafz:uvc3Y6W9KIAnpFJI@zeeshan.ybtmt9f.mongodb.net/?retryWrites=true&w=majority&appName=zeeshan")
    return client


def channel_details(channel_id):
    bar = st.progress(0, text="Getting Channel Details...")
    ch_details = get_channel_info(channel_id)
    bar.progress(10, text="Channel details extracted. Getting Playlist Details...")
    pl_details = get_playlist_details(channel_id)
    bar.progress(20, text="Playlist details extracted. Getting Video Ids Details...")
    vi_ids = get_video_ids(channel_id)
    bar.progress(40, text="Video Ids details extracted. Getting Video Details...")
    vi_details = get_video_info(vi_ids)
    bar.progress(70, text="Video details extracted. Getting Comment Details...")
    com_details = get_comment_info(vi_ids)
    bar.progress(90, text="Comment details extracted. Inserting into database...")
    client = mongodb_connection()
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information": pl_details,
                      "video_information": vi_details, "comment_information": com_details})

    bar.progress(100, text="Successfully Completed")
    bar.empty()
    return "Added to database successfully"


def get_channel_names():
    client = mongodb_connection()

    ch_names = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_names.append(ch_data['channel_information']['Channel_Name'])

    return ch_names


def channels_table(ch_name):
    mydb, cursor = sql_db_connection()

    create_query = '''create table if not exists channels(Channel_Name varchar(100), 
                                                            Channel_Id varchar(80) primary key, 
                                                            Subscribers bigint, 
                                                            Views bigint, 
                                                            Total_Videos int, 
                                                            Channel_Description text, 
                                                            Playlist_Id varchar(80))
                                                            '''
    cursor.execute(create_query)
    mydb.commit()

    client = mongodb_connection()

    ch_detail = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": ch_name}, {"_id": 0}):
        ch_detail.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_detail)

    for index, row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name, 
        Channel_Id, 
        Subscribers,
        Views, 
        Total_Videos, 
        Channel_Description, 
        Playlist_Id)

        values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscriber'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            status = f"{ch_name} YouTube Channel already exists in the DB"
            mydb.rollback()
            return status


def playlist_table(ch_name):
    mydb, cursor = sql_db_connection()

    create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key, 
                                                            Title varchar(100), 
                                                            Channel_Id varchar(100), 
                                                            Channel_Name varchar(100), 
                                                            PublishedAt timestamp, 
                                                            Video_Count int)
                                                            '''
    cursor.execute(create_query)
    mydb.commit()

    client = mongodb_connection()

    pl_detail = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": ch_name}, {"_id": 0}):
        pl_detail.append(ch_data["playlist_information"])

    df_pl = pd.DataFrame(pl_detail[0])

    for index, row in df_pl.iterrows():
        insert_query = '''insert into playlists(Playlist_Id, 
        Title, 
        Channel_Id,
        Channel_Name, 
        PublishedAt, 
        Video_Count)

        values(%s,%s,%s,%s,%s,%s)'''

        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  row['PublishedAt'],
                  row['Video_Count'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            mydb.rollback()


def videos_table(ch_name):
    mydb, cursor = sql_db_connection()

    create_query = '''create table if not exists videos(Channel_Name varchar(100), 
                                                        Channel_Id varchar(100), 
                                                        Video_Id varchar(30) primary key,
                                                       Title varchar(150),
                                                       Tags text,
                                                       Thumbnail varchar(200),
                                                       Description text,
                                                       Published_Date timestamp,
                                                       Duration interval,
                                                       Views bigint,
                                                       Likes bigint,
                                                       Comments int,
                                                       Favorite_Count int,
                                                       Definition varchar(10),
                                                       Caption_status varchar(50))
                                                            '''
    cursor.execute(create_query)
    mydb.commit()

    client = mongodb_connection()

    vi_detail = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": ch_name}, {"_id": 0}):
        vi_detail.append(ch_data["video_information"])

    df_vi = pd.DataFrame(vi_detail[0])

    for index, row in df_vi.iterrows():
        insert_query = '''insert into videos(Channel_Name, 
                                            Channel_Id, 
                                            Video_Id,
                                           Title,
                                           Tags,
                                           Thumbnail,
                                           Description,
                                           Published_Date,
                                           Duration,
                                           Views,
                                           Likes,
                                           Comments,
                                           Favorite_Count,
                                           Definition,
                                           Caption_status)

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_status'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            mydb.rollback()


def comments_table(ch_name):
    mydb, cursor = sql_db_connection()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp)
                                                            '''
    cursor.execute(create_query)
    mydb.commit()

    client = mongodb_connection()

    co_detail = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": ch_name}, {"_id": 0}):
        co_detail.append(ch_data["comment_information"])

    df_co = pd.DataFrame(co_detail[0])

    for index, row in df_co.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)

                                        values(%s,%s,%s,%s,%s)'''

        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  row['Comment_Published'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            mydb.rollback()


def tables(channel_name):
    status = channels_table(channel_name)
    if status:
        return status
    else:
        playlist_table(channel_name)
        videos_table(channel_name)
        comments_table(channel_name)

    return "Successfully Migrated data to SQL DB"


def show_channels_table():
    client = mongodb_connection()

    ch_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    st.dataframe(ch_list, hide_index=False)
    return len(ch_list)


def show_playlists_table():
    client = mongodb_connection()

    pl_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])

    st.dataframe(pl_list, hide_index=False)
    return len(pl_list)


def show_videos_table():
    client = mongodb_connection()

    vi_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])

    st.dataframe(vi_list, hide_index=False)
    return len(vi_list)


def show_comments_table():
    client = mongodb_connection()

    co_list = []
    db = client["Youtube_Data"]
    coll1 = db["channel_details"]
    for co_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(co_data['comment_information'])):
            co_list.append(co_data['comment_information'][i])

    st.dataframe(co_list, hide_index=False)
    return len(co_list)
