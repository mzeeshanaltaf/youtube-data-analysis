# List of questions, respective sql query to extract answers from sql database and column names for pandas dataframe
questions = [
    {
        "question": "1. All the videos and the channel name",
        "sql": "select title as videos, channel_name as channelname from videos",
        "df_cols": ['Video Title', 'Channel Name'],
    },
    {
        "question": "2. Channels with most number of videos",
        "sql": "select channel_name as channelname, total_videos as no_videos from channels order by total_videos desc",
        "df_cols": ['Channel Name', 'Number of Videos'],
    },
    {
        "question": "3. 10 most viewed videos",
        "sql": "select views as views, channel_name as channelname, title as videotitle from videos "
               "where views is not null order by views desc limit 10",
        "df_cols": ['Views', 'Channel Name', 'Video Title'],
    },
    {
        "question": "4. Comments in each videos",
        "sql": "select comments as no_commnets, title as videotitle from videos where comments is not null",
        "df_cols": ['No. of Comments', 'Video Title'],
    },
    {
        "question": "5. Videos with highest likes",
        "sql": "select title as videotitle, channel_name as channelname, likes as likecount from videos "
               "where likes is not null order by likes desc",
        "df_cols": ['Video Title', 'Channel Name', 'No of Likes'],
    },
    {
        "question": "6. Likes of all videos",
        "sql": "select likes as likecount, title as videotitle from videos",
        "df_cols": ['No of Likes', 'Video Title'],
    },
    {
        "question": "7. Views of each Channel",
        "sql": "select channel_name as channelname, views as totalviews from channels",
        "df_cols": ['Channel Title', 'Total Views'],
    },
    {
        "question": "8. Videos published in the year of 2023",
        "sql": "select title as videotitle, published_date as videorelease, channel_name as channelname from videos "
               "where extract(year from published_date) = 2023",
        "df_cols": ['Video Title', 'Published Year', 'Channel_Name'],
    },
    {
        "question": "9. Average duration of all videos in each channel",
        "sql": "select channel_name as channelname, AVG(duration) as average_duration from videos "
               "group by channel_name",
        "df_cols": ['Channel_Name', 'Avg Duration'],
    },
    {
        "question": "10. Videos with highest number of comments",
        "sql": "select title as videotitle, channel_name as channelname, comments as commentcount from videos "
               "where comments is not null order by comments desc",
        "df_cols": ['Video Title', 'Channel_Name', 'No of Comments'],
    },

]