from googleapiclient.discovery import build


def api_connect():
    api_id = 'AIzaSyAYaGIzAKpJ8Fhj45-S1t8eAODjyRj6RcE'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_id)
    return youtube


def get_channel_info(channel_id):

    # Establish Connection with YouTube Service
    youtube = api_connect()

    request = youtube.channels().list(
        part="snippet, contentDetails, statistics",
        id=channel_id,
    )
    response = request.execute()
    for detail in response['items']:
        data = dict(Channel_Name=detail['snippet']['title'],
                    Channel_Id=detail['id'],
                    Subscriber=detail['statistics']['subscriberCount'],
                    Views=detail['statistics']['viewCount'],
                    Total_Videos=detail['statistics']['videoCount'],
                    Channel_Description=detail['snippet']['description'],
                    Playlist_Id=detail['contentDetails']['relatedPlaylists']['uploads']
                    )
    return data


def get_video_ids(channel_id):

    # Establish Connection with YouTube Service
    youtube = api_connect()

    channel_details = get_channel_info(channel_id)
    playlist_id = channel_details['Playlist_Id']
    next_page_token = None
    video_ids = []

    while True:
        response_playlist = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()

        for vid in response_playlist['items']:
            video_ids.append(vid['snippet']['resourceId']['videoId'])
        next_page_token = response_playlist.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids


def get_video_info(video_ids):

    # Establish Connection with YouTube Service
    youtube = api_connect()

    video_data = []
    for index, vid in enumerate(video_ids):
        request = youtube.videos().list(
            part="snippet, ContentDetails, statistics",
            id=vid
        )
        response = request.execute()
        for detail in response["items"]:
            data = dict(Channel_Name=detail['snippet']['channelTitle'],
                        Channel_Id=detail['snippet']['channelId'],
                        Video_Id=detail['id'],
                        Title=detail['snippet']['title'],
                        Tags=detail['snippet'].get('tags'),
                        Thumbnail=detail['snippet']['thumbnails']['default']['url'],
                        Description=detail['snippet'].get('description'),
                        Published_Date=detail['snippet']['publishedAt'],
                        Duration=detail['contentDetails']['duration'],
                        Views=detail['statistics'].get('viewCount'),
                        Likes=detail['statistics'].get('likeCount'),
                        Comments=detail['statistics'].get('commentCount'),
                        Favorite_Count=detail['statistics']['favoriteCount'],
                        Definition=detail['contentDetails']['definition'],
                        Caption_status=detail['contentDetails']['caption'])
            video_data.append(data)
    return video_data


def get_comment_info(video_ids):

    # Establish Connection with YouTube Service
    youtube = api_connect()

    comment_data = []
    try:
        for vid in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=vid,
                maxResults=50
            )
            response = request.execute()

            for comment in response['items']:
                data = dict(Comment_Id=comment['snippet']['topLevelComment']['id'],
                            Video_Id=comment['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=comment['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass

    return comment_data


def get_playlist_details(channel_id):

    # Establish Connection with YouTube Service
    youtube = api_connect()

    playlist_data = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part='snippet, contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for playlist in response['items']:
            data = dict(Playlist_Id=playlist['id'],
                        Title=playlist['snippet']['title'],
                        Channel_Id=playlist['snippet']['channelId'],
                        Channel_Name=playlist['snippet']['channelTitle'],
                        PublishedAt=playlist['snippet']['publishedAt'],
                        Video_Count=playlist['contentDetails']['itemCount'])
            playlist_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data
