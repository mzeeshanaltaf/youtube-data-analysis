from database import *
from questions import questions
from streamlit_option_menu import option_menu

# --- PAGE SETUP ---
# Initialize streamlit app
page_title = "YouData Insight"
page_icon = "▶️"
st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")

st.title(page_title)
st.write("***:blue[Unlock the power of YouTube Data]***")
st.write("YouData Insight is a powerful application designed to extract and analyze comprehensive data from YouTube "
         "channels. By simply entering a YouTube channel ID, users can effortlessly retrieve detailed channel "
         "information and store it in a robust database. ")
st.write('''This application has following features.

- Data Collection from YouTube Channel and storing it in MongoDB.
- Detail Insights of YouTube Channels Playlists, Videos and Comments.
- Data Migration to SQL Database.
- Data Analysis.
            ''')
# ---- NAVIGATION MENU -----
selection = option_menu(
    menu_title=None,
    options=["Data Collection", "Channel Details", "Migrate Data", "Data Analysis"],
    icons=["", "", "", ""],  # https://icons.getbootstrap.com
    orientation="horizontal",
)

if selection == "Data Collection":
    st.subheader('Channel ID', help="See below instructions on how to get the channel ID")
    st.write('''Enter Youtube Channel Unique ID. Channel Data will be stored in MongoDB.
                ''')
    channel_id = st.text_input("Enter the Channel ID", placeholder='Enter Channel ID', label_visibility="collapsed")
    store_button = st.button("Store Data", type="primary", disabled=not channel_id)
    if store_button:
        with st.spinner('Processing...'):
            ch_ids = []
            client = mongodb_connection()
            db = client["Youtube_Data"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
                ch_ids.append(ch_data['channel_information']['Channel_Id'])

            if channel_id in ch_ids:
                st.success("Channel details already exists")
            else:
                insert = channel_details(channel_id)
                st.success(insert)

    st.subheader('FAQs')
    with st.expander('How to get the YouTube Channel ID'):
        st.write('''Here is how you can get the YouTube channel ID.
- Go to the YouTube Channel.
- Click on the channel description. It will open the "About" dialog.
- Click on "Share Channel" button.
- Clicking the "Copy Channel ID" will copy the channel ID to clipboard.
                    ''')
if selection == "Channel Details":
    st.subheader('Select Table')
    show_table = st.radio("Select the table", ("Channels", "Playlists", "Videos", "Comments"), label_visibility="collapsed")
    if show_table == "Channels":
        st.subheader(f"{show_table} Table")
        with st.spinner('Processing...'):
            records = show_channels_table()
            st.info(f"{records} Record(s) Found")
    elif show_table == "Playlists":
        st.subheader(f"{show_table} Table")
        with st.spinner('Processing...'):
            records = show_playlists_table()
            st.info(f"{records} Record(s) Found")
    elif show_table == "Videos":
        st.subheader(f"{show_table} Table")
        with st.spinner('Processing...'):
            records = show_videos_table()
            st.info(f"{records} Record(s) Found")
    elif show_table == "Comments":
        st.subheader(f"{show_table} Table")
        with st.spinner('Processing...'):
            records = show_comments_table()
            st.info(f"{records} Record(s) Found")

if selection == "Migrate Data":
    st.subheader("Select the Channel")
    st.write("*Select the channel whose data needs to be migrated to SQL Database.*")
    ch_names = get_channel_names()
    channel_name = st.selectbox("Select the Channel", ch_names, label_visibility="collapsed")
    migration_button = st.button("Start Migration", type='primary', disabled=not channel_name)
    if migration_button:
        with st.spinner('Processing ...'):
            status = tables(channel_name)
            st.success(status)

if selection == "Data Analysis":
    st.subheader('Select the Question')
    st.write("*Select the question from the below list. "
             "Data Analysis will be performed on Channels data stored in SQL database.*")
    mydb, cursor = sql_db_connection()
    question_list = [item["question"] for item in questions]
    question_input = st.selectbox("Select your question", question_list, label_visibility="collapsed")

    if question_input:
        # Extract sql query from the questions list for asked question
        query = next((item["sql"] for item in questions if item["question"] == question_input), None)

        # Extract columns for pandas dataframe from the questions list for asked question
        df_cols = next((item["df_cols"] for item in questions if item["question"] == question_input), None)

        cursor.execute(query)
        mydb.commit()
        ans = cursor.fetchall()
        df = pd.DataFrame(ans, columns=df_cols)
        st.dataframe(df)


