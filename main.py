import pandas as pd
import sqlite3
import sqlalchemy
import requests

# Setting up constants
DB_LOCATION = "sqlite:///my_played_tracks.sqlite"
UID = "xxx"
TOKEN = "xxxx"
# token url "https://developer.spotify.com/console/get-recently-played/"

#Transformation
def ValidityCheck(df: pd.DataFrame) -> bool:

# Taking care of empty dataset
    if df.empty:
        print("EMPTY Dataset. Proceeding with execution...")
        return False

# Taking care of duplication. This is done by cross checking the played at time stamp
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Duplication occured")

# Null value elimination
    if df.isnull().values.any():
        raise Exception("Null values present")

    return True

if __name__ == "__main__":

# setting up headers according to spotify api format
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

# Time stamp of specific time after which we are getting the data. Time stamp shoud be in unix.
    TimeStamp = 1650608012

# Sending requests
    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=TimeStamp),
                       headers=headers)
# Assigning the received data
    data = req.json()

# Creating arrays
    song_name_ = []
    artist_name_ = []
    played_at_list = []
    timestamp_ = []

# Creating dictionaries
    for song in data["items"]:
        song_name_.append(song["track"]["name"])
        artist_name_.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamp_.append(song["played_at"][0:10])

    SpotifyDict = {
        "song_name": song_name_,
        "artist_name": artist_name_,
        "played_at": played_at_list,
        "timestamp": timestamp_
    }

# Assigning values to Pandas df
    SpotifyDf = pd.DataFrame(SpotifyDict, columns=["song_name", "artist_name", "played_at", "timestamp"])

# Transformation called
    if ValidityCheck(SpotifyDf):
        print("Data received. Loading activated...")

# Load

    engine = sqlalchemy.create_engine(DB_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")
    print(data)

    try:
        SpotifyDf.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
