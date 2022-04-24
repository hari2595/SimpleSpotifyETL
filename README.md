# SimpleSpotifyETL
A simple ETL project with Spotify API.
It uses SQLITE db for storing data.
First the script send request to Spotify API
Next it extracts songs played after a certain timestamp mentioned by the user.
This is cross checked for null values and repetitions in the script.
Finally the extracted data is srored in dictionaries which are then writtern into .sqlite file
