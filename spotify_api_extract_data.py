import json
import os
import spotipy
import boto3
from datetime import datetime as dt
from spotipy.oauth2 import SpotifyClientCredentials


def lambda_handler(event, context):
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager = client_manager)
    uri="https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_uri=uri.split("/")[-1].split("?")[0]
    data=sp.playlist_tracks(playlist_uri)
    
    cli=boto3.client('s3')
    file_name="spotify_raw_data"+f"_{dt.now()}"+".json"
    
    cli.put_object(
        Bucket="spotify-etl-project-asish",
        Key="raw_data/to_processed/"+file_name,
        Body=json.dumps(data)
        )
