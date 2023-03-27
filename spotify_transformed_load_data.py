import json
import boto3
import pandas as pd
from datetime import datetime as dt
from io import StringIO

def album(data):
    album_data=[]

    for i in data['items']:
        temp_dict={
                'name':i['track']['album']['name'],
                'id':i['track']['album']['id'],
                'release_date':i['track']['album']['release_date'],
                'total_tracks':i['track']['album']['total_tracks'],
                'external_urls':i['track']['album']['external_urls']['spotify']
            }
        album_data.append(temp_dict)
        
    return album_data
    
def artist(data):
    artists_data=[]

    for i in data['items']:
        for key,val in i.items():
            if key == 'track':
                for j in val['artists']:
                    art_dict={
                        'Artist_name':j['name'],
                        'id':j['id'],
                        'external_urls':j['external_urls']['spotify']
                    }
                    artists_data.append(art_dict)
    return artists_data
    

def songs(data):
    songs_data=[]

    for i in data['items']:
        song_dict={
            'name':i['track']['name'],
            'id':i['track']['id'],
            'popularity':i['track']['popularity'],
            'url':i['track']['external_urls']['spotify'],
            'duration (Min)':round((i['track']['duration_ms']/60000),2),
            'artist_id':i['track']['album']['artists'][0]['id'],
            'album_id':i['track']['album']['id']
        }
    
        songs_data.append(song_dict)
    return songs_data

def lambda_handler(event, context):
    
    s3=boto3.client('s3')
    Bucket="spotify-etl-project-asish"
    Key="raw_data/to_processed/"
    
    spotify_data=[]
    keys=[]
    
    for i in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        if i['Key'].split('.')[-1]=='json':
            response=s3.get_object(Bucket=Bucket,Key=i['Key'])
            content=response['Body']
            JsonObject=json.loads(content.read())
            spotify_data.append(JsonObject)
            keys.append(i['Key'])
    
    for data in spotify_data:
        album_list=album(data)
        artist_list=artist(data)
        songs_list=songs(data)
        
        #album data tranformation
        
        album_df=pd.DataFrame(album_list)
        album_df['name']=album_df['name'].astype('string')
        album_df['id']=album_df['id'].astype('string')
        album_df['external_urls']=album_df['external_urls'].astype('string')
        album_df['release_date']=pd.to_datetime(album_df['release_date'])
        album_df=album_df.drop_duplicates(subset="id")
        
        
        #artits data tranformation
        
        artist_df=pd.DataFrame(artist_list)
        artist_df['Artist_name']=artist_df['Artist_name'].astype('string')
        artist_df['id']=artist_df['id'].astype('string')
        artist_df['external_urls']=artist_df['external_urls'].astype('string')
        artist_df=artist_df.drop_duplicates(subset="Artist_name")
        
        
        #songs data transformation
        
        songs_df=pd.DataFrame(songs_list)
        songs_df['id']=songs_df['id'].astype('string')
        songs_df['name']=songs_df['name'].astype('string')
        songs_df['url']=songs_df['url'].astype('string')
        songs_df['artist_id']=songs_df['artist_id'].astype('string')
        songs_df['album_id']=songs_df['album_id'].astype('string')
        
        
        #Data load into target locations 
        
        #album data load into target location
        album_key="album"+f"_{dt.now()}"+".csv"
        
        album_buffer=StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content=album_buffer.getvalue()
        
        s3.put_object(
            Bucket="spotify-etl-project-asish",
            Key="transformed_data/album_data/"+album_key,
            Body=album_content
            )
            
            
            
          
        #artist data load into target location
            
        artist_key="artist"+f"_{dt.now()}"+".csv"
        
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content=artist_buffer.getvalue()
        
        s3.put_object(
            Bucket="spotify-etl-project-asish",
            Key="transformed_data/artists_data/"+artist_key,
            Body=artist_content
            )
            
        #songs data load into traget loaction    
            
        song_key="songs"+f"_{dt.now()}"+".csv"
        
        song_buffer=StringIO()
        songs_df.to_csv(song_buffer,index=False)
        songs_content=song_buffer.getvalue()
        
        s3.put_object(
            Bucket="spotify-etl-project-asish",
            Key="transformed_data/songs_data/"+song_key,
            Body=songs_content
            )
            
    s3_resource=boto3.resource('s3')
    
    for key in keys:
        copy_resource={
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_resource,Bucket,"raw_data/processed/"+key.split('/')[-1])
        s3_resource.Object(Bucket,key).delete()
