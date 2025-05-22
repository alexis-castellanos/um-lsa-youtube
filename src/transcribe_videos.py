# api_call.py
import os
import pandas as pd
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
from dotenv import load_dotenv
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi
from langdetect import detect as detect_language

# Load environment variables
load_dotenv()

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"youtube_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Configuration
CONFIG = {
    'query': 'sourdough bread baking',
    'published_after': '2020-03-15T00:00:00Z',
    'published_before': '2021-04-01T00:00:00Z',
    'max_results_per_page': 50,
    'csv_file_path': 'data/scrapped_data.csv',  # Changed to match your file path
    'batch_size': 50,  # For video details (max 50 per request)
    'update_existing': False,  # Set to True to update stats for existing videos
    'fetch_transcripts': True,  # Set to True to fetch transcripts for new videos
    'update_transcripts': False  # Set to True to update missing transcripts for existing videos
}

def get_api_key():
    """Get API key from environment variables"""
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        logging.error("API key not found. Please set YOUTUBE_API_KEY in your .env file.")
        raise ValueError("Missing API key")
    return api_key

def initialize_youtube_client():
    """Initialize the YouTube API client"""
    try:
        return build('youtube', 'v3', developerKey=get_api_key())
    except Exception as e:
        logging.error(f"Failed to initialize YouTube client: {e}")
        raise

def load_existing_data(csv_file):
    """Load existing data from CSV or create a new DataFrame"""
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    if os.path.exists(csv_file):
        return pd.read_csv(csv_file)
    else:
        return pd.DataFrame(columns=[
            'video_id', 'title', 'description', 'published_at', 
            'view_count', 'like_count', 'comment_count', 
            'video_url', 'region', 'transcription', 'detected_language'
        ])

def save_data(df, csv_file):
    """Save DataFrame to CSV with error handling"""
    try:
        df.to_csv(csv_file, index=False)
        logging.info(f"Data saved to {csv_file}")
    except Exception as e:
        logging.error(f"Failed to save data: {e}")
        # Save to a backup file
        backup_file = f"{csv_file}.backup.{int(time.time())}.csv"
        df.to_csv(backup_file, index=False)
        logging.info(f"Data saved to backup file: {backup_file}")

def fetch_video_details(youtube, video_ids):
    """Fetch detailed stats for a batch of videos"""
    if not video_ids:
        return []
    
    results = []
    # Process in batches of 50 (API limit)
    for i in range(0, len(video_ids), CONFIG['batch_size']):
        batch = video_ids[i:i + CONFIG['batch_size']]
        try:
            request = youtube.videos().list(
                part='snippet,statistics',
                id=','.join(batch)
            )
            response = request.execute()
            results.extend(response.get('items', []))
            # Avoid rate limiting
            if i + CONFIG['batch_size'] < len(video_ids):
                time.sleep(1)
        except HttpError as e:
            logging.error(f"Error fetching video details: {e}")
            if e.resp.status == 403:
                logging.error("API quota exceeded.")
                break
    return results

def search_youtube_videos(youtube, existing_video_ids):
    """Search for videos with pagination"""
    all_videos = []
    next_page_token = None
    page_count = 0
    
    while True:
        try:
            request = youtube.search().list(
                q=CONFIG['query'],
                part='snippet',
                type='video',
                publishedAfter=CONFIG['published_after'],
                publishedBefore=CONFIG['published_before'],
                maxResults=CONFIG['max_results_per_page'],
                pageToken=next_page_token
            )
            response = request.execute()
            videos = response.get('items', [])
            
            # Filter out videos that are already in our dataset
            new_videos = [v for v in videos if v['id']['videoId'] not in existing_video_ids]
            all_videos.extend(new_videos)
            
            page_count += 1
            logging.info(f"Processed page {page_count}, found {len(new_videos)} new videos (total new: {len(all_videos)})")
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                logging.info("No more pages available.")
                break
                
            # Slight delay to avoid rate limiting
            time.sleep(1)
            
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("API quota exceeded.")
                break
            else:
                logging.error(f"HTTP error: {e}")
                time.sleep(10)  # Wait before retrying
                
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            time.sleep(10)  # Wait before retrying
    
    return all_videos

def get_transcript_and_language(video_id):
    """Fetch transcript and detect language for a video"""
    if not video_id:
        return "Video Error", "unknown"
        
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
        full_transcript = ' '.join([item['text'] for item in transcript_list])
        
        # Detect language from transcript
        try:
            language = detect_language(full_transcript)
        except:
            language = "unknown"
            
        return full_transcript, language
    
    except Exception as e:
        logging.error(f"Error getting transcript for video {video_id}: {str(e)}")
        return "Video Error", "unknown"

def process_videos(youtube, search_results, include_transcripts=True):
    """Process search results and fetch additional details"""
    if not search_results:
        return []
    
    # Extract video IDs
    video_ids = [video['id']['videoId'] for video in search_results]
    
    # Fetch detailed statistics for these videos
    detailed_videos = fetch_video_details(youtube, video_ids)
    
    # Create a lookup dictionary for easy access
    video_details = {v['id']: v for v in detailed_videos}
    
    # Create records for each video
    records = []
    for i, video in enumerate(search_results):
        video_id = video['id']['videoId']
        details = video_details.get(video_id, {})
        statistics = details.get('statistics', {})
        
        # Basic video info
        record = {
            'video_id': video_id,
            'title': video['snippet']['title'],
            'description': video['snippet']['description'],
            'published_at': video['snippet']['publishedAt'],
            'view_count': statistics.get('viewCount'),
            'like_count': statistics.get('likeCount'),
            'comment_count': statistics.get('commentCount'),
            'video_url': f"https://www.youtube.com/watch?v={video_id}",
            'region': video['snippet'].get('regionCode', ''),
            'transcription': '',
            'detected_language': ''
        }
        
        # Add transcript and language if requested
        if include_transcripts:
            logging.info(f"Processing transcript for video {i+1}/{len(search_results)}: {video_id}")
            transcript, language = get_transcript_and_language(video_id)
            record['transcription'] = transcript
            record['detected_language'] = language
            
            # Add a small delay to avoid rate limiting on transcript API
            time.sleep(1)
        
        records.append(record)
    
    return records

def update_existing_stats(youtube, existing_data):
    """Update statistics for existing videos"""
    if existing_data.empty:
        return existing_data
    
    # Get list of video IDs to update
    video_ids = existing_data['video_id'].tolist()
    
    logging.info(f"Updating statistics for {len(video_ids)} existing videos...")
    
    # Fetch updated details
    detailed_videos = fetch_video_details(youtube, video_ids)
    video_details = {v['id']: v for v in detailed_videos}
    
    # Update statistics in the DataFrame
    updated_count = 0
    for index, row in existing_data.iterrows():
        video_id = row['video_id']
        if video_id in video_details:
            statistics = video_details[video_id].get('statistics', {})
            existing_data.at[index, 'view_count'] = statistics.get('viewCount')
            existing_data.at[index, 'like_count'] = statistics.get('likeCount')
            existing_data.at[index, 'comment_count'] = statistics.get('commentCount')
            updated_count += 1
    
    logging.info(f"Updated statistics for {updated_count} videos")
    return existing_data

def update_missing_transcripts(existing_data):
    """Update transcripts for videos that don't have them yet"""
    if existing_data.empty:
        return existing_data
    
    # Find rows with missing or error transcripts
    missing_transcripts = existing_data[
        (existing_data['transcription'].isna()) | 
        (existing_data['transcription'] == '') | 
        (existing_data['transcription'] == 'Video Error')
    ]
    
    if missing_transcripts.empty:
        logging.info("No missing transcripts to update")
        return existing_data
    
    logging.info(f"Updating transcripts for {len(missing_transcripts)} videos")
    
    # Process each video
    updated_count = 0
    for index, row in missing_transcripts.iterrows():
        video_id = row['video_id']
        logging.info(f"Processing transcript for video {updated_count+1}/{len(missing_transcripts)}: {video_id}")
        
        transcript, language = get_transcript_and_language(video_id)
        existing_data.at[index, 'transcription'] = transcript
        existing_data.at[index, 'detected_language'] = language
        
        updated_count += 1
        
        # Save intermediate results every 10 videos
        if updated_count % 10 == 0:
            save_data(existing_data, CONFIG['csv_file_path'])
            logging.info(f"Saved interim results after updating {updated_count} transcripts")
        
        # Add a small delay to avoid rate limiting
        time.sleep(1)
    
    logging.info(f"Updated transcripts for {updated_count} videos")
    return existing_data

def main():
    try:
        # Initialize YouTube client
        youtube = initialize_youtube_client()
        
        # Load existing data
        existing_data = load_existing_data(CONFIG['csv_file_path'])
        existing_video_ids = set(existing_data['video_id'])
        logging.info(f"Loaded {len(existing_data)} existing videos")
        
        # Optionally update statistics for existing videos
        if CONFIG['update_existing'] and not existing_data.empty:
            existing_data = update_existing_stats(youtube, existing_data)
            save_data(existing_data, CONFIG['csv_file_path'])
        
        # Optionally update missing transcripts
        if CONFIG['update_transcripts'] and not existing_data.empty:
            existing_data = update_missing_transcripts(existing_data)
            save_data(existing_data, CONFIG['csv_file_path'])
        
        # Search for new videos
        logging.info("Searching for new videos...")
        search_results = search_youtube_videos(youtube, existing_video_ids)
        
        if not search_results:
            logging.info("No new videos found.")
            return existing_data
            
        # Process videos and get additional details including transcripts if configured
        logging.info(f"Processing {len(search_results)} new videos...")
        video_records = process_videos(youtube, search_results, include_transcripts=CONFIG['fetch_transcripts'])
        
        # Add to existing data
        if video_records:
            new_data = pd.DataFrame(video_records)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            save_data(combined_data, CONFIG['csv_file_path'])
            logging.info(f"Added {len(new_data)} new videos. Total: {len(combined_data)}")
            return combined_data
        
        return existing_data
        
    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    main()