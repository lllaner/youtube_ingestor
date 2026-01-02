#!/usr/bin/env python3
"""
YouTube Vote Ingestor
Reads vote commands from YouTube Live Chat (gRPC streaming) or stdin and sends them to Rails API endpoint.
"""

import os
import re
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from typing import Optional

import grpc
import requests
import yaml
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Try to import generated proto files
try:
    import stream_list_pb2
    import stream_list_pb2_grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False
    logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# OAuth configuration
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
CLIENT_SECRET_FILE = 'client_secret.json'
TOKEN_YAML_FILE = 'token.yaml'  # Ruby format
TOKEN_JSON_FILE = 'token.json'  # Python format (will be created if needed)
LIVE_CHAT_ID_CACHE = 'live_chat_id.txt'

# gRPC endpoint
YOUTUBE_GRPC_ENDPOINT = 'youtube.googleapis.com:443'


def parse_vote(text: str) -> Optional[str]:
    """
    Parse vote command from text.
    
    Supports only:
    - "!<digit>" (e.g. "!1", "!2", "!3")
    
    Args:
        text: Input text to parse
        
    Returns:
        Option key (digit as string) if valid vote command found, None otherwise
    """
    if not text:
        return None
    
    # Normalize: strip whitespace
    text = text.strip()
    
    # Pattern: "!<digit>" (e.g. "!1", "!2")
    match = re.match(r'^!(\d+)$', text)
    if match:
        return match.group(1)
    
    return None


def send_vote(author_channel_id: str, option_key: str, 
              ingest_url: str, ingest_key: str, 
              max_retries: int = 5) -> bool:
    """
    Send vote to Rails API endpoint.
    
    Args:
        author_channel_id: YouTube channel ID of the voter
        option_key: Option key (digit as string)
        ingest_url: Rails API endpoint URL
        ingest_key: Authentication key for X-INGEST-KEY header
        max_retries: Maximum number of retry attempts
        
    Returns:
        True if vote was sent successfully, False otherwise
    """
    payload = {
        "author_channel_id": author_channel_id,
        "option_key": option_key
    }
    
    headers = {
        "X-INGEST-KEY": ingest_key,
        "Content-Type": "application/json"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                ingest_url,
                json=payload,
                headers=headers,
                timeout=2
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(
                    f"Vote sent successfully: user={author_channel_id}, "
                    f"option={option_key}, result={result.get('result', 'unknown')}"
                )
                return True
            elif response.status_code == 401:
                logger.error("Authentication failed: invalid INGEST_KEY")
                return False
            elif response.status_code == 422:
                logger.error("Invalid request: missing or invalid parameters")
                return False
            else:
                logger.warning(
                    f"Unexpected status code {response.status_code}: {response.text}"
                )
                # Retry on server errors (5xx)
                if response.status_code >= 500:
                    raise requests.exceptions.RequestException(
                        f"Server error: {response.status_code}"
                    )
                return False
                
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Connection error (attempt {attempt + 1}/{max_retries}): {e}"
            )
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Request error (attempt {attempt + 1}/{max_retries}): {e}"
            )
        
        # Exponential backoff: 1s, 2s, 4s, 8s, 10s (capped at 10s)
        if attempt < max_retries - 1:
            wait_time = min(2 ** attempt, 10)
            logger.info(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to send vote after {max_retries} attempts")
    return False


def load_token_from_yaml(yaml_path: Path) -> Optional[dict]:
    """
    Load token from Ruby YAML format and convert to Python dict.
    
    Args:
        yaml_path: Path to token.yaml file
        
    Returns:
        Token dict or None if not found/invalid
    """
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
            if yaml_data and 'default' in yaml_data:
                # Extract JSON string from YAML
                json_str = yaml_data['default']
                if isinstance(json_str, str):
                    token_data = json.loads(json_str)
                    # Convert expiration_time_millis to token_uri format if needed
                    if 'expiration_time_millis' in token_data:
                        # Convert milliseconds to seconds timestamp
                        expiration_seconds = token_data['expiration_time_millis'] / 1000.0
                        token_data['expiry'] = expiration_seconds
                        # Remove the old field
                        del token_data['expiration_time_millis']
                    return token_data
                return json_str
    except Exception as e:
        logger.warning(f"Could not load token from YAML: {e}")
    return None


def get_credentials() -> Credentials:
    """
    Get valid user credentials from storage or prompt for authorization.
    Tries to use existing token.yaml (Ruby format) or token.json (Python format).
    
    Returns:
        Credentials object for YouTube API access
    """
    creds = None
    token_yaml_path = Path(TOKEN_YAML_FILE)
    token_json_path = Path(TOKEN_JSON_FILE)
    client_secret_path = Path(CLIENT_SECRET_FILE)
    
    # Try to load from Python JSON format first
    if token_json_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_json_path), SCOPES)
            logger.debug("Loaded credentials from token.json")
        except Exception as e:
            logger.debug(f"Error loading token.json: {e}")
    
    # If not found, try to load from Ruby YAML format
    if not creds and token_yaml_path.exists():
        try:
            token_data = load_token_from_yaml(token_yaml_path)
            if token_data:
                creds = Credentials.from_authorized_user_info(token_data)
                logger.info("Loaded credentials from existing token.yaml (Ruby format)")
                # Save in Python format for next time
                try:
                    token_json_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(token_json_path, 'w') as f:
                        f.write(creds.to_json())
                    logger.debug("Converted token.yaml to token.json for future use")
                except Exception as e:
                    logger.warning(f"Could not save converted token: {e}")
        except Exception as e:
            logger.warning(f"Error loading token from YAML: {e}")
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired credentials...")
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Error refreshing credentials: {e}")
                creds = None
        
        if not creds:
            if not client_secret_path.exists():
                logger.error(
                    f"Client secret file not found: {client_secret_path}\n"
                    f"Please ensure client_secret.json exists in youtube_ingestor folder."
                )
                sys.exit(1)
            
            logger.info("Requesting new authorization...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path), SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run (in Python JSON format)
        try:
            token_json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(token_json_path, 'w') as token:
                token.write(creds.to_json())
            logger.info(f"Credentials saved to {token_json_path}")
        except Exception as e:
            logger.warning(f"Could not save credentials: {e}")
    
    return creds


def get_live_chat_id(credentials: Credentials, force_refresh: bool = False) -> Optional[str]:
    """
    Get live chat ID from active YouTube broadcast.
    
    Args:
        credentials: OAuth credentials
        force_refresh: If True, ignore cache and fetch fresh
        
    Returns:
        Live chat ID string or None if not found
    """
    # Check cache first
    if not force_refresh:
        cache_path = Path(LIVE_CHAT_ID_CACHE)
        if cache_path.exists():
            try:
                cached_id = cache_path.read_text().strip()
                # Clean the cached ID (remove all whitespace, newlines, etc.)
                cached_id = re.sub(r'\s+', '', cached_id)
                if cached_id:
                    logger.info(f"Using cached live chat ID: {cached_id[:30]}... (length: {len(cached_id)})")
                    return cached_id
            except Exception as e:
                logger.warning(f"Error reading cache: {e}")
    
    try:
        youtube = build('youtube', 'v3', credentials=credentials)
        
        # Get live broadcasts
        request = youtube.liveBroadcasts().list(
            part='snippet,status',
            mine=True,
            maxResults=50
        )
        response = request.execute()
        
        if not response.get('items'):
            logger.warning("No broadcasts found")
            return None
        
        # Find active broadcast
        for item in response.get('items', []):
            status = item.get('status', {})
            if status.get('lifeCycleStatus') == 'live':
                snippet = item.get('snippet', {})
                live_chat_id = snippet.get('liveChatId')
                if live_chat_id:
                    # Clean the ID (remove all whitespace, newlines, etc.)
                    live_chat_id = re.sub(r'\s+', '', live_chat_id)
                    logger.info(f"Found live chat ID: {live_chat_id[:30]}... (length: {len(live_chat_id)})")
                    # Cache it
                    try:
                        Path(LIVE_CHAT_ID_CACHE).write_text(live_chat_id)
                    except Exception as e:
                        logger.warning(f"Could not cache live chat ID: {e}")
                    return live_chat_id
        
        logger.warning("No active live broadcast found")
        return None
        
    except HttpError as e:
        logger.error(f"Error fetching live chat ID: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return None


def stream_youtube_chat(ingest_url: str, ingest_key: str):
    """
    Stream YouTube Live Chat messages via gRPC and process votes.
    
    Args:
        ingest_url: Rails API endpoint URL
        ingest_key: Authentication key for X-INGEST-KEY header
    """
    if not GRPC_AVAILABLE:
        logger.error(
            "gRPC proto files not found. Please run:\n"
            "  python scripts/gen_proto.bat\n"
            "or:\n"
            "  python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/stream_list.proto"
        )
        sys.exit(1)
    
    # Get credentials
    credentials = get_credentials()
    if not credentials:
        logger.error("Failed to obtain credentials")
        sys.exit(1)
    
    # Get live chat ID
    live_chat_id = get_live_chat_id(credentials)
    if not live_chat_id:
        logger.error("Could not obtain live chat ID. Is there an active stream?")
        sys.exit(1)
    
    # Clean live_chat_id (remove ALL whitespace/newlines - YouTube IDs should be continuous)
    # More aggressive cleaning: remove all whitespace characters
    live_chat_id = re.sub(r'\s+', '', live_chat_id)
    logger.info(f"Using live chat ID: {live_chat_id[:50]}{'...' if len(live_chat_id) > 50 else ''} (length: {len(live_chat_id)})")
    
    # Track processed message IDs to avoid duplicates
    processed_message_ids = set()
    
    # Verify live_chat_id is valid by trying REST API first
    try:
        youtube = build('youtube', 'v3', credentials=credentials)
        test_request = youtube.liveChatMessages().list(
            liveChatId=live_chat_id,
            part='id,snippet,authorDetails',
            maxResults=1
        )
        test_response = test_request.execute()
        logger.info(f"✓ Verified live_chat_id is valid via REST API")
    except HttpError as e:
        logger.error(f"✗ Live chat ID validation failed via REST API: {e}")
        logger.error("This suggests the live_chat_id may be invalid or the stream is not active")
        sys.exit(1)
    except Exception as e:
        logger.warning(f"Could not verify live_chat_id via REST API: {e}")
        logger.info("Proceeding with gRPC anyway...")
    
    retry_count = 0
    max_retry_wait = 30
    
    while True:
        try:
            # Create secure channel
            channel = grpc.secure_channel(
                YOUTUBE_GRPC_ENDPOINT,
                grpc.ssl_channel_credentials()
            )
            
            # Create stub
            stub = stream_list_pb2_grpc.V3DataLiveChatMessageServiceStub(channel)
            
            # Prepare request
            # According to proto, part is repeated string, so list is correct
            # YouTube API requires 'id' to be included in part
            # Ensure live_chat_id has no whitespace (already cleaned, but double-check)
            clean_live_chat_id = re.sub(r'\s+', '', live_chat_id)
            
            # Log the actual values being sent
            logger.info(f"Preparing gRPC request:")
            logger.info(f"  live_chat_id: {repr(clean_live_chat_id)} (length: {len(clean_live_chat_id)})")
            logger.info(f"  part: ['id', 'snippet', 'authorDetails']")
            logger.info(f"  endpoint: {YOUTUBE_GRPC_ENDPOINT}")
            
            # Create request object and set fields explicitly
            request = stream_list_pb2.LiveChatMessageListRequest()
            request.live_chat_id = clean_live_chat_id
            request.part.extend(['id', 'snippet', 'authorDetails'])
            
            # Verify what was actually set in the request
            logger.info(f"Request object created:")
            logger.info(f"  request.live_chat_id = {repr(request.live_chat_id)} (length: {len(request.live_chat_id)})")
            logger.info(f"  request.part = {list(request.part)}")
            logger.info(f"  live_chat_id is set: {bool(request.live_chat_id)}")
            logger.info(f"  part count: {len(request.part)}")
            
            # Get access token
            if not credentials.valid:
                credentials.refresh(Request())
            
            # Create metadata with authorization
            metadata = [
                ('authorization', f'Bearer {credentials.token}')
            ]
            
            logger.info(f"Connecting to YouTube Live Chat (ID: {clean_live_chat_id[:50]}{'...' if len(clean_live_chat_id) > 50 else ''} (length: {len(clean_live_chat_id)}))...")
            
            # Stream messages
            try:
                for response in stub.StreamList(request, metadata=metadata):
                    # Reset retry count on successful response
                    retry_count = 0
                    
                    # Process messages
                    for message in response.items:
                        try:
                            # Skip if message already processed
                            message_id = message.id
                            if not message_id:
                                continue
                            
                            if message_id in processed_message_ids:
                                continue  # Skip duplicate message
                            
                            # Mark as processed
                            processed_message_ids.add(message_id)
                            
                            # Extract author channel ID
                            author_details = message.author_details
                            if not author_details or not author_details.channel_id:
                                continue
                            
                            author_channel_id = author_details.channel_id
                            
                            # Extract message text
                            snippet = message.snippet
                            if not snippet or not snippet.display_message:
                                continue
                            
                            message_text = snippet.display_message.strip().lower()
                            
                            # Parse vote
                            option_key = parse_vote(message_text)
                            if not option_key:
                                continue
                            
                            # Send vote
                            send_vote(author_channel_id, option_key, ingest_url, ingest_key)
                            
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            continue
                    
                    # Update page token if provided
                    if response.next_page_token:
                        request.page_token = response.next_page_token
                        
            except grpc.RpcError as e:
                error_code = e.code()
                error_details = e.details()
                
                if error_code == grpc.StatusCode.UNAUTHENTICATED:
                    logger.warning("Authentication failed, refreshing token...")
                    credentials.refresh(Request())
                    retry_count = 0
                    continue
                elif error_code == grpc.StatusCode.NOT_FOUND:
                    logger.warning("Live chat not found, refreshing live chat ID...")
                    live_chat_id = get_live_chat_id(credentials, force_refresh=True)
                    if not live_chat_id:
                        logger.error("Could not refresh live chat ID")
                        time.sleep(5)
                        continue
                    live_chat_id = re.sub(r'\s+', '', live_chat_id)
                    request.live_chat_id = live_chat_id
                    retry_count = 0
                    continue
                elif error_code == grpc.StatusCode.INVALID_ARGUMENT:
                    logger.error(f"gRPC INVALID_ARGUMENT error: {error_details}")
                    logger.error("This may indicate:")
                    logger.error("  1. The proto file doesn't match YouTube's actual API")
                    logger.error("  2. The request format is incorrect")
                    logger.error("  3. The live_chat_id format is wrong")
                    logger.error("Trying alternative request format...")
                    # Try with minimal request
                    request = stream_list_pb2.LiveChatMessageListRequest()
                    request.live_chat_id = clean_live_chat_id
                    request.part.append('id')
                    request.part.append('snippet')
                    request.part.append('authorDetails')
                    logger.info("Retrying with alternative format...")
                    retry_count += 1
                    if retry_count >= 3:
                        logger.error("Failed after multiple attempts. The proto file may not match YouTube's API.")
                        logger.error("Consider using REST API polling instead of gRPC streaming.")
                        raise
                    continue
                else:
                    logger.error(f"gRPC error: {error_code} - {error_details}")
                    raise
                    
        except KeyboardInterrupt:
            logger.info("\nReceived Ctrl+C, exiting gracefully...")
            break
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, max_retry_wait)
            logger.error(
                f"Streaming error (attempt {retry_count}): {e}\n"
                f"Retrying in {wait_time} seconds..."
            )
            time.sleep(wait_time)


def run_stdin_mode(ingest_url: str, ingest_key: str):
    """
    Run in stdin mode for debugging.
    
    Args:
        ingest_url: Rails API endpoint URL
        ingest_key: Authentication key for X-INGEST-KEY header
    """
    logger.info("Running in stdin mode (debugging)")
    logger.info("Input format: <author_channel_id> <message text...>")
    logger.info("Example: user123 !1")
    logger.info("Example: user456 !2")
    logger.info("Press Ctrl+C to exit")
    logger.info("")
    
    last_post_time = 0
    min_post_interval_ms = int(os.getenv("MIN_POST_INTERVAL_MS", "0"))
    
    try:
        while True:
            try:
                # Read line from stdin
                line = input().strip()
                
                if not line:
                    continue
                
                # Parse input: "<author_channel_id> <message text...>"
                parts = line.split(None, 1)
                if len(parts) < 2:
                    logger.warning(f"Invalid input format: {line}")
                    continue
                
                author_channel_id = parts[0]
                message_text = parts[1]
                
                # Parse vote command
                option_key = parse_vote(message_text)
                if not option_key:
                    logger.debug(f"No vote command found in: {message_text}")
                    continue
                
                # Throttle if configured
                if min_post_interval_ms > 0:
                    current_time = time.time() * 1000  # Convert to milliseconds
                    time_since_last = current_time - last_post_time
                    if time_since_last < min_post_interval_ms:
                        wait_time = (min_post_interval_ms - time_since_last) / 1000
                        logger.debug(f"Throttling: waiting {wait_time:.2f}s")
                        time.sleep(wait_time)
                
                # Send vote
                send_vote(author_channel_id, option_key, ingest_url, ingest_key)
                last_post_time = time.time() * 1000
                
            except EOFError:
                # End of input (e.g., when stdin is closed)
                logger.info("End of input, exiting...")
                break
            except KeyboardInterrupt:
                logger.info("\nReceived Ctrl+C, exiting gracefully...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                
    except KeyboardInterrupt:
        logger.info("\nExiting...")
    finally:
        logger.info("Vote ingestor stopped")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='YouTube Vote Ingestor - Stream votes from YouTube Live Chat to Rails API'
    )
    parser.add_argument(
        '--stdin',
        action='store_true',
        help='Run in stdin mode for debugging (default: YouTube streaming mode)'
    )
    args = parser.parse_args()
    
    # Read environment variables
    ingest_url = os.getenv(
        "RAILS_INGEST_URL",
        "http://127.0.0.1:3000/api/votes/ingest"
    )
    ingest_key = os.getenv("INGEST_KEY")
    
    # Validate required configuration
    if not ingest_key:
        logger.error("INGEST_KEY environment variable is required")
        logger.error("Please set it in .env file or environment variables")
        sys.exit(1)
    
    logger.info(f"Starting vote ingestor...")
    logger.info(f"Rails endpoint: {ingest_url}")
    logger.info(f"Mode: {'stdin (debugging)' if args.stdin else 'YouTube streaming'}")
    logger.info("")
    
    if args.stdin:
        run_stdin_mode(ingest_url, ingest_key)
    else:
        stream_youtube_chat(ingest_url, ingest_key)


if __name__ == "__main__":
    main()
