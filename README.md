# YouTube Vote Ingestor

Python service that streams vote commands from YouTube Live Chat (gRPC) and sends them to the Rails API endpoint.

## Features

- **YouTube Live Chat Streaming**: Real-time gRPC streaming from YouTube Live Chat
- **Vote Parsing**: Parses vote commands: `!1`, `!2`, `!3`, etc.
- **Rails Integration**: Sends votes to Rails API with authentication
- **OAuth Support**: Automatic OAuth token management
- **Message Deduplication**: Tracks processed message IDs to prevent duplicate processing on reconnection
- **Robust Error Handling**: Automatic reconnection with exponential backoff
- **Debug Mode**: Stdin mode for testing without live stream
- **Windows-friendly**: Works with PowerShell and Git Bash

## Prerequisites

- Python 3.7 or higher
- Active YouTube Live Stream
- OAuth credentials (`client_secret.json` in `dota_stream/` folder)
- Rails server running with active poll

## Setup

### 1. Create Virtual Environment

**PowerShell:**
```powershell
cd youtube_ingestor
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Git Bash:**
```bash
cd youtube_ingestor
python -m venv venv
source venv/Scripts/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `grpcio` and `grpcio-tools` for gRPC support
- `google-auth` and related packages for OAuth
- `google-api-python-client` for YouTube Data API
- `requests` for HTTP requests
- `python-dotenv` for environment variables

### 3. Generate gRPC Stubs

You need to generate Python stubs from the proto file before running:

**PowerShell:**
```powershell
.\scripts\gen_proto.bat
```

**Git Bash:**
```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. stream_list.proto
```

This will generate:
- `stream_list_pb2.py` - Message definitions
- `stream_list_pb2_grpc.py` - Service stubs

**Note:** You only need to run this once, or when the proto file changes.

### 4. Configure Environment Variables

Create a `.env` file in the `youtube_ingestor/` directory:

```env
# Required: Authentication key (must match Rails ENV["INGEST_KEY"])
INGEST_KEY=your-secret-key-here

# Optional: Rails API endpoint (default: http://127.0.0.1:3000/api/votes/ingest)
RAILS_INGEST_URL=http://127.0.0.1:3000/api/votes/ingest

# Optional: Minimum interval between posts in milliseconds (default: 0, no throttling)
# Only used in stdin mode
MIN_POST_INTERVAL_MS=0
```

**Important:** The `INGEST_KEY` must match the value set in Rails `ENV["INGEST_KEY"]`.

### 5. OAuth Setup

The ingestor uses OAuth credentials from the same folder:

- `client_secret.json` should be in `youtube_ingestor/` folder (copied from Rails project)
- If `token.yaml` exists (from Ruby scripts), it will be automatically converted and used
- Token will be saved to `token.json` for Python (Ruby continues using `token.yaml` in Rails folder)

**Using existing token:**
- Copy `token.yaml` from `dota_stream/` to `youtube_ingestor/` folder
- The ingestor will automatically use it and convert to `token.json`
- No need to re-authenticate if the token is still valid

**First-time OAuth flow (if no token exists):**
1. Run the ingestor
2. Browser will open for Google OAuth authorization
3. Authorize the application
4. Token will be saved automatically to `token.json`

### 6. Start Rails Server

Make sure your Rails server is running:

```bash
cd dota_stream
rails server
```

The server should be accessible at `http://127.0.0.1:3000`.

## Usage

### YouTube Streaming Mode (Default)

**PowerShell:**
```powershell
python main.py
```

**Git Bash:**
```bash
python main.py
```

The ingestor will:
1. Authenticate with YouTube using OAuth
2. Find the active live stream
3. Connect to YouTube Live Chat via gRPC
4. Stream messages in real-time
5. Track processed message IDs to avoid duplicates (especially on reconnection)
6. Parse vote commands (`!1`, `!2`, etc.)
7. Send votes to Rails API

### Stdin Mode (Debugging)

For testing without a live stream:

**PowerShell:**
```powershell
python main.py --stdin
```

**Git Bash:**
```bash
python main.py --stdin
```

Input format:
```
<author_channel_id> <message text...>
```

**Examples:**
```
user123 !1
user456 !2
user789 !3
```

### Supported Vote Commands

The ingestor recognizes only the following pattern:

- `!<digit>` - e.g., `!1`, `!2`, `!3`

Only exclamation mark followed by a digit is supported.

## Example Session (YouTube Streaming)

```bash
$ python main.py
2024-12-28 12:00:00 - INFO - Starting vote ingestor...
2024-12-28 12:00:00 - INFO - Rails endpoint: http://127.0.0.1:3000/api/votes/ingest
2024-12-28 12:00:00 - INFO - Mode: YouTube streaming
2024-12-28 12:00:00 - INFO - 
2024-12-28 12:00:01 - INFO - Found live chat ID: xxxxxxxxxxxxxx
2024-12-28 12:00:02 - INFO - Connecting to YouTube Live Chat (ID: xxxxxxxxxxxxxx)...
2024-12-28 12:00:05 - INFO - Vote sent successfully: user=UCxxxxxxxxxxxxx, option=1, result=accepted
2024-12-28 12:00:10 - INFO - Vote sent successfully: user=UCyyyyyyyyyyyyy, option=2, result=accepted
^C
2024-12-28 12:00:15 - INFO - Received Ctrl+C, exiting gracefully...
```

## Testing

### Manual Testing (Stdin Mode)

1. Start Rails server:
   ```bash
   cd dota_stream
   rails server
   ```

2. Create an active poll in Rails admin (if needed)

3. Run the ingestor in stdin mode:
   ```bash
   cd youtube_ingestor
   python main.py --stdin
   ```

4. Type test commands:
   ```
   test_user_1 !1
   test_user_2 !2
   test_user_3 !3
   ```

5. Check Rails logs to verify votes are being received

### Testing with File Input

You can also pipe input from a file:

```bash
# Create test file
echo "user1 !1" > test_input.txt
echo "user2 !2" >> test_input.txt
echo "user3 !3" >> test_input.txt

# Run ingestor with file input (stdin mode)
python main.py --stdin < test_input.txt
```

### Testing with Live Stream

1. Ensure you have an active YouTube Live stream
2. Start Rails server
3. Create and activate a poll in Rails admin
4. Run the ingestor:
   ```bash
   python main.py
   ```
5. Send test messages in YouTube Live Chat: `!1`, `!2`, etc.
6. Check Rails logs and admin interface for votes

## Error Handling

The ingestor includes robust error handling:

- **Network errors**: Automatic retry with exponential backoff (1s → 2s → 4s ... max 30s)
- **Authentication errors (UNAUTHENTICATED)**: Automatic token refresh and retry
- **Live chat not found (NOT_FOUND)**: Automatic live chat ID refresh and retry
- **Connection errors**: Reconnection loop with exponential backoff
- **Rails API errors**: Retry logic for transient errors

## Caching

The ingestor caches the live chat ID to `live_chat_id.txt` for faster startup. The cache is automatically refreshed if:
- The streaming call returns NOT_FOUND
- You manually delete the cache file

## Message Deduplication

The ingestor uses an in-memory `set()` to track processed message IDs. This prevents duplicate processing when:
- YouTube API sends historical messages on initial connection
- The stream reconnects after a network error
- The same message is received multiple times

**Important notes:**
- The message ID set is stored in memory and resets on script restart
- Rails provides additional protection with a unique index on `[poll_id, author_channel_id]`
- Even if the Python set is lost (e.g., on restart), Rails will reject duplicate votes
- This two-layer protection ensures no votes are counted twice

## Logging

Logs include:
- Timestamp
- Log level (INFO, WARNING, ERROR)
- Vote processing status
- Connection status
- Error details and retry attempts

## Troubleshooting

### "gRPC proto files not found"
- Run the proto generation script: `.\scripts\gen_proto.bat` (PowerShell) or use the manual command
- Ensure `stream_list_pb2.py` and `stream_list_pb2_grpc.py` are in the `youtube_ingestor/` folder
- Ensure `stream_list.proto` exists in the `youtube_ingestor/` folder (root directory)

### "INGEST_KEY environment variable is required"
- Make sure you have a `.env` file with `INGEST_KEY` set
- Or set it as an environment variable: `$env:INGEST_KEY="your-key"` (PowerShell)

### "Client secret file not found"
- Ensure `client_secret.json` exists in `youtube_ingestor/` folder
- Copy it from `dota_stream/` folder if needed

### "Could not obtain live chat ID"
- Ensure you have an active YouTube Live stream
- Check that the stream status is "live" in YouTube Studio
- The ingestor looks for broadcasts with `lifeCycleStatus == "live"`

### "Connection error" or "Request timeout"
- Verify Rails server is running: `http://127.0.0.1:3000`
- Check firewall settings
- Verify `RAILS_INGEST_URL` in `.env` matches your Rails server URL

### "Authentication failed: invalid INGEST_KEY"
- Ensure `INGEST_KEY` in `.env` matches `ENV["INGEST_KEY"]` in Rails
- Restart Rails server after changing environment variables

### "UNAUTHENTICATED" gRPC error
- The ingestor will automatically refresh the token
- If it persists, delete `../dota_stream/token.json` and re-authenticate

### "NOT_FOUND" gRPC error
- The ingestor will automatically refresh the live chat ID
- Ensure the stream is still live
- Check that the live chat is enabled for your stream

### Votes not appearing in Rails
- Check that there's an active poll in Rails admin
- Verify Rails logs for incoming requests
- Check that poll options exist with matching keys (1, 2, 3, etc.)
- Ensure the stream is actually live (not just scheduled)

### Double-counting votes
- **Two-layer protection:**
  1. **Python side**: In-memory `set()` tracks processed message IDs to prevent re-processing the same message
  2. **Rails side**: Unique database index on `[poll_id, author_channel_id]` prevents duplicate votes
- Reconnecting to the stream won't cause double-counting
- Each user can only have one vote per poll
- If the Python script restarts, old messages may be processed again, but Rails will reject duplicates

## File Structure

```
youtube_ingestor/
├── main.py                 # Main ingestor script
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (create this)
├── live_chat_id.txt         # Cached live chat ID (auto-generated)
├── stream_list.proto        # gRPC proto definition
├── scripts/
│   └── gen_proto.bat        # Script to generate proto stubs
├── stream_list_pb2.py       # Generated proto messages (after gen_proto)
└── stream_list_pb2_grpc.py # Generated proto stubs (after gen_proto)
```

## Development

### Regenerating Proto Files

If you modify `stream_list.proto`, regenerate the Python stubs:

```bash
.\scripts\gen_proto.bat
```

Or manually:
```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. stream_list.proto
```

### Adding New Vote Commands

Edit the `parse_vote()` function in `main.py` to add new patterns.

## Notes

- The ingestor automatically handles token refresh
- Live chat ID is cached for faster startup
- Reconnection is automatic with exponential backoff
- Message deduplication prevents processing the same message twice within a session
- Rails provides additional vote deduplication via unique database constraints
- The message ID tracking set is in-memory and resets on restart (Rails still protects against duplicates)
- The `--stdin` flag is useful for testing without a live stream
# youtube_ingestor
