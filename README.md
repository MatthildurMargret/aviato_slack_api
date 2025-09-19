# Aviato Company Enrichment Slack Bot

A Slack bot that enriches company data using the Aviato API and returns comprehensive company information as a JSON file.

## Features

- **Slash Command**: Use `/company` followed by a website URL or LinkedIn company URL
- **Company Enrichment**: Fetches comprehensive company data including:
  - Basic company information (name, description, location, etc.)
  - Funding information (total funding, funding rounds)
  - Founders and investors
  - Acquisitions
  - Web traffic analytics
  - Industry classification
  - Patents and government awards
- **File Upload**: Returns enriched data as a formatted JSON file in the same Slack thread

## Setup

### Prerequisites

1. Python 3.7+
2. Slack workspace with admin permissions
3. Aviato API key

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables in `.env`:
```
AVIATO_API_KEY=your_aviato_api_key
SLACK_APP_TOKEN=xapp-your-app-token
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_CLIENT_SECRET=your-client-secret
```

### Slack App Configuration

1. Create a new Slack app at https://api.slack.com/apps
2. Enable Socket Mode and generate an App-Level Token
3. Add the following OAuth scopes:
   - `chat:write`
   - `files:write`
   - `commands`
4. Create a slash command `/company` with the following settings:
   - Command: `/company`
   - Request URL: Not needed (using Socket Mode)
   - Short Description: "Enrich company data"
   - Usage Hint: "https://example.com or https://linkedin.com/company/example"
5. Install the app to your workspace

## Usage

### Running the Bot

```bash
python app.py
```

The bot will start and connect to Slack via Socket Mode.

### Using the Bot

In any Slack channel where the bot is present, use:

```
/company https://example.com
```

or

```
/company https://linkedin.com/company/example-company
```

The bot will:
1. Show a processing message
2. Fetch company data from the Aviato API
3. Upload a JSON file with the enriched data
4. Update the processing message to indicate completion

### Example Output

The bot returns a JSON file containing:
- Company basic information
- Location details
- Industry classification
- Funding information
- Founders and investors
- Acquisitions
- Web traffic analytics
- Patents and awards
- Job listings
- News mentions

## Error Handling

- Invalid URLs: Bot will ask for a valid website or LinkedIn URL
- API errors: Bot will display error messages
- Network issues: Bot will handle timeouts and connection errors gracefully

## Logging

The bot logs all activities with timestamps. Check the console output for debugging information.

## File Structure

```
aviato_api_slack/
├── api/
│   ├── enrich_company.py    # Aviato API integration
│   └── ...
├── slack/
│   ├── bot.py              # Main Slack bot implementation
│   └── handlers.py         # Additional handlers (if needed)
├── app.py                  # Main application entry point
├── requirements.txt        # Python dependencies
├── .env                   # Environment variables
└── README.md              # This file
```

## Troubleshooting

1. **Import errors**: Ensure all dependencies are installed and the current directory is in the Python path
2. **Slack connection issues**: Verify your tokens are correct and the app has proper permissions
3. **API errors**: Check your Aviato API key and ensure you have sufficient quota
4. **File upload issues**: Ensure the bot has `files:write` permission in your Slack workspace
