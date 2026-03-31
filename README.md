# Inhouse Hours Report Monthly

This Python script automates the generation of monthly in-house member activity and hour statistics reports from Google Sheets data. It processes attendance records and merged reports to create detailed breakdowns and summaries, then uploads the results back to Google Sheets.

## Features

- Calculates active days and office hours from attendance sheets
- Aggregates monthly metrics per team member from merged reports
- Merges attendance and report data
- Generates detailed breakdown and summary sheets
- Automatically uploads results to Google Sheets

## Prerequisites

- Python 3.8+
- Google Cloud service account with Sheets API access
- Access to the required Google Sheets (attendance, reports, output)

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up Google Cloud credentials:
   - Create a service account in Google Cloud Console
   - Enable Google Sheets API
   - Download the JSON key file
   - Set the path to the credentials file

## Configuration

The script uses environment variables for configuration. You can set them in your environment or pass as command-line arguments:

- `GOOGLE_CREDS_FILE`: Path to Google service account JSON credentials file
- `DELIVERY_SHEET_KEY`: Google Sheet key for the attendance source
- `DELIVERY_WORKSHEET_NAME`: Worksheet name for the attendance source
- `REPORT_SHEET_KEY`: Google Sheet key for the merged report source
- `REPORT_WORKSHEET_NAME`: Worksheet name for the merged report source
- `OUTPUT_SHEET_KEY`: Google Sheet key for the upload target
- `OUTPUT_BREAKDOWN_WORKSHEET`: Worksheet name for the detailed output
- `OUTPUT_SUMMARY_WORKSHEET`: Worksheet name for the summary output
- `REPORT_YEAR_FILTER`: Year to filter reports (default: 2026)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

## Usage

Run the script directly:

```bash
python inhouse_members_active_days_and_effective_hour_stats.py
```

Or with custom arguments:

```bash
python inhouse_members_active_days_and_effective_hour_stats.py --creds-file path/to/creds.json --report-year-filter 2026
```

## GitHub Actions Setup

This repository includes a GitHub Actions workflow that runs automatically on the 15th of every month.

### Required Secrets

Set up the following secrets in your GitHub repository settings:

- `GOOGLE_CREDS`: The content of your Google service account JSON key file (paste the entire JSON as-is)
- `DELIVERY_SHEET_KEY`: Google Sheet key for attendance data
- `REPORT_SHEET_KEY`: Google Sheet key for merged reports
- `OUTPUT_SHEET_KEY`: Google Sheet key for output destination

### Optional Secrets

- `DELIVERY_WORKSHEET_NAME`: Worksheet name for attendance (default: "Team List & Activity")
- `REPORT_WORKSHEET_NAME`: Worksheet name for reports (default: "Merged")
- `OUTPUT_BREAKDOWN_WORKSHEET`: Output worksheet name for breakdown (default: "Total Breakdown")
- `OUTPUT_SUMMARY_WORKSHEET`: Output worksheet name for summary (default: "Summary")
- `REPORT_YEAR_FILTER`: Year filter (default: "2026")

The workflow will automatically:
1. Check out the code
2. Set up Python environment
3. Install dependencies
4. Configure Google credentials
5. Run the report generation script

## Security Notes

- Never commit Google service account JSON files to the repository
- Use GitHub secrets for sensitive configuration
- Ensure the service account has minimal required permissions for the sheets

## License

[Add your license here]