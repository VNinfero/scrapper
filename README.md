# AI Lead Generation Application

A comprehensive lead generation system that uses AI-powered search query generation and multi-platform scraping (including Instagram, LinkedIn, YouTube, FB, Twitter, Reddit, Quora and general websites.) to identify potential customers based on Ideal Customer Profiles (ICP).

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [System Flow](#system-flow)
- [Scrapers](#scrapers)
- [Database](#database)
- [API Documentation](#api-documentation)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)

## Overview

This application orchestrates multiple specialized scrapers to collect potential lead data from various platforms based on a predefined Ideal Customer Profile. It uses Google's Gemini AI to generate targeted search queries and coordinates data collection across web scraping, Instagram, LinkedIn, and YouTube platforms.

**Target Use Case:** Premium bus travel and group tour services seeking corporate clients, wedding planners, educational institutions, and family group organizers.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flask API Layer                          │
│                     (app.py)                                │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│           Lead Generation Orchestrator                      │
│                   (main.py)                                 │
├─────────────────────┼───────────────────────────────────────┤
│  ICP Management     │  Query Generation    │   URL Collection │
│  Scraper Selection  │  (Gemini AI)        │  Classification   │
└─────────────────────┼───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 Scraper Layer                               │
├─────────────────┬─────────────┬─────────────┬───────────────┤
│  Web Scraper    │ Instagram   │  LinkedIn   │   YouTube     │
│   (General)     │   Scraper   │   Scraper   │   Scraper     │
└─────────────────┴─────────────┴─────────────┴───────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 Data Storage Layer                          │
├─────────────────┬───────────────────────────────────────────┤
│   MongoDB       │            JSON Reports                   │
│  (Primary)      │           (Backup/Export)                 │
└─────────────────┴───────────────────────────────────────────┘
```

### Core Components

1. **Flask API Layer** (`app.py`) - RESTful API interface
2. **Orchestrator** (`main.py`) - Coordinates the entire pipeline
3. **Scraper Modules** - Platform-specific data collection
4. **Database Layer** - MongoDB for data persistence
5. **AI Integration** - Gemini AI for query generation

## System Flow

### Complete Pipeline Flow

```
1. ICP Definition
   ├─ Load ICP data
   └─ Define target customer characteristics

2. Scraper Selection
   ├─ User selects desired scrapers
   └─ Available: web, instagram, linkedin, youtube

3. Query Generation (AI-Powered)
   ├─ Gemini AI analyzes ICP data
   ├─ Generates 15 base search queries
   └─ Adds platform-specific variations

4. URL Collection
   ├─ Execute queries via web_url_scraper
   ├─ Collect search result URLs
   └─ Classify URLs by platform type

5. Data Scraping
   ├─ Route URLs to appropriate scrapers
   ├─ Extract lead information
   └─ Store results in MongoDB

6. Report Generation
   ├─ Compile results from all scrapers
   ├─ Generate summary statistics
   └─ Export JSON report
```

### Data Classification Flow

```
URLs from Search Results
          │
          ▼
    URL Classifier
          │
    ┌─────┼─────┬─────┬─────┐
    ▼     ▼     ▼     ▼     ▼
 General │  Instagram │ LinkedIn │ YouTube
         │           │          │
         ▼           ▼          ▼
   Web Scraper | IG Scraper | LI Scraper | YT Scraper
```

## Scrapers

### 1. Web Scraper (General)
- **Purpose**: Scrapes general websites for contact information
- **Target**: Corporate websites, business directories, service pages
- **Data Extracted**: Company info, contact details, business descriptions
- **Technology**: Selenium-based with anti-detection measures

### 2. Instagram Scraper
- **Purpose**: Extracts profile information from Instagram accounts
- **Target**: Travel influencers, wedding planners, event organizers
- **Data Extracted**: Profile bio, follower count, contact info, recent posts
- **Technology**: Browser automation with headless mode

### 3. LinkedIn Scraper
- **Purpose**: Collects professional profiles and company information
- **Target**: HR managers, corporate executives, event coordinators
- **Data Extracted**: Professional background, company details, contact information
- **Technology**: Selenium with anti-detection capabilities

### 4. YouTube Scraper
- **Purpose**: Analyzes YouTube channels for travel content creators
- **Target**: Travel vloggers, tour companies, educational channels
- **Data Extracted**: Channel info, subscriber count, video content analysis
- **Technology**: Browser automation with content analysis

## Database

### MongoDB Collections

#### URLs Collection
```javascript
{
  "_id": ObjectId,
  "url": "https://example.com",
  "url_type": "general|instagram|linkedin|youtube",
  "query": "search query used",
  "domain": "example.com",
  "created_at": ISODate,
  "scraped": boolean
}
```

#### Platform-Specific Collections
- `web_leads` - General website data
- `instagram_leads` - Instagram profile data  
- `linkedin_leads` - LinkedIn profile/company data
- `youtube_leads` - YouTube channel data

#### Unified Leads Collection
All scraped leads are normalized and stored in the `unified_leads` collection with a consistent schema:

```javascript
{
  "_id": ObjectId,
  "url": "source_url",
  "platform": "instagram|linkedin|youtube|web",
  "content_type": "profile|post|video|article",
  "source": "scraper_source",
  "profile": {
    "username": "username",
    "full_name": "Full Name",
    "bio": "Bio text",
    "job_title": "Job Title",
    "location": "City, Country",
    "employee_count": "100-500"
  },
  "contact": {
    "emails": ["email@example.com"],
    "phone_numbers": ["+1234567890"],
    "address": "123 Street, City, Country",
    "websites": ["https://example.com"],
    "social_media_handles": {
      "twitter": "@handle",
      "facebook": "username",
      "linkedin": "username"
    },
    "bio_links": ["https://link1.com", "https://link2.com"]
  },
  "content": {
    "caption": "Post/video caption",
    "upload_date": "2024-01-01T12:00:00Z",
    "channel_name": "Channel Name",
    "author_name": "Author Name"
  },
  "metadata": {
    "scraped_at": "2024-01-01T12:00:00Z",
    "data_quality_score": 0.95,
    "original_source": "original_platform_collection"
  }
}
```

### Data Transformation Pipeline

1. **Platform-Specific Collection**: Raw data is first stored in platform-specific collections
2. **Normalization**: Data is transformed into the unified schema
3. **Deduplication**: Duplicate leads are identified and merged
4. **Quality Scoring**: Each lead receives a data quality score
5. **Unified Storage**: Processed leads are stored in the `unified_leads` collection

### Database Services

The application uses a centralized MongoDB manager that provides:
- Connection pooling
- Data validation and transformation
- Error handling and retries
- Consistent document structure across platforms
- URL deduplication
- Batch processing for improved performance
- Data quality scoring
- Schema validation and migration support

## API Documentation

### Base URL
```
http://localhost:5000
```

### Endpoints

#### Health Check
```http
GET /health
```
**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00",
  "service": "Lead Generation Backend"
}
```

#### Get Available Scrapers
```http
GET /api/scrapers
```
**Response:**
```json
{
  "success": true,
  "data": {
    "available_scrapers": ["web_scraper", "instagram", "linkedin", "youtube"],
    "scrapers_info": {
      "web_scraper": "General web scraping for websites",
      "instagram": "Instagram profiles and posts",
      "linkedin": "LinkedIn profiles and companies",
      "youtube": "YouTube channels and videos"
    }
  }
}
```

#### Get ICP Template
```http
GET /api/icp/template
```
**Response:**
```json
{
  "success": true,
  "data": {
    "icp_template": {
      "product_details": {
        "product_name": "Premium Bus Travel & Group Tour Services",
        "product_category": "Travel & Tourism/Transportation Services",
        "usps": ["..."],
        "pain_points_solved": ["..."]
      },
      "icp_information": {
        "target_industry": ["Corporate Companies", "Educational Institutions", "..."],
        "competitor_companies": ["RedBus", "MakeMyTrip", "..."],
        "company_size": "10-1000+ employees/members",
        "decision_maker_persona": ["HR Manager", "Event Coordinator", "..."],
        "region": ["India", "Major Cities", "Tourist Destinations"],
        "budget_range": "$5,000-$50,000 annually",
        "travel_occasions": ["Corporate offsites", "Wedding functions", "..."]
      }
    }
  }
}
```

#### Run Complete Lead Generation Pipeline
```http
POST /api/lead-generation/run
```
**Request Body:**
```json
{
  "icp_data": {
    "product_details": {
      "product_name": "Premium Bus Travel Services",
      "product_category": "Travel & Tourism",
      "usps": ["Luxury fleet", "Custom packages"],
      "pain_points_solved": ["Complex logistics", "High costs"]
    },
    "icp_information": {
      "target_industry": ["Corporate Companies", "Wedding Planners"],
      "company_size": "10-500 employees",
      "decision_maker_persona": ["HR Manager", "Event Coordinator"],
      "region": ["India", "Major Cities"],
      "budget_range": "$5,000-$25,000 annually"
    }
  },
  "selected_scrapers": ["web_scraper", "instagram", "linkedin"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "pipeline_metadata": {
      "execution_time_seconds": 245.67,
      "start_time": "2025-01-15T10:30:00",
      "end_time": "2025-01-15T10:34:05",
      "selected_scrapers": ["web_scraper", "instagram", "linkedin"],
      "total_queries_generated": 9,
      "total_urls_collected": 156,
      "successful_scrapers": 3,
      "total_scrapers": 3
    },
    "url_collection": {
      "classified_urls_count": {
        "general": 89,
        "instagram": 23,
        "linkedin": 31,
        "youtube": 13
      },
      "total_urls": 156
    },
    "scraper_results_summary": {
      "web_scraper": {
        "status": "success",
        "leads_found": 45,
        "urls_processed": 10
      },
      "instagram": {
        "status": "success", 
        "profiles_found": 12,
        "success_rate": 0.85
      },
      "linkedin": {
        "status": "success",
        "profiles_found": 18,
        "failed_scrapes": 2
      }
    },
    "report_file": "orchestration_report_20250115_103405.json",
    "queries_used": [
      "corporate team building outings bus travel",
      "wedding destination travel packages India",
      "..."
    ]
  }
}
```

#### Generate Search Queries Only
```http
POST /api/queries/generate
```
**Request Body:**
```json
{
  "icp_data": { /* same as above */ },
  "selected_scrapers": ["web_scraper", "instagram"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "queries": [
      "corporate team building outings bus travel",
      "wedding destination travel packages India",
      "family reunion vacation planning services",
      "..."
    ],
    "total_queries": 9,
    "selected_scrapers": ["web_scraper", "instagram"]
  }
}
```

#### Get System Status
```http
GET /api/status
```
**Response:**
```json
{
  "success": true,
  "data": {
    "system_status": "operational",
    "components": {
      "gemini_ai": {
        "available": true,
        "status": "connected"
      },
      "mongodb": {
        "available": true,
        "status": "connected"
      },
      "scrapers": {
        "web_scraper": true,
        "instagram": true,
        "linkedin": true,
        "youtube": true
      }
    },
    "timestamp": "2025-01-15T10:30:00"
  }
}
```
# API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/scrapers` | List available scrapers |
| GET | `/api/icp/template` | Get ICP data template |
| POST | `/api/lead-generation/run` | Run complete pipeline |
| POST | `/api/queries/generate` | Generate queries only |
| GET | `/api/status` | System status |

## Installation

### Prerequisites
- Python 3.8+
- MongoDB instance
- Google Gemini AI API key
- Chrome/Chromium browser (for scrapers)

### Setup Steps

1. **Clone the repository**
```bash
git clone <repository-url>
cd lead-generation-app
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Install additional packages**
```bash
pip install google-generativeai python-dotenv flask flask-cors
```

4. **Set up environment variables**
Create a `.env` file:
```env
GEMINI_API_KEY=your_gemini_api_key_here

GOOGLE_API_KEY=you_google_client_key
GOOGLE_SEARCH_ENGINE_ID=your_google_search_engine_id

MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE_NAME=lead_generation_db

FLASK_ENV=development
PORT=5000
```

5. **Install Chrome WebDriver**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# Or download ChromeDriver manually
# Place in PATH or specify path in scraper configs
```

### MongoDB Setup
1. Install MongoDB locally or use MongoDB Atlas
2. Create a database named `lead_generation`
3. The application will automatically create necessary collections

### Gemini AI Setup
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Generate an API key
3. Add the key to your `.env` file

## Configuration

### MongoDB Configuration
The application expects MongoDB to be running on the default port (27017). Update the connection string in your environment variables if using a different setup.

### Scraper Configuration
Each scraper module has its own configuration options:
- **Headless mode**: Enabled by default for production
- **Anti-detection**: Enabled to bypass bot detection
- **Rate limiting**: Built-in delays between requests
- **Error handling**: Automatic retries with exponential backoff

### AI Configuration
Configure the Gemini AI model in the orchestrator:
- Model: `gemini-2.0-flash`
- Temperature: Default (configurable)
- Max tokens: Default (configurable)

## Usage

### Running the Flask API

1. **Start the Flask server**
```bash
python app.py
```

2. **Access the API**
The server will start on `http://localhost:5000`

### Using the CLI Interface

1. **Run the orchestrator directly**
```bash
python main.py
```

2. **Follow the interactive prompts**
- Select scrapers to use
- Monitor progress in real-time
- Review results in generated reports

### API Usage Examples

#### Complete Pipeline via API
```python
import requests

# Define your ICP data
icp_data = {
    "product_details": {
        "product_name": "Premium Bus Travel Services",
        # ... other product details
    },
    "icp_information": {
        "target_industry": ["Corporate Companies", "Wedding Planners"],
        # ... other ICP details
    }
}

# Run the complete pipeline
response = requests.post(
    "http://localhost:5000/api/lead-generation/run",
    json={
        "icp_data": icp_data,
        "selected_scrapers": ["web_scraper", "instagram", "linkedin"]
    }
)

results = response.json()
print(f"Pipeline completed in {results['data']['pipeline_metadata']['execution_time_seconds']} seconds")
```

#### Generate Queries Only
```python
response = requests.post(
    "http://localhost:5000/api/queries/generate",
    json={
        "icp_data": icp_data,
        "selected_scrapers": ["web_scraper", "instagram"]
    }
)

queries = response.json()['data']['queries']
print(f"Generated {len(queries)} search queries")
```


## Project Structure

```
lead-generation-app/
├── main.py                    # Main orchestrator
├── app.py                     # Flask API server
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables
│
├── web_url_scraper/          # URL collection module
│   ├── main.py
│   └── database_service.py
│
├── web_scraper/              # General web scraper
│   └── main_app.py
│
├── instagram_scraper/        # Instagram scraper
│   └── main.py
│
├── linkedin_scraper/         # LinkedIn scraper
│   └── main.py
│
├── yt_scraper/              # YouTube scraper
│   └── main.py
│
├── database/                 # Database management
│   └── mongodb_manager.py
│
└── reports/                  # Generated reports
    ├── orchestration_report_*.json
    ├── web_scraper_results.json
    ├── instagram_results.json
    ├── linkedin_results.json
    └── youtube_results.json
```

## Key Features

### AI-Powered Query Generation
- Uses Google Gemini AI to analyze ICP data
- Generates contextually relevant search queries
- Adapts queries based on target industries and personas
- Falls back to predefined queries if AI is unavailable

### Multi-Platform Data Collection
- **Web Scraper**: General websites and business directories
- **Instagram**: Social media profiles and engagement data
- **LinkedIn**: Professional networks and company information
- **YouTube**: Content creators and channel analytics

### Intelligent URL Classification
- Automatically routes URLs to appropriate scrapers
- Supports multiple domain patterns per platform
- Handles edge cases and unknown domains gracefully

### Robust Error Handling
- Individual scraper failures don't stop the pipeline
- Automatic retries with exponential backoff
- Comprehensive logging at all levels
- Graceful degradation when services are unavailable

### Data Persistence
- MongoDB for scalable data storage
- JSON exports for backup and analysis
- Automatic deduplication of URLs and leads
- Historical data tracking

## Error Handling

### Common Error Scenarios

1. **Gemini AI Unavailable**
   - Falls back to predefined queries
   - Logs warning but continues execution

2. **MongoDB Connection Failed**
   - Continues with file-based storage
   - Warns user about data persistence limitations

3. **Individual Scraper Failures**
   - Logs error details
   - Continues with other scrapers
   - Reports failure in final summary

4. **Network Issues**
   - Implements retry logic with exponential backoff
   - Times out gracefully after maximum attempts
   - Preserves partial results

### API Error Responses

```json
{
  "success": false,
  "error": "Detailed error message",
  "error_type": "validation|system|network",
  "timestamp": "2025-01-15T10:30:00"
}
```

## Performance Considerations

### Rate Limiting
- 2-second delays between search queries
- Platform-specific rate limiting for scrapers
- Configurable batch sizes for URL processing

### Resource Management
- Headless browser mode for reduced memory usage
- Automatic cleanup of browser instances
- Limited concurrent scraper instances

### Scalability
- Asynchronous operation support
- Modular scraper architecture
- Database connection pooling
- Horizontal scaling potential

## Security Features

### Anti-Detection Measures
- Randomized user agents
- Proxy support (configurable)
- Human-like interaction patterns
- Session management

### Data Protection
- No storage of sensitive authentication data
- Configurable data retention policies
- Secure API endpoints
- Input validation and sanitization

## Monitoring and Logging

### Log Levels
- **INFO**: Normal operation status
- **WARNING**: Non-critical issues (fallbacks, retries)
- **ERROR**: Failed operations with error details

### Metrics Tracked
- Query generation success rate
- URL collection statistics
- Scraper success rates
- Pipeline execution time
- Data quality metrics

## Future Enhancements

### Planned Features
1. **Dynamic ICP Management**
   - Web form for ICP configuration
   - Multiple ICP profiles support
   - ICP validation and optimization

2. **Advanced AI Integration**
   - Lead qualification scoring
   - Sentiment analysis of scraped content
   - Automated follow-up suggestions

3. **Enhanced Data Processing**
   - Real-time duplicate detection
   - Lead enrichment from multiple sources
   - Export to CRM systems

4. **Monitoring Dashboard**
   - Real-time pipeline status
   - Performance analytics
   - Data quality metrics

### Extensibility
- Plugin architecture for new scrapers
- Configurable data processing pipelines
- Custom export formats
- Third-party integrations

## Troubleshooting

### Common Issues

1. **Scrapers failing to start**
   - Check Chrome/ChromeDriver installation
   - Verify display configuration (for headless mode)
   - Check system resources and memory

2. **MongoDB connection errors**
   - Verify MongoDB service is running
   - Check connection string format
   - Ensure database permissions

3. **Gemini AI errors**
   - Verify API key is correctly set
   - Check network connectivity
   - Monitor API quota usage

4. **Low URL collection**
   - Adjust search queries for broader results
   - Check web_url_scraper configuration
   - Verify search engine accessibility

### Debug Mode
Enable detailed logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring
Track execution with timing logs and monitor system resources during scraping operations.

## Contributing

When adding new scrapers or modifying the pipeline:

1. Follow the existing scraper interface patterns
2. Implement proper error handling and logging
3. Add MongoDB integration
4. Update the orchestrator routing logic
5. Document new configuration options
6. Add appropriate tests

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: This application is designed for legitimate business lead generation purposes. Please ensure compliance with all applicable laws and platform terms of service when using this software.