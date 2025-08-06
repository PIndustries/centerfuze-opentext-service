# CenterFuze OpenText Service

A comprehensive microservice for integrating with OpenText APIs, providing account management, fax usage tracking, number porting, and usage data aggregation through NATS messaging.

## Features

- **Account Management**: Retrieve and synchronize OpenText accounts with child account hierarchy support
- **Fax Usage Tracking**: Monitor fax page usage across accounts with cost calculations
- **Number Porting**: Track and manage phone number porting status and updates
- **Usage Data Aggregation**: Aggregate usage metrics across multiple accounts and time periods
- **NATS Integration**: Full NATS messaging support with structured topic handling
- **Rate Limiting**: Adaptive rate limiting to prevent API throttling
- **Caching**: Built-in caching with TTL support for improved performance
- **Batch Processing**: Efficient batch operations for handling multiple records
- **Health Monitoring**: Comprehensive health checks and monitoring endpoints

## Architecture

The service is built with a modular architecture:

```
centerfuze-opentext-service/
├── app/
│   ├── config/          # Configuration management
│   ├── controllers/     # NATS message handlers
│   ├── models/         # Data models
│   ├── services/       # Business logic
│   └── utils/          # Utilities (caching, rate limiting, logging)
├── tests/              # Test files
├── docs/               # Documentation
├── main.py             # Application entry point
├── Dockerfile          # Container configuration
└── requirements.txt    # Python dependencies
```

## NATS Topics

The service subscribes to the following NATS topics:

### Account Management
- `opentext.account.sync` - Synchronize multiple accounts
- `opentext.account.get` - Retrieve single account

### Fax Usage
- `opentext.fax.usage.get` - Get fax usage for an account
- `opentext.fax.usage.sync` - Synchronize fax usage for multiple accounts

### Number Porting
- `opentext.porting.status` - Get porting status for phone numbers
- `opentext.porting.update` - Update porting status

### Usage Aggregation
- `opentext.usage.aggregate` - Aggregate usage data across accounts

### Health Check
- `opentext.health.check` - Service health status

## Quick Start

### Prerequisites

- Python 3.11+
- NATS Server
- OpenText API credentials

### Installation

1. Clone the repository:
```bash
git clone https://github.com/pindustries/centerfuze-opentext-service.git
cd centerfuze-opentext-service
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Run the service:
```bash
python main.py
```

### Docker Deployment

1. Build the Docker image:
```bash
docker build -t centerfuze-opentext-service .
```

2. Run the container:
```bash
docker run -d \
  --name opentext-service \
  -e NATS_SERVERS=nats://your-nats-server:4222 \
  -e OPENTEXT_API_BASE_URL=https://api.opentext.com/v1 \
  -e OPENTEXT_API_KEY=your_api_key \
  -e OPENTEXT_API_SECRET=your_api_secret \
  centerfuze-opentext-service
```

## Configuration

The service is configured through environment variables. See `.env.example` for all available options.

### Key Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `NATS_SERVERS` | NATS server URLs (comma-separated) | `nats://localhost:4222` |
| `OPENTEXT_API_BASE_URL` | OpenText API base URL | Required |
| `OPENTEXT_API_KEY` | OpenText API key | Required |
| `OPENTEXT_API_SECRET` | OpenText API secret | Required |
| `RATE_LIMIT_REQUESTS_PER_SECOND` | Request rate limit | `10.0` |
| `CACHE_ENABLED` | Enable caching | `true` |
| `LOG_LEVEL` | Logging level | `INFO` |

## API Usage

### Account Synchronization

```python
# NATS message to sync accounts
await nats.publish("opentext.account.sync", json.dumps({
    "account_ids": ["account1", "account2"],
    "include_children": true
}))
```

### Fax Usage Retrieval

```python
# Get fax usage for an account
await nats.publish("opentext.fax.usage.get", json.dumps({
    "account_id": "account123",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T23:59:59Z"
}))
```

### Number Porting Status

```python
# Check porting status for phone numbers
await nats.publish("opentext.porting.status", json.dumps({
    "phone_numbers": ["1234567890", "0987654321"]
}))
```

### Usage Aggregation

```python
# Aggregate usage data
await nats.publish("opentext.usage.aggregate", json.dumps({
    "account_ids": ["account1", "account2"],
    "usage_type": "fax_pages_sent",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T23:59:59Z"
}))
```

## Data Models

### OpenTextAccount
Represents an OpenText customer account with:
- Account ID and name
- Child account relationships
- Status and timestamps
- Contact and billing information

### FaxUsage
Tracks fax usage metrics:
- Pages sent and received
- Cost calculations
- Period-based reporting

### NumberPorting
Manages phone number porting:
- Porting status tracking
- Carrier information
- Request and completion dates

### UsageData
Generic usage tracking:
- Various usage types (fax, phone, SMS, data)
- Quantity and cost tracking
- Flexible metadata support

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests
pytest

# Run with coverage
pytest --cov=app tests/
```

### Code Quality

The project uses several tools for code quality:

```bash
# Format code
black app/ tests/

# Sort imports
isort app/ tests/

# Lint code
flake8 app/ tests/

# Type checking
mypy app/
```

### Adding New Features

1. Create feature branch: `git checkout -b feature/new-feature`
2. Implement changes with tests
3. Run quality checks
4. Submit pull request

## Monitoring and Observability

### Health Checks

The service provides comprehensive health checks:

```python
# Check service health
await nats.publish("opentext.health.check", b"")
```

Health check response includes:
- Service status
- Component health (NATS, OpenText API, cache)
- Performance metrics

### Logging

The service uses structured JSON logging in production with the following levels:
- `DEBUG`: Detailed debugging information
- `INFO`: General operational messages
- `WARNING`: Important events that aren't errors
- `ERROR`: Error conditions that don't stop the service
- `CRITICAL`: Serious errors that may abort the service

### Metrics

Rate limiter and cache statistics are available through the health check endpoint.

## Troubleshooting

### Common Issues

1. **NATS Connection Failed**
   - Verify NATS server is running
   - Check network connectivity
   - Validate NATS_SERVERS configuration

2. **OpenText API Errors**
   - Verify API credentials
   - Check rate limiting settings
   - Review API endpoint URL

3. **High Memory Usage**
   - Adjust CACHE_MAX_SIZE
   - Reduce BATCH_SIZE
   - Monitor cache cleanup

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
export LOG_LEVEL=DEBUG
python main.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is proprietary software owned by CenterFuze.

## Support

For support, please contact the CenterFuze development team or create an issue in the repository.