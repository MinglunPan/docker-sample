# Docker Sample Project

A Docker-based data pipeline project using Dagster for orchestration and Jupyter Lab for data analysis.

## Project Overview

This project demonstrates a modern data engineering setup with:
- **Dagster**: Data orchestration and pipeline management
- **Jupyter Lab**: Interactive data analysis and visualization
- **Python 3.12**: Modern Python environment with data science libraries
- **Docker**: Containerized development environment

## Services

- **Dagster UI**: Available at `http://localhost:3000`
- **Jupyter Lab**: Available at `http://localhost:8890`

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Git (to clone the repository)

### Setup Instructions

1. **Clone the repository** (if not already done)
   ```bash
   git clone <repository-url>
   cd docker-sample
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   ```
   > Note: If `.env.example` doesn't exist, create a `.env` file with your environment variables

3. **Build the containers**
   ```bash
   docker compose build
   ```

4. **Start all services**
   ```bash
   docker compose up
   ```

5. **Start services in background** (optional)
   ```bash
   docker compose up -d
   ```

6. **Start only Jupyter** (if you only need Jupyter)
   ```bash
   docker compose up jupyter
   ```

## Access Points

- **Dagster UI**: http://localhost:3000
- **Jupyter Lab**: http://localhost:8890

## Project Structure

```
├── src/
│   └── demo/                 # Main application code
│       ├── definitions.py    # Dagster definitions
│       ├── defs/             # Dagster assets and resources
│       └── utils.py          # Utility functions
├── notebooks/                # Jupyter notebooks
│   └── sample_analysis.ipynb
├── data/                     # Data storage directory
├── tests/                    # Test files
├── docker-compose.yml       # Docker services configuration
├── Dockerfile               # Container build instructions
├── requirements.txt         # Python dependencies
└── pyproject.toml          # Project configuration
```

## Development

### Running Individual Services

**Start only Dagster:**
```bash
docker compose up dagster
```

**Start only Jupyter:**
```bash
docker compose up jupyter
```

### Stopping Services

```bash
docker compose down
```

### Rebuilding After Changes

```bash
docker compose down
docker compose build
docker compose up
```

## Data Pipeline

The project includes sample data pipelines demonstrating:
- Data ingestion and processing
- Asset management with Dagster
- Interactive analysis with Jupyter

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 3000 and 8890 are not in use
2. **Permission issues**: Make sure Docker has proper permissions
3. **Build failures**: Check that all dependencies are properly installed

### Logs

View service logs:
```bash
docker compose logs dagster
docker compose logs jupyter
```

## Contributing

1. Make your changes
2. Test with `docker compose up`
3. Submit a pull request

## License

[Add your license information here]