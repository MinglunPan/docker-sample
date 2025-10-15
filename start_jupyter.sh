#!/bin/bash

# Start Jupyter Lab for Dagster Analysis
# This script provides an easy way to start Jupyter Lab with Dagster integration

echo "🚀 Starting Dagster + Jupyter Lab Environment"
echo "=============================================="

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Build and start services
echo "📦 Building Docker images..."
docker-compose build

echo "🏃‍♂️ Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo "🔍 Checking service status..."
DAGSTER_STATUS=$(docker-compose ps dagster --format "table {{.State}}" | grep -v STATE)
JUPYTER_STATUS=$(docker-compose ps jupyter --format "table {{.State}}" | grep -v STATE)

if [[ "$DAGSTER_STATUS" == *"Up"* ]]; then
    echo "✅ Dagster is running at: http://localhost:3000"
else
    echo "❌ Dagster failed to start"
    docker-compose logs dagster
fi

if [[ "$JUPYTER_STATUS" == *"Up"* ]]; then
    echo "✅ Jupyter Lab is running at: http://localhost:8890"
    echo "📓 Example notebook: notebooks/dagster_assets_analysis.ipynb"
else
    echo "❌ Jupyter Lab failed to start"
    docker-compose logs jupyter
fi

echo ""
echo "🛠️  Available commands:"
echo "  docker-compose logs dagster  # View Dagster logs"
echo "  docker-compose logs jupyter  # View Jupyter logs"
echo "  docker-compose down          # Stop all services"
echo "  docker-compose exec jupyter bash  # Access Jupyter container"
echo ""
echo "📚 Quick start in Jupyter:"
echo "  1. Open http://localhost:8890"
echo "  2. Navigate to notebooks/dagster_assets_analysis.ipynb"
echo "  3. Run the cells to analyze your Dagster assets"
echo ""
echo "Happy analyzing! 🎉"