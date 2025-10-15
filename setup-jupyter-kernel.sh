#!/bin/bash

# Jupyter Kernel Setup Script
# This script configures a complete Jupyter kernel with all required dependencies

echo "🔧 Setting up Jupyter kernel for Dagster analysis..."

# Create custom kernel directory
KERNEL_DIR="/root/.local/share/jupyter/kernels/dagster-analytics"
mkdir -p "$KERNEL_DIR"

# Create kernel specification
cat > "$KERNEL_DIR/kernel.json" << EOF
{
 "argv": [
  "python",
  "-m",
  "ipykernel_launcher",
  "-f",
  "{connection_file}"
 ],
 "display_name": "Dagster Analytics",
 "language": "python",
 "env": {
  "PYTHONPATH": "/app:/app/src:/app/src/demo",
  "DAGSTER_HOME": "/app/.dagster_home"
 }
}
EOF

# Create kernel startup script
cat > "$KERNEL_DIR/startup.py" << 'EOF'
# Jupyter Kernel Startup Script
import sys
import os

# Ensure proper Python paths
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/src')  
sys.path.insert(0, '/app/src/demo')

# Set environment variables
os.environ['DAGSTER_HOME'] = '/app/.dagster_home'
os.environ['PYTHONPATH'] = '/app:/app/src:/app/src/demo'

print("✅ Dagster Analytics kernel initialized")
print(f"Python path: {sys.path[:5]}...")  # Show first 5 paths
EOF

# Create custom ipython startup
IPYTHON_DIR="/root/.ipython/profile_default/startup"
mkdir -p "$IPYTHON_DIR"

cat > "$IPYTHON_DIR/00-dagster-setup.py" << 'EOF'
# IPython startup script for Dagster environment
import sys
import os
import warnings

# Suppress specific warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')

# Setup paths
sys.path.insert(0, '/app/src/demo')

# Common imports for convenience
try:
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import plotly.express as px
    from dagster import DagsterInstance
    
    # Try to import project definitions
    try:
        import definitions
        defs = definitions.defs
        print("📊 Dagster definitions loaded successfully")
    except ImportError as e:
        print(f"⚠️  Could not load Dagster definitions: {e}")
    
    print("🚀 Environment ready! Common packages imported:")
    print("   - pandas as pd")
    print("   - numpy as np") 
    print("   - matplotlib.pyplot as plt")
    print("   - seaborn as sns")
    print("   - plotly.express as px")
    print("   - DagsterInstance")
    
except ImportError as e:
    print(f"❌ Error setting up environment: {e}")
EOF

echo "✅ Jupyter kernel setup complete!"
echo "🎯 Kernel name: 'Dagster Analytics'"
echo "📂 Kernel location: $KERNEL_DIR"