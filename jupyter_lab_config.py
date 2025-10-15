# Jupyter Lab Configuration for Dagster Project
c = get_config()

# Allow all origins for development (adjust for production)
c.ServerApp.allow_origin = '*'
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8890
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True

# Set the notebook directory to the project root
c.ServerApp.root_dir = '/app'

# Disable token authentication for local development (enable for production)
c.ServerApp.token = ''
c.ServerApp.password = ''

# Enable extensions
c.ServerApp.jpserver_extensions = {
    'jupyterlab': True
}

# Configure file browser to show hidden files
c.ContentsManager.allow_hidden = True

# Set up kernel settings
c.KernelManager.autorestart = True