"""
Flask application for UnifyDB Web Dashboard.
"""

from typing import Optional, Dict, Any
import os

try:
    from flask import Flask, render_template, jsonify, request
    from flask_cors import CORS
except ImportError:
    raise ImportError(
        "Flask not installed. Install with: pip install unifydb[web]"
    )


def create_flask_app(config: Optional[dict] = None) -> Flask:
    """
    Create and configure Flask application.
    
    Args:
        config: Optional configuration dictionary
        
    Returns:
        Flask application instance
    """
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static")
    )
    
    # Default configuration
    app.config.update({
        "SECRET_KEY": os.environ.get("UNIFYDB_SECRET", "dev-secret-key"),
        "UNIFYDB_CONNECTIONS": {},
    })
    
    if config:
        app.config.update(config)
    
    # Enable CORS
    CORS(app)
    
    # Register routes
    from .routes import register_routes
    register_routes(app)
    
    # Register API
    from .api import register_api
    register_api(app)
    
    return app
