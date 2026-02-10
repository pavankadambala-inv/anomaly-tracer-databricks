"""Load secrets from app.secrets.yaml file."""

import os
import yaml
from pathlib import Path
from typing import Optional


def load_secrets_from_yaml(secrets_file: Optional[str] = None) -> dict:
    """
    Load secrets from YAML file and set as environment variables.
    
    Args:
        secrets_file: Path to secrets YAML file. Defaults to app.secrets.yaml in project root.
        
    Returns:
        Dictionary of loaded secrets
    """
    if secrets_file is None:
        # Default to app.secrets.yaml in project root
        project_root = Path(__file__).parent.parent
        secrets_file = project_root / "app.secrets.yaml"
    else:
        secrets_file = Path(secrets_file)
    
    if not secrets_file.exists():
        print(f"⚠️  Secrets file not found: {secrets_file}")
        print(f"   Falling back to environment variables")
        return {}
    
    try:
        with open(secrets_file, 'r') as f:
            secrets = yaml.safe_load(f)
        
        if not secrets:
            print(f"⚠️  Secrets file is empty: {secrets_file}")
            return {}
        
        # Set environment variables
        loaded_count = 0
        for key, value in secrets.items():
            if key.startswith('#') or not value:
                continue
            
            # Only set if not already set (environment variables take precedence)
            if key not in os.environ:
                os.environ[key] = str(value)
                loaded_count += 1
                
                # Show abbreviated value for security
                if 'SECRET' in key or 'PASSWORD' in key or 'TOKEN' in key or 'KEY' in key:
                    display_value = f"{str(value)[:10]}..." if len(str(value)) > 10 else "***"
                elif 'JSON' in key:
                    display_value = f"{len(str(value))} characters"
                else:
                    display_value = f"{str(value)[:20]}..." if len(str(value)) > 20 else str(value)
                
                print(f"  ✓ Loaded {key}: {display_value}")
        
        print(f"✓ Loaded {loaded_count} secrets from {secrets_file.name}")
        return secrets
        
    except Exception as e:
        print(f"❌ Error loading secrets from {secrets_file}: {e}")
        return {}


def ensure_required_secrets(required: list[str]) -> bool:
    """
    Check that all required secrets are set in environment.
    
    Args:
        required: List of required environment variable names
        
    Returns:
        True if all required secrets are set, False otherwise
    """
    missing = [key for key in required if not os.getenv(key)]
    
    if missing:
        print(f"❌ Missing required secrets: {', '.join(missing)}")
        print(f"   Please set them in app.secrets.yaml or as environment variables")
        return False
    
    return True
