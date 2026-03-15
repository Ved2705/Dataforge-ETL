#!/bin/bash
set -e

echo "Creating tables..."
python -c "from backend.app import app, db; 
app.app_context().push()
db.create_all()
print('Done!')"

echo "Starting Gunicorn..."
exec gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 2 --timeout 120 backend.app:app