#!/bin/bash
# setup_merton_integration.sh
# Automated setup script for Merton model integration

echo "=========================================="
echo "Merton Model Integration Setup"
echo "=========================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Check if we're in the project root
if [ ! -d "src" ]; then
    print_error "Error: Must be run from project root directory"
    exit 1
fi

print_info "Creating directory structure..."

# Create directories
mkdir -p src/merton
mkdir -p src/utils
mkdir -p logs
mkdir -p migrations

print_success "Directories created"

# Create __init__.py files
print_info "Creating __init__.py files..."

touch src/__init__.py
touch src/merton/__init__.py
touch src/utils/__init__.py

print_success "__init__.py files created"

# Check for required Python packages
print_info "Checking Python dependencies..."

python3 -c "import scipy" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "scipy installed"
else
    print_error "scipy not found. Install with: pip install scipy"
fi

python3 -c "import numpy" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "numpy installed"
else
    print_error "numpy not found. Install with: pip install numpy"
fi

python3 -c "import pandas" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "pandas installed"
else
    print_error "pandas not found. Install with: pip install pandas"
fi

python3 -c "import sqlalchemy" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "sqlalchemy installed"
else
    print_error "sqlalchemy not found. Install with: pip install sqlalchemy"
fi

python3 -c "import yaml" 2>/dev/null
if [ $? -eq 0 ]; then
    print_success "pyyaml installed"
else
    print_error "pyyaml not found. Install with: pip install pyyaml"
fi

# Check database connection
print_info "Checking database connection..."

psql -U merton_user -d merton_db -c "SELECT 1" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    print_success "Database connection successful"
else
    print_error "Cannot connect to database. Check credentials in src/db/engine.py"
fi

# Run database migration
print_info "Running database migration..."

if [ -f "migrations/001_add_merton_outputs.sql" ]; then
    psql -U merton_user -d merton_db -f migrations/001_add_merton_outputs.sql > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        print_success "Database migration completed"
    else
        print_error "Migration failed. Run manually: psql -U merton_user -d merton_db -f migrations/001_add_merton_outputs.sql"
    fi
else
    print_error "Migration file not found: migrations/001_add_merton_outputs.sql"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Copy the Merton module files to src/merton/"
echo "2. Copy the utility files to src/utils/"
echo "3. Run tests: python scripts/test_merton_integration.py"
echo "4. Run for one ticker: python scripts/run_merton_model.py AAPL"
echo ""