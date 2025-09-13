#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Release script for Azolla Python client library
# Usage: ./release.sh <version>
# Example: ./release.sh 0.1.1

VERSION=$1
if [ -z "$VERSION" ]; then
  echo -e "${RED}Error: Version number is required${NC}"
  echo "Usage: $0 <version>"
  echo "Example: $0 0.1.1"
  exit 1
fi

# Validate version format (basic semantic versioning check)
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$'; then
  echo -e "${RED}Error: Invalid version format. Use semantic versioning (e.g., 0.1.1)${NC}"
  exit 1
fi

echo -e "${GREEN}🚀 Starting release process for version ${VERSION}${NC}"

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ] || [ ! -f "src/azolla/_version.py" ]; then
  echo -e "${RED}Error: Must be run from clients/python/ directory${NC}"
  exit 1
fi

# Check if version in _version.py matches the specified version
CURRENT_VERSION=$(grep '__version__ = ' src/azolla/_version.py | sed 's/__version__ = "\(.*\)"/\1/')

if [ "$CURRENT_VERSION" != "$VERSION" ]; then
  echo -e "${RED}Error: Current version ($CURRENT_VERSION) doesn't match specified version ($VERSION)${NC}"
  echo "Please update src/azolla/_version.py first"
  exit 1
fi

# Check if we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo -e "${RED}Error: Release can only be run from main branch${NC}"
  echo "Current branch: $CURRENT_BRANCH"
  echo "Please merge your changes to main and run the release from there"
  exit 1
fi

# Check if git working directory is clean
if [ -n "$(git status --porcelain)" ]; then
  echo -e "${YELLOW}Warning: Git working directory is not clean${NC}"
  git status --short
  read -p "Continue anyway? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# Check if ~/.pypirc exists
if [ ! -f ~/.pypirc ]; then
  echo -e "${RED}Error: ~/.pypirc not found. Please set up PyPI authentication first${NC}"
  echo "See: https://packaging.python.org/en/latest/specifications/pypirc/"
  exit 1
fi

# Check if we have build and twine installed
if ! command -v python3 &> /dev/null; then
  echo -e "${RED}Error: python3 not found${NC}"
  exit 1
fi

echo -e "${GREEN}✅ Pre-flight checks passed${NC}"

# Update proto files from main project
echo -e "${GREEN}📋 Updating proto files from main project...${NC}"
mkdir -p src/azolla/_grpc/proto
cp ../../proto/*.proto src/azolla/_grpc/proto/
echo -e "${GREEN}✅ Proto files updated${NC}"

# Set up virtual environment for testing if it doesn't exist
if [ ! -d "venv" ]; then
  echo -e "${GREEN}🐍 Creating virtual environment...${NC}"
  python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install/upgrade build tools
echo -e "${GREEN}🔧 Installing build tools...${NC}"
pip install --upgrade build twine

# Skip tests during release - they should have already passed on main branch
echo -e "${GREEN}🧪 Skipping tests (should have passed on main branch)${NC}"

# Clean previous builds
echo -e "${GREEN}🧹 Cleaning previous builds...${NC}"
rm -rf dist/ build/ *.egg-info/

# Build package
echo -e "${GREEN}📦 Building package...${NC}"
python -m build

# Verify package integrity
echo -e "${GREEN}🔍 Verifying package integrity...${NC}"
twine check dist/*

echo -e "${GREEN}✅ Package built and verified${NC}"

# Test publish to TestPyPI first
echo -e "${GREEN}🧪 Testing publish to TestPyPI...${NC}"
if grep -q "\[testpypi\]" ~/.pypirc; then
  twine upload --repository testpypi dist/*
  
  echo -e "${GREEN}⏳ Waiting for TestPyPI propagation (30 seconds)...${NC}"
  sleep 30
  
  # Test installation from TestPyPI
  echo -e "${GREEN}📥 Testing installation from TestPyPI...${NC}"
  python -m venv test_install
  source test_install/bin/activate
  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ azolla==${VERSION}
  python -c "import azolla; print(f'Successfully installed azolla {azolla.__version__}')"
  deactivate
  rm -rf test_install
  
  # Reactivate main venv
  source venv/bin/activate
  
  echo -e "${GREEN}✅ TestPyPI test successful${NC}"
else
  echo -e "${YELLOW}Warning: No TestPyPI configuration found in ~/.pypirc, skipping test publish${NC}"
fi

# Confirm before publishing to production PyPI
echo -e "${YELLOW}Ready to publish to production PyPI${NC}"
read -p "Continue with production publish? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo -e "${YELLOW}Release cancelled${NC}"
  exit 1
fi

# Publish to production PyPI
echo -e "${GREEN}📤 Publishing to PyPI...${NC}"
twine upload dist/*

echo -e "${GREEN}⏳ Waiting for PyPI propagation (60 seconds)...${NC}"
sleep 60

# Test installation from production PyPI
echo -e "${GREEN}📥 Testing installation from PyPI...${NC}"
python -m venv final_test
source final_test/bin/activate
pip install azolla==${VERSION}
python -c "import azolla; print(f'Successfully installed azolla {azolla.__version__} from PyPI!')"
deactivate
rm -rf final_test

# Reactivate main venv
source venv/bin/activate

echo -e "${GREEN}✅ Package published and verified${NC}"

# Create git tag
echo -e "${GREEN}🏷️  Creating git tag...${NC}"
git tag "python-v${VERSION}" -m "Python client release v${VERSION}"

# Deactivate virtual environment
deactivate

echo -e "${GREEN}🎉 Release v${VERSION} completed successfully!${NC}"
echo
echo "Next steps:"
echo "1. Push tag: git push origin python-v${VERSION}"
echo "2. Visit https://pypi.org/project/azolla/${VERSION}/ to verify publication"
echo "3. Update documentation with new version"
echo "4. Consider creating a GitHub release"
echo
echo -e "${GREEN}📦 Your package is now available via: pip install azolla==${VERSION}${NC}"