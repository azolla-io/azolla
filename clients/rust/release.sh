#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Release script for Azolla Rust client libraries
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
if [ ! -f "azolla-client/Cargo.toml" ] || [ ! -f "azolla-macros/Cargo.toml" ]; then
  echo -e "${RED}Error: Must be run from clients/rust/ directory${NC}"
  exit 1
fi

# Check if versions in Cargo.toml match the specified version
MACROS_VERSION=$(grep '^version = ' azolla-macros/Cargo.toml | sed 's/version = "\(.*\)"/\1/')
CLIENT_VERSION=$(grep '^version = ' azolla-client/Cargo.toml | sed 's/version = "\(.*\)"/\1/')

if [ "$MACROS_VERSION" != "$VERSION" ]; then
  echo -e "${RED}Error: azolla-macros version ($MACROS_VERSION) doesn't match specified version ($VERSION)${NC}"
  echo "Please update azolla-macros/Cargo.toml first"
  exit 1
fi

if [ "$CLIENT_VERSION" != "$VERSION" ]; then
  echo -e "${RED}Error: azolla-client version ($CLIENT_VERSION) doesn't match specified version ($VERSION)${NC}"
  echo "Please update azolla-client/Cargo.toml first"
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

# Check if we're logged into crates.io
if ! cargo owner --list azolla-macros >/dev/null 2>&1; then
  echo -e "${YELLOW}Warning: You may not be logged into crates.io${NC}"
  echo "Run 'cargo login <your-api-token>' if needed"
  read -p "Continue? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

echo -e "${GREEN}✅ Pre-flight checks passed${NC}"

# Update proto files from main project
echo -e "${GREEN}📋 Updating proto files from main project...${NC}"
cp ../../proto/*.proto azolla-client/proto/
echo -e "${GREEN}✅ Proto files updated${NC}"

# Run tests first with path dependencies
echo -e "${GREEN}🧪 Running test suite...${NC}"
echo "Testing azolla-macros..."
(cd azolla-macros && cargo test)

echo "Testing azolla-client..."
(cd azolla-client && cargo test)

echo "Testing azolla-client with no default features..."
(cd azolla-client && cargo test --no-default-features)

echo "Testing azolla-client with macros feature..."
(cd azolla-client && cargo test --features macros)

echo -e "${GREEN}✅ All tests passed${NC}"

# Verify packaging
echo -e "${GREEN}📦 Verifying packaging...${NC}"
(cd azolla-macros && cargo package --list --allow-dirty > /dev/null)
(cd azolla-client && cargo package --list --allow-dirty > /dev/null)
echo -e "${GREEN}✅ Packaging verified${NC}"

# Publish azolla-macros first
echo -e "${GREEN}📤 Publishing azolla-macros v${VERSION}...${NC}"
(cd azolla-macros && cargo publish --allow-dirty)

echo -e "${GREEN}⏳ Waiting for crates.io propagation (2 minutes)...${NC}"
sleep 120

# Test that the published version is available
echo -e "${GREEN}🔍 Verifying azolla-macros is available on crates.io...${NC}"
timeout 30 bash -c "until cargo search azolla-macros | grep -q \"$VERSION\"; do sleep 5; done" || {
  echo -e "${YELLOW}Warning: azolla-macros v${VERSION} not yet visible on crates.io${NC}"
  echo "You may need to wait longer before publishing azolla-client"
}

# Update azolla-client to use published version
echo -e "${GREEN}📝 Updating azolla-client dependency to published version...${NC}"
perl -i -pe "s/azolla-macros = \\{ path = \"[^\"]*\", optional = true \\}/azolla-macros = { version = \"$VERSION\", optional = true }/" azolla-client/Cargo.toml
echo -e "${GREEN}✅ Dependency updated to published version${NC}"

# Publish azolla-client
echo -e "${GREEN}📤 Publishing azolla-client v${VERSION}...${NC}"
(cd azolla-client && cargo check)  # Final verification with published dependency
(cd azolla-client && cargo publish --allow-dirty)

echo -e "${GREEN}✅ Both crates published successfully!${NC}"

# Revert azolla-client Cargo.toml back to path dependency for development
echo -e "${GREEN}🔄 Reverting to development configuration...${NC}"
perl -i -pe "s/azolla-macros = \\{ version = \"[^\"]*\", optional = true \\}/azolla-macros = { path = \"..\\\/azolla-macros\", optional = true }/" azolla-client/Cargo.toml

# Verify development setup still works
(cd azolla-client && cargo check --features macros)

echo -e "${GREEN}✅ Development configuration restored${NC}"

# Create git tag
echo -e "${GREEN}🏷️  Creating git tag...${NC}"
git tag "v${VERSION}" -m "Release v${VERSION}"

echo -e "${GREEN}🎉 Release v${VERSION} completed successfully!${NC}"
echo
echo "Next steps:"
echo "1. Push tag: git push origin v${VERSION}"
echo "2. Visit https://crates.io/crates/azolla-client to verify publication"
echo "3. Consider creating a GitHub release"