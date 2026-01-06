#!/bin/bash
# Build script for printer tunnel executable

echo "Building printer tunnel executable..."

# Install dependencies
npm install

# Create dist directory
mkdir -p dist

# Build the executable
npm run build

echo "Build completed! Executables are in the dist/ folder"
echo "To run the Windows executable: ./dist/printer-tunnel.exe (on Windows)"
echo "To run the Linux executable: ./dist/printer-tunnel (on Linux)"
echo "To run the macOS executable: ./dist/printer-tunnel (on macOS)"