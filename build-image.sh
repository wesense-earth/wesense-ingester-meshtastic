#!/bin/bash
# Build script for WeSense Data Ingester Docker image

set -e

# Default values
IMAGE_NAME="wesense-ingester-meshtastic"
IMAGE_TAG="latest"
SAVE_TAR=false
REGISTRY=""
PLATFORM=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Build Docker image for WeSense Data Ingester"
    echo ""
    echo "Options:"
    echo "  -n, --name NAME       Image name (default: wesense-ingester-meshtastic)"
    echo "  -t, --tag TAG         Image tag (default: latest)"
    echo "  -s, --save            Save image as tar file"
    echo "  -r, --registry URL    Registry URL (e.g., username/repo or registry.com:5000/repo)"
    echo "  -p, --push            Push to registry after build"
    echo "  --platform PLATFORM   Target platform (e.g., linux/amd64, linux/arm64)"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Build image locally"
    echo "  $0 --save                             # Build and save as tar file"
    echo "  $0 --platform linux/amd64 --save      # Build for Intel/AMD x86_64"
    echo "  $0 --registry myuser/myrepo --push    # Build and push to Docker Hub"
    echo "  $0 --registry registry.local:5000/app # Build with custom registry"
    exit 1
}

# Parse arguments
PUSH=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        -t|--tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        -s|--save)
            SAVE_TAR=true
            shift
            ;;
        -r|--registry)
            REGISTRY="$2"
            shift 2
            ;;
        -p|--push)
            PUSH=true
            shift
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            usage
            ;;
    esac
done

# Determine full image name
if [ -n "$REGISTRY" ]; then
    FULL_IMAGE_NAME="${REGISTRY}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo -e "${GREEN}Building Docker image: ${FULL_IMAGE_NAME}${NC}"
if [ -n "$PLATFORM" ]; then
    echo -e "${YELLOW}Target platform: ${PLATFORM}${NC}"
fi
echo ""

# Build the image
if [ -n "$PLATFORM" ]; then
    docker build --platform "${PLATFORM}" -t "${FULL_IMAGE_NAME}" .
else
    docker build -t "${FULL_IMAGE_NAME}" .
fi

echo ""
echo -e "${GREEN}✓ Build complete!${NC}"
echo ""

# Save as tar if requested
if [ "$SAVE_TAR" = true ]; then
    TAR_FILE="${IMAGE_NAME}-${IMAGE_TAG}.tar"
    echo -e "${YELLOW}Saving image to ${TAR_FILE}...${NC}"
    docker save "${FULL_IMAGE_NAME}" -o "${TAR_FILE}"
    echo -e "${GREEN}✓ Image saved to ${TAR_FILE}${NC}"

    # Show file size
    if command -v du &> /dev/null; then
        SIZE=$(du -h "${TAR_FILE}" | cut -f1)
        echo -e "${GREEN}  File size: ${SIZE}${NC}"
    fi
    echo ""
    echo -e "${YELLOW}To transfer to your Docker host:${NC}"
    echo "  scp ${TAR_FILE} user@host:/path/to/destination/"
    echo ""
    echo -e "${YELLOW}On the Docker host, load with:${NC}"
    echo "  docker load -i ${TAR_FILE}"
    echo ""
fi

# Push if requested
if [ "$PUSH" = true ]; then
    if [ -z "$REGISTRY" ]; then
        echo -e "${RED}Error: --push requires --registry to be set${NC}"
        exit 1
    fi

    echo -e "${YELLOW}Pushing image to registry...${NC}"
    docker push "${FULL_IMAGE_NAME}"
    echo -e "${GREEN}✓ Image pushed successfully!${NC}"
    echo ""
fi

# Show summary
echo -e "${GREEN}Summary:${NC}"
echo "  Image name: ${FULL_IMAGE_NAME}"
docker images "${FULL_IMAGE_NAME}"
echo ""
echo -e "${GREEN}Next steps:${NC}"
if [ "$SAVE_TAR" = true ]; then
    echo "  1. Transfer ${TAR_FILE} to your Docker host"
    echo "  2. Run: docker load -i ${TAR_FILE}"
elif [ "$PUSH" = true ]; then
    echo "  On your Docker host, run: docker pull ${FULL_IMAGE_NAME}"
else
    echo "  Run: docker run -d --name wesense-ingester-meshtastic \\"
    echo "         -v \$(pwd)/cache:/app/cache \\"
    echo "         -v \$(pwd)/config:/app/config:ro \\"
    echo "         -v \$(pwd)/logs:/app/logs \\"
    echo "         -e INFLUXDB_URL=http://your-influx:8086 \\"
    echo "         ${FULL_IMAGE_NAME}"
fi
echo ""
