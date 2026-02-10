#!/bin/bash

# ============================================================================
# VolGuard v3.2 - Quick Start Script
# ============================================================================
# This script helps you set up and deploy VolGuard quickly
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "${BLUE}======================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}======================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        echo "Visit: https://docs.docker.com/get-docker/"
        exit 1
    fi
    print_success "Docker is installed: $(docker --version)"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed."
        exit 1
    fi
    print_success "Docker Compose is installed: $(docker-compose --version)"
    
    # Check if files exist
    if [ ! -f "Dockerfile" ]; then
        print_error "Dockerfile not found in current directory"
        exit 1
    fi
    print_success "Dockerfile found"
    
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found"
        exit 1
    fi
    print_success "docker-compose.yml found"
    
    if [ ! -f "requirements.txt" ]; then
        print_error "requirements.txt not found"
        exit 1
    fi
    print_success "requirements.txt found"
    
    echo ""
}

# Setup environment
setup_environment() {
    print_header "Setting Up Environment"
    
    if [ -f ".env" ]; then
        print_warning ".env file already exists"
        read -p "Do you want to overwrite it? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Keeping existing .env file"
            return
        fi
    fi
    
    if [ ! -f ".env.example" ]; then
        print_error ".env.example not found"
        exit 1
    fi
    
    cp .env.example .env
    print_success "Created .env file from template"
    
    echo ""
    print_info "Please edit .env file with your credentials:"
    print_info "  - UPSTOX_ACCESS_TOKEN (required)"
    print_info "  - TELEGRAM_TOKEN (optional)"
    print_info "  - TELEGRAM_CHAT_ID (optional)"
    print_info "  - BASE_CAPITAL (default: 1000000)"
    echo ""
    
    read -p "Press Enter to open .env in nano editor (or Ctrl+C to edit manually)..."
    nano .env || vi .env || echo "Please edit .env manually"
    
    echo ""
}

# Rename application file
setup_app_file() {
    print_header "Setting Up Application File"
    
    if [ -f "volguard.py" ]; then
        print_success "volguard.py already exists"
        return
    fi
    
    if [ -f "volguard_integrated_full.py" ]; then
        mv volguard_integrated_full.py volguard.py
        print_success "Renamed volguard_integrated_full.py to volguard.py"
    else
        print_warning "volguard.py not found. Please ensure the main Python file is named volguard.py"
    fi
    
    echo ""
}

# Build container
build_container() {
    print_header "Building Docker Container"
    
    print_info "This may take a few minutes..."
    if docker-compose build; then
        print_success "Container built successfully"
    else
        print_error "Container build failed"
        exit 1
    fi
    
    echo ""
}

# Start services
start_services() {
    print_header "Starting VolGuard Services"
    
    if docker-compose up -d; then
        print_success "Services started successfully"
    else
        print_error "Failed to start services"
        exit 1
    fi
    
    echo ""
    print_info "Waiting for services to be ready..."
    sleep 5
    
    # Check if container is running
    if docker-compose ps | grep -q "Up"; then
        print_success "Container is running"
    else
        print_error "Container failed to start. Check logs: docker-compose logs"
        exit 1
    fi
    
    echo ""
}

# Verify deployment
verify_deployment() {
    print_header "Verifying Deployment"
    
    # Test health endpoint
    print_info "Testing health endpoint..."
    if curl -f -s http://localhost:8000/api/health > /dev/null 2>&1; then
        print_success "Health check passed"
    else
        print_warning "Health check failed. Service might still be starting..."
    fi
    
    echo ""
    print_info "Checking container logs..."
    docker-compose logs --tail=20 volguard
    
    echo ""
}

# Show access information
show_info() {
    print_header "VolGuard v3.2 - Deployment Complete!"
    
    echo ""
    print_success "Your VolGuard system is now running!"
    echo ""
    
    echo "📊 Access Points:"
    echo "   • API Documentation: http://localhost:8000/docs"
    echo "   • Dashboard: http://localhost:8000/api/dashboard"
    echo "   • Health Check: http://localhost:8000/api/health"
    echo ""
    
    echo "🔧 Management Commands:"
    echo "   • View logs:     docker-compose logs -f"
    echo "   • Stop system:   docker-compose stop"
    echo "   • Start system:  docker-compose start"
    echo "   • Restart:       docker-compose restart"
    echo "   • Status:        docker-compose ps"
    echo ""
    
    echo "🧪 Testing:"
    echo "   • Dashboard:     curl http://localhost:8000/api/dashboard"
    echo "   • Test Alert:    curl -X POST http://localhost:8000/api/test-alert"
    echo ""
    
    echo "📖 Documentation:"
    echo "   • See DEPLOYMENT.md for detailed instructions"
    echo ""
    
    print_warning "IMPORTANT: Review your .env settings before enabling auto-trading!"
    print_warning "Current mode: $(grep ENABLE_AUTO_TRADING .env || echo 'Not set')"
    
    echo ""
}

# Cleanup function
cleanup() {
    print_header "Cleaning Up (if needed)"
    
    read -p "Do you want to remove old containers and images? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose down
        docker system prune -f
        print_success "Cleanup complete"
    else
        print_info "Skipping cleanup"
    fi
    
    echo ""
}

# Main menu
main_menu() {
    print_header "VolGuard v3.2 - Quick Start"
    
    echo "Choose an option:"
    echo "  1) Full Setup (First time deployment)"
    echo "  2) Just Build and Start"
    echo "  3) Start Existing Container"
    echo "  4) Stop Container"
    echo "  5) View Logs"
    echo "  6) Restart Container"
    echo "  7) Cleanup and Reset"
    echo "  0) Exit"
    echo ""
    
    read -p "Enter choice [0-7]: " choice
    
    case $choice in
        1)
            check_prerequisites
            setup_environment
            setup_app_file
            build_container
            start_services
            verify_deployment
            show_info
            ;;
        2)
            check_prerequisites
            build_container
            start_services
            verify_deployment
            show_info
            ;;
        3)
            docker-compose start
            print_success "Container started"
            docker-compose ps
            ;;
        4)
            docker-compose stop
            print_success "Container stopped"
            ;;
        5)
            docker-compose logs -f
            ;;
        6)
            docker-compose restart
            print_success "Container restarted"
            ;;
        7)
            cleanup
            ;;
        0)
            print_info "Goodbye!"
            exit 0
            ;;
        *)
            print_error "Invalid option"
            exit 1
            ;;
    esac
}

# Run main menu
main_menu
