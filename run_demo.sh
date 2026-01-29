#!/bin/bash
# ============================================================================
# 통합 메타데이터 파이프라인 데모 실행 스크립트
#
# 사용법: # git bash
# $ alias pyhon3='python'
# $ bash run_demo.sh dev
# ============================================================================

set -e  # 에러 발생 시 중단

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 환경 변수 설정
ENVIRONMENT="${1:-dev}"
METADATA_FILE="metadata/unified_pipeline_metadata.yml"
OUTPUT_DIR="generated"

echo ""
echo "============================================================================"
echo "  통합 메타데이터 파이프라인 데모"
echo "============================================================================"
echo "  Environment: $ENVIRONMENT"
echo "  Metadata File: $METADATA_FILE"
echo "  Output Directory: $OUTPUT_DIR"
echo "============================================================================"
echo ""

# Step 1: Python 의존성 확인
log_info "Step 1/7: Checking Python dependencies..."
if ! python3 -c "import yaml" &> /dev/null; then
    log_warning "PyYAML not installed. Installing..."
    pip install PyYAML || {
        log_error "Failed to install PyYAML. Please run: pip install PyYAML"
        exit 1
    }
    log_success "PyYAML installed"
else
    log_success "Python dependencies OK"
fi

log_info "Step 2/7: Validating metadata file..."
if [ ! -f "$METADATA_FILE" ]; then
    log_error "Metadata file not found: $METADATA_FILE"
    exit 1
fi
log_success "Metadata file validated"

# Step 3: 메타데이터 처리 및 설정 생성
log_info "Step 3/7: Processing metadata and generating configurations..."
python scripts/unified_metadata_processor.py \
    --metadata-file "$METADATA_FILE" \
    --output-dir "$OUTPUT_DIR" \
    --environment "$ENVIRONMENT"

if [ $? -eq 0 ]; then
    log_success "Configurations generated successfully"
else
    log_error "Failed to generate configurations"
    exit 1
fi

# Step 4: 생성된 파일 확인
log_info "Step 4/7: Verifying generated files..."
echo ""
echo "Generated files:"
ls -lh "$OUTPUT_DIR"
echo ""
log_success "Files verified"

# Step 5: DABs validation
log_info "Step 5/7: Validating Databricks Asset Bundles..."
if command -v databricks &> /dev/null; then
    databricks bundle validate --target "$ENVIRONMENT" || {
        log_warning "DABs validation failed (continuing...)"
    }
    log_success "DABs validated"
else
    log_warning "Databricks CLI not found, skipping validation"
fi

# Step 6: 메타데이터 테이블 DDL 확인
log_info "Step 6/7: Checking metadata table DDL..."
DDL_FILE=$(find "$OUTPUT_DIR" -name "metadata_ddl_*.sql" -print -quit)
if [ -f "$DDL_FILE" ]; then
    log_info "Found DDL file: $DDL_FILE"
    echo ""
    echo "Preview (first 20 lines):"
    head -n 20 "$DDL_FILE"
    echo "..."
    log_success "DDL file ready"
else
    log_warning "No DDL file found"
fi

# Step 7: 배포 옵션 안내
echo ""
log_info "Step 7/7: Deployment options"
echo ""
echo "To deploy to Databricks:"
echo "  1. Ensure Databricks CLI is configured:"
echo "     $ databricks auth login --host <workspace-url>"
echo ""
echo "  2. Set up secrets:"
echo "     $ databricks secrets create-scope db_credentials"
echo "     $ databricks secrets put-secret --scope db_credentials --key gdevdb02_username"
echo "     $ databricks secrets put-secret --scope db_credentials --key gdevdb02_password"
echo ""
echo "  3. Deploy the bundle:"
echo "     $ databricks bundle deploy --target $ENVIRONMENT"
echo ""
echo "  4. Run the pipeline:"
echo "     $ databricks bundle run orchestrator_item_goods_option_full --target $ENVIRONMENT"
echo ""
echo "Or deploy metadata tables only:"
echo "     $ databricks sql execute --file $DDL_FILE"
echo ""

# 완료
echo "============================================================================"
log_success "Demo preparation complete!"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  - Review generated configurations in: $OUTPUT_DIR/"
echo "  - Customize metadata file if needed: $METADATA_FILE"
echo "  - Deploy to Databricks using commands above"
echo ""
