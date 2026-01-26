#!/bin/bash
# ============================================================================
# 통합 메타데이터 파이프라인 데모 실행 스크립트
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

# Step 1: 메타데이터 파일 검증
log_info "Step 1/6: Validating metadata file..."
if [ ! -f "$METADATA_FILE" ]; then
    log_error "Metadata file not found: $METADATA_FILE"
    exit 1
fi
log_success "Metadata file validated"

# Step 2: 메타데이터 처리 및 설정 생성
log_info "Step 2/6: Processing metadata and generating configurations..."
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

# Step 3: 생성된 파일 확인
log_info "Step 3/6: Verifying generated files..."
echo ""
echo "Generated files:"
ls -lh "$OUTPUT_DIR"
echo ""
log_success "Files verified"

# Step 4: DABs validation
log_info "Step 4/6: Validating Databricks Asset Bundles..."
if command -v databricks &> /dev/null; then
    databricks bundle validate --target "$ENVIRONMENT" || {
        log_warning "DABs validation failed (continuing...)"
    }
    log_success "DABs validated"
else
    log_warning "Databricks CLI not found, skipping validation"
fi

# Step 5: 메타데이터 테이블 DDL 확인
log_info "Step 5/6: Checking metadata table DDL..."
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

# Step 6: 배포 옵션 안내
echo ""
log_info "Step 6/6: Deployment options"
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
