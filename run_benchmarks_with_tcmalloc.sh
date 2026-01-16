#!/bin/bash
# Run hybrid interval benchmarks with tcmalloc
# Compares performance between standard malloc and tcmalloc

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== FastEC Hybrid Interval Benchmarks - tcmalloc Comparison ===${NC}"
echo "Date: $(date)"
echo ""

# Find tcmalloc library
TCMALLOC_LIB=""
for lib in /usr/lib/x86_64-linux-gnu/libtcmalloc.so.4 \
           /usr/lib/x86_64-linux-gnu/libtcmalloc.so \
           /usr/lib64/libtcmalloc.so.4 \
           /usr/lib64/libtcmalloc.so \
           /usr/lib/libtcmalloc.so.4 \
           /usr/lib/libtcmalloc.so \
           /usr/local/lib/libtcmalloc.so.4 \
           /usr/local/lib/libtcmalloc.so; do
    if [ -f "$lib" ]; then
        TCMALLOC_LIB="$lib"
        break
    fi
done

if [ -z "$TCMALLOC_LIB" ]; then
    echo -e "${RED}ERROR: tcmalloc library not found!${NC}"
    echo "Please install gperftools:"
    echo "  Ubuntu/Debian: sudo apt-get install libgoogle-perftools-dev"
    echo "  RHEL/CentOS: sudo yum install gperftools-devel"
    echo "  macOS: brew install gperftools"
    exit 1
fi

echo -e "${GREEN}Found tcmalloc: ${TCMALLOC_LIB}${NC}"
echo ""

# Configuration
BUILD_DIR="${BUILD_DIR:-build}"
ITERATIONS="${ITERATIONS:-100000}"
OUTPUT_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"

# Check if build directory exists
if [ ! -d "$BUILD_DIR" ]; then
    echo -e "${RED}ERROR: Build directory '$BUILD_DIR' not found${NC}"
    echo "Please build the project first or set BUILD_DIR environment variable"
    exit 1
fi

# Find benchmark executables
BENCHMARKS=(
    "benchmark_hybrid_interval_set"
    "benchmark_hybrid_interval_map"
    "benchmark_hybrid_striping"
    "benchmark_hybrid_transition"
)

# Check if benchmarks exist
MISSING=0
for bench in "${BENCHMARKS[@]}"; do
    if [ ! -f "$BUILD_DIR/bin/$bench" ]; then
        echo -e "${YELLOW}Warning: $bench not found in $BUILD_DIR/bin/${NC}"
        MISSING=$((MISSING + 1))
    fi
done

if [ $MISSING -eq ${#BENCHMARKS[@]} ]; then
    echo -e "${RED}ERROR: No benchmark executables found in $BUILD_DIR/bin/${NC}"
    echo "Please build the benchmarks first:"
    echo "  cd $BUILD_DIR"
    echo "  make benchmark_hybrid_interval_set benchmark_hybrid_interval_map"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Configuration:"
echo "  Build directory: $BUILD_DIR"
echo "  Iterations: $ITERATIONS"
echo "  Output directory: $OUTPUT_DIR"
echo "  tcmalloc library: $TCMALLOC_LIB"
echo ""

# Function to run a benchmark
run_benchmark() {
    local bench_name=$1
    local bench_path="$BUILD_DIR/bin/$bench_name"
    
    if [ ! -f "$bench_path" ]; then
        echo -e "${YELLOW}Skipping $bench_name (not found)${NC}"
        return
    fi
    
    echo -e "${GREEN}Running $bench_name...${NC}"
    
    # Run with standard malloc
    echo "  With standard malloc..."
    "$bench_path" $ITERATIONS > "$OUTPUT_DIR/${bench_name}_malloc.txt" 2>&1
    
    # Run with tcmalloc
    echo "  With tcmalloc..."
    LD_PRELOAD="$TCMALLOC_LIB" "$bench_path" $ITERATIONS > "$OUTPUT_DIR/${bench_name}_tcmalloc.txt" 2>&1
    
    echo "  Results saved to $OUTPUT_DIR/"
    echo ""
}

# Run all benchmarks
for bench in "${BENCHMARKS[@]}"; do
    run_benchmark "$bench"
done

# Generate comparison report
echo -e "${GREEN}Generating comparison report...${NC}"

cat > "$OUTPUT_DIR/comparison_report.txt" <<EOF
FastEC Hybrid Interval Benchmarks - malloc vs tcmalloc Comparison
==================================================================
Date: $(date)
Branch: $(git rev-parse --abbrev-ref HEAD)
Commit: $(git rev-parse --short HEAD)
Iterations: $ITERATIONS
tcmalloc: $TCMALLOC_LIB

Results Summary:
----------------

EOF

# Extract key metrics from each benchmark
for bench in "${BENCHMARKS[@]}"; do
    malloc_file="$OUTPUT_DIR/${bench}_malloc.txt"
    tcmalloc_file="$OUTPUT_DIR/${bench}_tcmalloc.txt"
    
    if [ -f "$malloc_file" ] && [ -f "$tcmalloc_file" ]; then
        echo "$bench:" >> "$OUTPUT_DIR/comparison_report.txt"
        echo "----------------------------------------" >> "$OUTPUT_DIR/comparison_report.txt"
        
        # Extract timing information (this is a simple approach, adjust based on actual output format)
        echo "Standard malloc:" >> "$OUTPUT_DIR/comparison_report.txt"
        grep -E "Time|ms|seconds" "$malloc_file" | head -10 >> "$OUTPUT_DIR/comparison_report.txt" || echo "  (see ${bench}_malloc.txt)" >> "$OUTPUT_DIR/comparison_report.txt"
        
        echo "" >> "$OUTPUT_DIR/comparison_report.txt"
        echo "With tcmalloc:" >> "$OUTPUT_DIR/comparison_report.txt"
        grep -E "Time|ms|seconds" "$tcmalloc_file" | head -10 >> "$OUTPUT_DIR/comparison_report.txt" || echo "  (see ${bench}_tcmalloc.txt)" >> "$OUTPUT_DIR/comparison_report.txt"
        
        echo "" >> "$OUTPUT_DIR/comparison_report.txt"
        echo "" >> "$OUTPUT_DIR/comparison_report.txt"
    fi
done

cat >> "$OUTPUT_DIR/comparison_report.txt" <<EOF

Files Generated:
----------------
EOF

ls -1 "$OUTPUT_DIR" >> "$OUTPUT_DIR/comparison_report.txt"

echo -e "${GREEN}=== Benchmark Complete ===${NC}"
echo ""
echo "Results saved to: $OUTPUT_DIR/"
echo ""
echo "Summary:"
cat "$OUTPUT_DIR/comparison_report.txt"
echo ""
echo -e "${GREEN}To view detailed results:${NC}"
echo "  cat $OUTPUT_DIR/comparison_report.txt"
echo "  diff $OUTPUT_DIR/benchmark_hybrid_interval_set_malloc.txt $OUTPUT_DIR/benchmark_hybrid_interval_set_tcmalloc.txt"

# Made with Bob
