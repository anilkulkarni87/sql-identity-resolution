FROM python:3.11-slim

LABEL org.opencontainers.image.title="SQL Identity Resolution"
LABEL org.opencontainers.image.description="Production-grade deterministic identity resolution"
LABEL org.opencontainers.image.source="https://github.com/anilkulkarni87/sql-identity-resolution"

# Install dependencies
RUN pip install --no-cache-dir duckdb pyarrow

# Copy source code
WORKDIR /app
COPY sql/ sql/
COPY tools/ tools/
COPY examples/ examples/

# Create output directory
RUN mkdir -p /output

# Default command: run demo
CMD ["python", "-c", "\
import duckdb\n\
import subprocess\n\
import sys\n\
print('🔗 SQL Identity Resolution - Docker Demo')\n\
print('='*50)\n\
print()\n\
print('Creating database...')\n\
conn = duckdb.connect('/output/demo.duckdb')\n\
with open('sql/duckdb/00_ddl_all.sql') as f:\n\
    conn.execute(f.read())\n\
print('  ✓ Schema created')\n\
print()\n\
print('Generating sample data...')\n\
subprocess.run([sys.executable, 'examples/sample_data/generate_demo_data.py', '--db=/output/demo.duckdb', '--rows=10000'])\n\
print()\n\
print('Running identity resolution...')\n\
subprocess.run([sys.executable, 'sql/duckdb/idr_run.py', '--db=/output/demo.duckdb', '--run-mode=FULL'])\n\
print()\n\
print('Generating dashboard...')\n\
subprocess.run([sys.executable, 'tools/dashboard/generator.py', '--platform=duckdb', '--connection=/output/demo.duckdb', '--output=/output/dashboard.html'])\n\
print()\n\
print('='*50)\n\
print('✅ Demo complete!')\n\
print()\n\
print('Output files in /output:')\n\
print('  • demo.duckdb - Database with results')\n\
print('  • dashboard.html - Visual dashboard')\n\
"]
