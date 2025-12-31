# Deployment Strategy: The "One-Container" Control Plane

To make installation "Easy" for organizations, we will package the entire application (Configuration Loader + Graph Explorer UI) into a single, stateless Docker container.

## Why this works for Enterprise
1.  **Security**: They run the container in *their* private cloud (AWS ECS, Google Cloud Run, Azure Container Apps). No external SaaS access required.
2.  **Simplicity**: No complex Python virtual environment setup. Just `docker run`.
3.  **Portability**: Works on a developer's laptop or a production Kubernetes cluster.

## Implementation Steps

### 1. Refactor Codebase (The "idr_core" Package)
We need to separate the *logic* from the *scripts*.
*   Move `tools/load_metadata.py` logic into `idr_core/`
*   Create `idr_core/adapters/` (Shared DB connections)
*   Create `idr_core/metadata_loader.py` (The logic currently in the script)

### 2. Create the UI (Streamlit)
A lightweight web interface that imports `idr_core`.
*   **Page 1**: `Connection` (Input credentials safely)
*   **Page 2**: `Deploy Config` (Upload YAML -> Run Loader)
*   **Page 3**: `Graph Explorer` (Visualize IDR results)

### 3. Dockerize
Create a `Dockerfile` that bundles:
*   Python 3.10
*   All Drivers: `duckdb`, `snowflake-connector-python`, `google-cloud-bigquery`, `databricks-sql-connector`
*   Streamlit

### 4. Zero-Touch Config
Support Environment Variables for everything.
*   Organization installs by simply setting `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc., in their container orchestrator.

## The End-User Experience
**"Hey Data Team, to install the IDR Control Plane, just deploy this Docker image to your internal registry."**

```bash
docker run -p 8501:8501 \
  -e PLATFORM=snowflake \
  -e SNOWFLAKE_ACCOUNT=... \
  my-org/idr-control-plane:latest
```

They open `localhost:8501` and see a clean UI to Manage & Explore their identity graph.
