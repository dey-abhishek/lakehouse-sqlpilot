# SQLPilot Architecture Diagrams

This directory contains architecture documentation for Lakehouse SQLPilot.

## Files

### `architecture.drawio`
- **Editable diagram** in draw.io format
- Open with: https://app.diagrams.net or draw.io desktop app
- Contains the complete SQLPilot architecture with all layers

### How to Edit

1. **Online (Recommended):**
   - Go to https://app.diagrams.net
   - Click "Open Existing Diagram"
   - Select `architecture.drawio`
   - Edit and save

2. **Desktop:**
   - Install draw.io desktop: https://github.com/jgraph/drawio-desktop/releases
   - Open `architecture.drawio`
   - Edit and save

3. **VS Code:**
   - Install "Draw.io Integration" extension
   - Open `architecture.drawio` in VS Code
   - Edit inline

### Export Options

From draw.io, you can export to:
- **PNG** (for README embedding)
- **SVG** (for scalable vector graphics)
- **PDF** (for documentation)

**To export PNG for README:**
1. Open `architecture.drawio` in draw.io
2. File → Export as → PNG
3. Settings: 
   - Scale: 200% (for high resolution)
   - Border width: 10px
   - Transparent background: No
4. Save as `architecture.png` in this directory
5. Update README.md to reference: `![Architecture](docs/architecture.png)`

## Architecture Layers

The diagram shows 4 main layers:

1. **👥 User Layer** - Business Analysts, Data Engineers, Data Stewards, Platform Admins
2. **🎛️ SQLPilot Control Plane** - React UI, FastAPI Backend, AI Agents, Plan Registry
3. **⚡ Execution Plane** - Databricks SQL Warehouse, Execution Monitoring
4. **🔒 Governance & Data Layer** - Unity Catalog, Data Lineage, Audit Logs, Delta Lake

Plus a sidebar showing:
- 🔄 SQL Patterns (8 patterns)
- 🔐 OAuth 2.0 Authentication
- 🧪 Quality Assurance (416 tests)
- ✨ Key Features

## Color Scheme

- **Blue (#E3F2FD)** - User Layer
- **Purple (#F3E5F5)** - Control Plane
- **Orange (#FFF3E0)** - Execution Plane
- **Green (#E8F5E9)** - Governance Layer
- **Yellow (#FFF9C4)** - Patterns Sidebar

## Updates

When updating the architecture:
1. Edit `architecture.drawio`
2. Export new PNG (if needed for embedding)
3. Update README.md if architecture changes
4. Commit both files

