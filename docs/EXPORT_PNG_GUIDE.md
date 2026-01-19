# How to Generate Architecture PNG

This guide shows how to export the architecture diagram as PNG for embedding in README.

## Quick Steps

### Option 1: Online (Easiest)

1. **Open draw.io online:**
   - Go to https://app.diagrams.net
   - Click "Open Existing Diagram"
   - Navigate to and select `docs/architecture.drawio`

2. **Export as PNG:**
   - File → Export as → PNG...
   - Settings:
     - **Scale:** 200% (for high resolution)
     - **Zoom:** 100%
     - **Border Width:** 10
     - **Transparent Background:** ❌ (Unchecked)
     - **Selection Only:** ❌ (Unchecked)
     - **Shadow:** ✅ (Optional, looks nice)
     - **Grid:** ❌ (Unchecked)
   - Click "Export"
   - Save as `docs/architecture.png`

3. **Commit the PNG:**
   ```bash
   git add docs/architecture.png
   git commit -m "docs: Add architecture diagram PNG export"
   git push origin main
   ```

### Option 2: VS Code Extension

1. **Install Extension:**
   - Open VS Code
   - Install "Draw.io Integration" extension by Henning Dieterichs

2. **Open and Export:**
   - Open `docs/architecture.drawio` in VS Code
   - Right-click on the editor
   - Select "Export to PNG..."
   - Choose settings (same as above)
   - Save as `docs/architecture.png`

3. **Commit:**
   ```bash
   git add docs/architecture.png
   git commit -m "docs: Add architecture diagram PNG export"
   git push origin main
   ```

### Option 3: draw.io Desktop App

1. **Install desktop app:**
   - Download from: https://github.com/jgraph/drawio-desktop/releases
   - Install for your OS

2. **Open and Export:**
   - Open `docs/architecture.drawio`
   - File → Export as → PNG
   - Use same settings as Option 1
   - Save as `docs/architecture.png`

3. **Commit the PNG**

## After Exporting

The README will automatically display the PNG image since it's already configured to show:
```markdown
![SQLPilot Architecture](docs/architecture.png)
```

## Verify

After pushing, check your GitHub repo:
- The image should display in README.md
- The image should be clear and high resolution
- All text should be readable

## Troubleshooting

**Image too small/blurry?**
- Re-export with Scale: 300% or 400%

**Image too large (file size)?**
- Use Scale: 150% instead of 200%
- GitHub has a 100MB file limit (should be fine)

**Colors look washed out?**
- Make sure "Transparent Background" is unchecked
- Try checking "Shadow" option

**Text not readable?**
- Increase scale to 300%
- Use PNG instead of JPG (better for text)

## Current Status

✅ draw.io file created: `docs/architecture.drawio`
⏳ **PNG export needed:** `docs/architecture.png`

Once you export the PNG, the README will automatically show it!

