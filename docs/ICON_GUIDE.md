# Using Databricks Icons in Architecture Diagram

## 📦 **Available Icons**

I've copied the official Databricks icons to `docs/icons/`:

| Icon File | Use For | Component |
|-----------|---------|-----------|
| `unity-catalog.png` | Unity Catalog | Governance & Data Layer |
| `sql-warehouse.png` | SQL Warehouse | Execution Plane |
| `delta-lake.png` | Delta Lake | Governance & Data Layer |
| `governance.png` | General governance | Governance features |
| `data-lineage.png` | Data Lineage | Governance features |
| `workflow.png` | Execution/Workflow | Execution monitoring |

---

## 🎨 **How to Add Icons to Draw.io**

### **Method 1: Image Shape (Recommended)**

1. **Open diagram:**
   - Go to https://app.diagrams.net
   - Open `docs/architecture.drawio`

2. **Add icon as image:**
   - Click on a shape (e.g., Unity Catalog box)
   - From left sidebar: **"General" shapes**
   - Drag **"Image"** shape onto canvas
   - Browse to `/Users/abhishek.dey/lakehouse-sqlpilot/docs/icons/unity-catalog.png`
   - Position icon inside or next to the component box

3. **Repeat for each component:**
   - Unity Catalog → `unity-catalog.png`
   - SQL Warehouse → `sql-warehouse.png`
   - Delta Lake → `delta-lake.png`
   - Data Lineage → `data-lineage.png`
   - Governance boxes → `governance.png`

---

### **Method 2: Embed Icon in Box**

1. **Select component box** (e.g., Unity Catalog)

2. **Edit Style:**
   - Right-click → **Edit Style** (or press Ctrl/Cmd + E)

3. **Add image property:**
   ```
   image=data:image/png;base64,[base64-encoded-image]
   ```

   **Or use file path:**
   ```
   image=/docs/icons/unity-catalog.png
   ```

4. **Adjust positioning:**
   ```
   imageAlign=left;imageBorder=default;imageAspect=1
   ```

---

### **Method 3: Simple Drag & Drop**

1. Open Finder/Explorer
2. Navigate to: `/Users/abhishek.dey/lakehouse-sqlpilot/docs/icons/`
3. Drag PNG files directly onto the draw.io canvas
4. Resize and position as needed

---

## 🎯 **Suggested Icon Placement**

### **Governance & Data Layer (Green Section):**
```
┌─────────────────────────────────────────┐
│ [icon] 🏛️ Unity Catalog                 │
│        unity-catalog.png                │
└─────────────────────────────────────────┘

┌─────────────┬─────────────┬─────────────┐
│ [icon]      │ [icon]      │ [icon]      │
│ Data        │ Audit Logs  │ Access      │
│ Lineage     │             │ Control     │
│ (lineage)   │ (governance)│ (governance)│
└─────────────┴─────────────┴─────────────┘

┌─────────────────────────────────────────┐
│ [icon] 💾 Delta Lake Tables             │
│        delta-lake.png                   │
└─────────────────────────────────────────┘
```

### **Execution Plane (Orange Section):**
```
┌─────────────────────────────────────────┐
│ [icon] 🏭 Databricks SQL Warehouse      │
│        sql-warehouse.png                │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ [icon] 📊 Execution Monitoring          │
│        workflow.png                     │
└─────────────────────────────────────────┘
```

---

## 🎨 **Icon Sizing Tips**

- **Small icons (16x16 to 24x24)**: For inline with text
- **Medium icons (32x32 to 48x48)**: Next to component names
- **Large icons (64x64)**: For major components

**In draw.io:**
1. Select icon image
2. Right-click → **Edit Style**
3. Add: `width=32;height=32;` (adjust as needed)

---

## 📸 **After Adding Icons**

1. **Export new PNG:**
   ```
   File → Export as → PNG
   Scale: 200%
   Border: 10px
   Save as: SQLPilot.drawio.png
   ```

2. **Update repository:**
   ```bash
   cp ~/Downloads/SQLPilot.drawio.png \
      /Users/abhishek.dey/lakehouse-sqlpilot/docs/architecture.png
   
   cd /Users/abhishek.dey/lakehouse-sqlpilot
   git add -f docs/architecture.png docs/architecture.drawio
   git commit -m "docs: Add official Databricks icons to architecture diagram"
   git push origin main
   ```

---

## 💡 **Pro Tips**

1. **Keep icon style consistent** - All should be same style (dark red theme)
2. **Position icons on left** - Before text for better visual flow
3. **Maintain spacing** - Keep icons aligned and evenly spaced
4. **Test visibility** - Ensure icons are visible at 100% and 50% zoom

---

## 🎨 **Alternative: Icon Library**

Create a custom library in draw.io:

1. **File → New Library**
2. Add all 6 icons
3. Save as `databricks-icons.xml`
4. Reuse icons by dragging from library

---

## 📋 **Icon Reference**

All icons are from official Databricks icon set (dark red theme, Oct 2023).

**Icon locations:**
- Source: `/Users/abhishek.dey/Downloads/databricks-icons/dark red/`
- Project: `/Users/abhishek.dey/lakehouse-sqlpilot/docs/icons/`

**Additional icons available in source folder if needed:**
- `data-warehouse-1.png`, `data-warehouse-2.png`
- `delta-lake-2.png`, `delta-lakehouse.png`
- `delta-live-tables.png`
- `automation-orchestration.png`
- And 250+ more!

---

**Ready to enhance the architecture diagram with official Databricks branding! 🎨**

