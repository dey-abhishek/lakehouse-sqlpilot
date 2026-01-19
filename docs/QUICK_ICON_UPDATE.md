# Quick Icon Update for Draw.io

The draw.io file has been prepared for icons with proper spacing and alignment.

## 🚀 **5-Minute Icon Update**

### **Step 1: Open Draw.io**
```
https://app.diagrams.net
→ Open Existing Diagram
→ Select: docs/architecture.drawio
```

### **Step 2: Open Icons Folder**
```
Open Finder:
/Users/abhishek.dey/lakehouse-sqlpilot/docs/icons/
```

### **Step 3: Drag & Drop Icons** (in order)

#### **Governance Layer (Green - Bottom)**

1. **Unity Catalog** (line 770):
   - Drag `unity-catalog.png` 
   - Place at: x=110, y=777 (left side of box)
   - Resize to: 36x36 pixels

2. **Data Lineage** (line 840):
   - Drag `data-lineage.png`
   - Place at: x=110, y=852 (top-left of first governance box)
   - Resize to: 24x24 pixels

3. **Audit Logs** (line 840):
   - Drag `governance.png`
   - Place at: x=350, y=852 (top-left of second box)
   - Resize to: 24x24 pixels

4. **Access Control** (line 840):
   - Drag `governance.png`
   - Place at: x=590, y=852 (top-left of third box)
   - Resize to: 24x24 pixels

5. **Delta Lake** (line 930):
   - Drag `delta-lake.png`
   - Place at: x=110, y=937 (left side of box)
   - Resize to: 36x36 pixels

#### **Execution Layer (Orange - Middle)**

6. **SQL Warehouse** (line 605):
   - Drag `sql-warehouse.png`
   - Place at: x=110, y=617 (left side of box)
   - Resize to: 36x36 pixels

7. **Execution Monitoring** (line 605):
   - Drag `workflow.png`
   - Place at: x=610, y=617 (left side of box)
   - Resize to: 36x36 pixels

### **Step 4: Align & Style**

For each icon:
1. Select icon
2. Right-click → Edit Style
3. Add: `opacity=100;`
4. Verify icon is on top layer (right-click → To Front)

### **Step 5: Export**

```
File → Export as → PNG
Settings:
  - Scale: 200%
  - Border: 10px
  - Transparent: OFF
  - Include: "Diagram"

Save as: SQLPilot.drawio.png to Downloads
```

### **Step 6: Update Repo**

```bash
cp ~/Downloads/SQLPilot.drawio.png \
   /Users/abhishek.dey/lakehouse-sqlpilot/docs/architecture.png

cd /Users/abhishek.dey/lakehouse-sqlpilot
git add -f docs/architecture.png docs/architecture.drawio
git commit -m "docs: Add Databricks icons to architecture diagram"
git push origin main
```

## 📐 **Icon Positions Reference**

| Component | Icon | X | Y | Size |
|-----------|------|---|---|------|
| Unity Catalog | unity-catalog.png | 110 | 777 | 36x36 |
| SQL Warehouse | sql-warehouse.png | 110 | 617 | 36x36 |
| Execution Monitor | workflow.png | 610 | 617 | 36x36 |
| Data Lineage | data-lineage.png | 110 | 852 | 24x24 |
| Audit/Access | governance.png | 350/590 | 852 | 24x24 |
| Delta Lake | delta-lake.png | 110 | 937 | 36x36 |

## ✅ **What's Already Done**

- ✅ Draw.io file updated with space for icons
- ✅ Text alignment changed to left with left padding
- ✅ Emoji removed (will be replaced by icons)
- ✅ Proper spacing allocated

## 🎨 **Result**

After adding icons, you'll have:
- Professional Databricks branding
- Official product icons
- Clean, modern look
- Production-ready architecture diagram

**Time required: ~5 minutes** ⏱️

