# Genie ↔ SQLPilot Handoff

## Overview

This document describes the structured handoff workflow between Databricks Genie (exploration) and Lakehouse SQLPilot (production).

## Separation of Concerns

### Genie's Domain: Exploration
- Answer ad-hoc questions about data
- Validate table structures and schemas
- Preview sample data
- Test business logic with exploratory queries
- Understand data relationships

### SQLPilot's Domain: Production
- Execute governed SQL with audit trails
- Manage versioned plans
- Provide idempotency guarantees
- Schedule and monitor executions
- Enforce Unity Catalog policies

## Handoff Process

### Phase 1: Exploration in Genie

User explores data using natural language:

```
User: "Show me the schema of raw.customer_events"
Genie: [Returns schema with columns]

User: "How many events were recorded yesterday?"
Genie: [Returns count]

User: "Show me 10 sample rows"
Genie: [Returns sample data]
```

### Phase 2: Express Production Intent

User transitions from exploration to production:

```
User: "I want to load new customer events daily into curated.customer_daily"
```

At this point, Genie **CANNOT** create the production pipeline. Instead:

```
Genie: "I can help you explore the data, but for production SQL execution, 
        you'll need to use SQLPilot. Let me prepare the handoff information..."
```

### Phase 3: Create Handoff Context

Genie creates a structured handoff with:
- **Validated table references**: `raw.customer_events`, `curated.customer_daily`
- **Column information**: From Unity Catalog metadata
- **User intent**: "Load new customer events daily"
- **Exploration queries**: Queries run in Genie (for reference only)

### Phase 4: Transition to SQLPilot

User opens SQLPilot with handoff context:

1. SQLPilot receives validated table references
2. SQLPilot suggests appropriate pattern (INCREMENTAL_APPEND)
3. User reviews and customizes the plan
4. User previews generated SQL
5. User deploys the plan

## Boundary Enforcement

### What Genie CANNOT Do

❌ Create SQLPilot plans  
❌ Execute production SQL  
❌ Schedule recurring jobs  
❌ Provide governance guarantees  
❌ Generate audit trails  

### What SQLPilot CANNOT Do

❌ Answer exploratory questions  
❌ Generate ad-hoc queries  
❌ Provide BI visualizations  
❌ Perform free-form analysis  

### What BOTH Provide

✅ Access to Unity Catalog metadata  
✅ Schema information  
✅ Permission checking  
✅ Table existence validation  

## Handoff API

### Genie Side

```python
# Pseudo-code for Genie integration
handoff_context = {
    'genie_session_id': 'session-123',
    'validated_tables': [
        'prod_catalog.raw.customer_events',
        'prod_catalog.curated.customer_daily'
    ],
    'validated_columns': {
        'prod_catalog.raw.customer_events': ['customer_id', 'event_type', 'event_timestamp'],
        'prod_catalog.curated.customer_daily': ['customer_id', 'event_type', 'event_date']
    },
    'user_intent': 'Load new customer events daily',
    'exploration_summary': {
        'queries_run': 3,
        'tables_explored': 2,
        'sample_data_reviewed': True
    }
}

# Generate handoff URL
sqlpilot_url = f"https://sqlpilot/handoff?context={encode(handoff_context)}"
```

### SQLPilot Side

```python
from genie_handoff import GenieHandoffWorkflow

workflow = GenieHandoffWorkflow(workspace_client)

# Receive handoff context
context = workflow.create_handoff_context(genie_session_id)

# Validate readiness
is_ready, issues = workflow.validate_handoff_readiness(context)

if is_ready:
    # Generate initial plan
    plan = workflow.generate_plan_from_handoff(context)
    
    # User reviews and deploys in SQLPilot UI
```

## Example Workflow

### Step-by-Step

1. **Exploration** (in Genie):
   ```
   User: "What columns does raw.customer_events have?"
   Genie: customer_id, event_type, event_timestamp, ...
   ```

2. **Validation** (in Genie):
   ```
   User: "Show me events from the last hour"
   Genie: [Returns sample data]
   ```

3. **Intent** (in Genie):
   ```
   User: "I need this running every day in production"
   Genie: "Let me hand this off to SQLPilot..."
   ```

4. **Handoff** (transition):
   - Genie creates handoff context
   - Opens SQLPilot with context
   - Tables and columns pre-filled

5. **Plan Creation** (in SQLPilot):
   - Pattern: INCREMENTAL_APPEND suggested
   - Configuration: watermark_column = event_timestamp
   - User reviews and customizes

6. **Preview** (in SQLPilot):
   ```sql
   -- Generated SQL (read-only)
   INSERT INTO prod_catalog.curated.customer_daily
   SELECT * FROM prod_catalog.raw.customer_events
   WHERE event_timestamp > (
       SELECT MAX(event_timestamp)
       FROM prod_catalog.curated.customer_daily
   );
   ```

7. **Deployment** (in SQLPilot):
   - Plan validated
   - Permissions checked
   - Schedule configured (daily at 2 AM)
   - Deployed to production

## UI Integration

### Genie UI
- "Create Production Pipeline" button appears when user expresses production intent
- Button triggers handoff workflow
- Opens SQLPilot in new tab/window with context

### SQLPilot UI
- "Import from Genie" option in plan creation
- Auto-fills table references from handoff
- Shows Genie session link for reference
- Pre-populates pattern based on intent

## Security & Governance

### Permissions
- Handoff only transfers **metadata**, never actual data
- SQLPilot re-validates all permissions at execution time
- Unity Catalog is the single source of truth

### Audit Trail
- Handoff is logged with Genie session ID
- SQLPilot execution references original Genie session
- Full lineage from exploration to production

## Future Enhancements

- Automatic pattern detection from Genie queries
- Richer exploration context (query performance, data profiles)
- Bi-directional feedback (SQLPilot execution stats back to Genie)


