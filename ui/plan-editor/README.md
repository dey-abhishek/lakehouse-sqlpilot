# Lakehouse SQLPilot UI - Plan Editor

React-based plan editor for Lakehouse SQLPilot.

## Features

- **Form-Based Plan Editor**: No SQL writing required
- **Live Preview**: See generated SQL before execution
- **Execution Dashboard**: Monitor plan executions in real-time
- **Pattern Selection**: Choose from validated SQL patterns
- **Impact Analysis**: Understand what your plan will do
- **Governed**: All SQL generation enforced through patterns

## Development

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build
```

## Architecture

- **React 18** with TypeScript
- **Material-UI** for components (Databricks design system)
- **Inter font** (Databricks standard web font)
- **Monaco Editor** for SQL display
- **React Router** for navigation
- **React Query** for data fetching

## Design System

The UI follows Databricks design guidelines:
- **Colors**: Databricks Red (#FF3621), Charcoal (#1B3139), Green (#00A972), Blue (#0099E0)
- **Typography**: Inter font family with consistent weight hierarchy
- **Components**: Databricks-styled buttons, cards, alerts, and tables

See `DESIGN_SYSTEM.md` for full style guide.

## Pages

1. **Plan List** (`/`): View all plans
2. **Plan Editor** (`/plans/:id`): Create/edit plans
3. **Preview Pane** (`/plans/:id/preview`): Preview before execution
4. **Execution Dashboard** (`/executions`): Monitor executions

