import { useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  Box,
  Paper,
  Typography,
  Alert,
  Button,
  Grid,
  Divider,
  Chip,
} from '@mui/material'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import WarningIcon from '@mui/icons-material/Warning'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import Editor from '@monaco-editor/react'

function PreviewPane() {
  const { id } = useParams()
  
  // Mock preview data
  const [preview] = useState({
    is_valid: true,
    sql: `-- LAKEHOUSE SQLPILOT GENERATED SQL
-- plan_id: 550e8400-e29b-41d4-a716-446655440000
-- plan_version: 1.0.0
-- pattern: INCREMENTAL_APPEND
-- generated_at: 2026-01-16T00:00:00Z

INSERT INTO prod_catalog.curated.customer_daily
SELECT 
  *
FROM prod_catalog.raw.customer_events
WHERE event_timestamp > (
    SELECT COALESCE(MAX(event_timestamp), '1970-01-01 00:00:00')
    FROM prod_catalog.curated.customer_daily
  );`,
    warnings: [
      'This operation affects a PRODUCTION catalog',
    ],
    permissions: {
      has_permissions: true,
      violations: [],
    },
    impact_analysis: {
      source_table: 'prod_catalog.raw.customer_events',
      target_table: 'prod_catalog.curated.customer_daily',
      operation: 'APPEND',
      is_destructive: false,
      estimated_risk: 'low',
    },
    sample_data: {
      columns: ['customer_id', 'event_type', 'event_timestamp'],
      rows: [
        ['1001', 'purchase', '2026-01-16 10:30:00'],
        ['1002', 'view', '2026-01-16 10:35:00'],
        ['1003', 'purchase', '2026-01-16 10:40:00'],
      ],
    },
  })

  const handleExecute = () => {
    // In production, call API to execute
    alert('Plan execution started!')
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">Preview Plan</Typography>
        <Button
          variant="contained"
          color="primary"
          startIcon={<PlayArrowIcon />}
          onClick={handleExecute}
          disabled={!preview.is_valid}
        >
          Execute
        </Button>
      </Box>

      {/* Validation Status */}
      <Alert
        severity={preview.is_valid ? 'success' : 'error'}
        icon={preview.is_valid ? <CheckCircleIcon /> : <WarningIcon />}
        sx={{ 
          mb: 2,
          fontWeight: 600,
          backgroundColor: preview.is_valid ? '#E8F5E9' : '#FFEBEE',
          border: `1px solid ${preview.is_valid ? '#00A972' : '#FF3621'}`,
        }}
      >
        {preview.is_valid ? 'Plan is valid and ready to execute' : 'Plan has validation errors'}
      </Alert>

      {/* Warnings */}
      {preview.warnings.length > 0 && (
        <Alert 
          severity="warning" 
          sx={{ 
            mb: 2,
            backgroundColor: '#FFF3E0',
            border: '1px solid #FF9F3A',
          }}
        >
          <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 600 }}>
            Warnings:
          </Typography>
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            {preview.warnings.map((warning, i) => (
              <li key={i}>{warning}</li>
            ))}
          </ul>
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Generated SQL */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Generated SQL
            </Typography>
            <Typography variant="body2" color="text.secondary" gutterBottom>
              This SQL is deterministically generated from your plan. No manual edits allowed.
            </Typography>
            <Box sx={{ mt: 2, border: '1px solid #ddd', borderRadius: 1 }}>
              <Editor
                height="400px"
                language="sql"
                value={preview.sql}
                options={{
                  readOnly: true,
                  minimap: { enabled: false },
                }}
              />
            </Box>
          </Paper>

          {/* Sample Data */}
          <Paper sx={{ p: 2, mt: 3 }}>
            <Typography variant="h6" gutterBottom>
              Sample Output (First 10 rows)
            </Typography>
            <Box sx={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid #ddd' }}>
                    {preview.sample_data.columns.map((col, i) => (
                      <th key={i} style={{ padding: '8px', textAlign: 'left' }}>
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.sample_data.rows.map((row, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #eee' }}>
                      {row.map((cell, j) => (
                        <td key={j} style={{ padding: '8px' }}>
                          {cell}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </Box>
          </Paper>
        </Grid>

        {/* Impact Analysis */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Impact Analysis
            </Typography>
            <Divider sx={{ mb: 2 }} />
            
            <Typography variant="body2" gutterBottom>
              <strong>Source:</strong>
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              {preview.impact_analysis.source_table}
            </Typography>
            
            <Typography variant="body2" gutterBottom>
              <strong>Target:</strong>
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              {preview.impact_analysis.target_table}
            </Typography>
            
            <Typography variant="body2" gutterBottom>
              <strong>Operation:</strong>
            </Typography>
            <Chip
              label={preview.impact_analysis.operation}
              size="small"
              sx={{ mb: 2 }}
            />
            
            <Typography variant="body2" gutterBottom>
              <strong>Destructive:</strong>
            </Typography>
            <Chip
              label={preview.impact_analysis.is_destructive ? 'Yes' : 'No'}
              color={preview.impact_analysis.is_destructive ? 'error' : 'success'}
              size="small"
              sx={{ mb: 2 }}
            />
            
            <Typography variant="body2" gutterBottom>
              <strong>Risk Level:</strong>
            </Typography>
            <Chip
              label={preview.impact_analysis.estimated_risk.toUpperCase()}
              color={preview.impact_analysis.estimated_risk === 'low' ? 'success' : 'warning'}
              size="small"
            />
          </Paper>

          {/* Permissions */}
          <Paper sx={{ p: 2, mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Permissions
            </Typography>
            <Divider sx={{ mb: 2 }} />
            
            {preview.permissions.has_permissions ? (
              <Alert severity="success">All required permissions granted</Alert>
            ) : (
              <Alert severity="error">
                Missing permissions:
                <ul style={{ margin: 0, paddingLeft: 20 }}>
                  {preview.permissions.violations.map((v, i) => (
                    <li key={i}>{v}</li>
                  ))}
                </ul>
              </Alert>
            )}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  )
}

export default PreviewPane

