import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Box,
  Button,
  Paper,
  TextField,
  MenuItem,
  Grid,
  Typography,
  Alert,
} from '@mui/material'
import SaveIcon from '@mui/icons-material/Save'
import PreviewIcon from '@mui/icons-material/Preview'

const PATTERN_TYPES = [
  'INCREMENTAL_APPEND',
  'FULL_REPLACE',
  'MERGE_UPSERT',
  'SCD2',
  'SNAPSHOT',
  'AGGREGATE_REFRESH',
]

const WRITE_MODES = ['append', 'overwrite', 'merge']

function PlanEditor() {
  const { id } = useParams()
  const navigate = useNavigate()
  const isNew = !id

  const [plan, setPlan] = useState({
    plan_name: '',
    description: '',
    owner: '',
    pattern_type: 'INCREMENTAL_APPEND',
    source_catalog: '',
    source_schema: '',
    source_table: '',
    target_catalog: '',
    target_schema: '',
    target_table: '',
    write_mode: 'append',
    warehouse_id: '',
  })

  const handleChange = (field: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
    setPlan({ ...plan, [field]: event.target.value })
  }

  const handleSave = () => {
    // In production, call API to save plan
    console.log('Saving plan:', plan)
    alert('Plan saved successfully!')
  }

  const handlePreview = () => {
    navigate(`/plans/${id || 'new'}/preview`)
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">{isNew ? 'New Plan' : 'Edit Plan'}</Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<PreviewIcon />}
            onClick={handlePreview}
            sx={{ mr: 1 }}
          >
            Preview
          </Button>
          <Button variant="contained" startIcon={<SaveIcon />} onClick={handleSave}>
            Save
          </Button>
        </Box>
      </Box>

      <Alert 
        severity="info" 
        sx={{ 
          mb: 3,
          backgroundColor: '#E3F2FD',
          border: '1px solid #0099E0',
          '& .MuiAlert-icon': {
            color: '#0099E0',
          },
        }}
      >
        <strong>Governed Plan Creation:</strong> This form creates a governed plan. No free-form SQL is allowed. All SQL is generated from validated patterns.
      </Alert>

      <Paper sx={{ p: 3 }}>
        <Grid container spacing={3}>
          {/* Plan Metadata */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom>
              Plan Metadata
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Plan Name"
              value={plan.plan_name}
              onChange={handleChange('plan_name')}
              helperText="Lowercase, alphanumeric, underscores only"
            />
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Owner Email"
              type="email"
              value={plan.owner}
              onChange={handleChange('owner')}
            />
          </Grid>
          
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Description"
              multiline
              rows={2}
              value={plan.description}
              onChange={handleChange('description')}
            />
          </Grid>

          {/* Pattern Selection */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Pattern
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              select
              label="Pattern Type"
              value={plan.pattern_type}
              onChange={handleChange('pattern_type')}
            >
              {PATTERN_TYPES.map((type) => (
                <MenuItem key={type} value={type}>
                  {type}
                </MenuItem>
              ))}
            </TextField>
          </Grid>

          {/* Source Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Source Table
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Catalog"
              value={plan.source_catalog}
              onChange={handleChange('source_catalog')}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Schema"
              value={plan.source_schema}
              onChange={handleChange('source_schema')}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Table"
              value={plan.source_table}
              onChange={handleChange('source_table')}
            />
          </Grid>

          {/* Target Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Target Table
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Catalog"
              value={plan.target_catalog}
              onChange={handleChange('target_catalog')}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Schema"
              value={plan.target_schema}
              onChange={handleChange('target_schema')}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Table"
              value={plan.target_table}
              onChange={handleChange('target_table')}
            />
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              select
              label="Write Mode"
              value={plan.write_mode}
              onChange={handleChange('write_mode')}
            >
              {WRITE_MODES.map((mode) => (
                <MenuItem key={mode} value={mode}>
                  {mode}
                </MenuItem>
              ))}
            </TextField>
          </Grid>

          {/* Execution Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Execution Configuration
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Warehouse ID"
              value={plan.warehouse_id}
              onChange={handleChange('warehouse_id')}
              helperText="Databricks SQL Warehouse ID"
            />
          </Grid>
        </Grid>
      </Paper>
    </Box>
  )
}

export default PlanEditor

