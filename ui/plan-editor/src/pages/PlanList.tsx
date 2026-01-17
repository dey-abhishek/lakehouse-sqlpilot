import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Button,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Chip,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'

interface Plan {
  plan_id: string
  plan_name: string
  version: string
  pattern_type: string
  owner: string
  created_at: string
  status: 'active' | 'draft' | 'archived'
}

function PlanList() {
  const navigate = useNavigate()
  
  // Mock data - in production, fetch from API
  const [plans] = useState<Plan[]>([
    {
      plan_id: '1',
      plan_name: 'customer_daily_incremental',
      version: '1.2.0',
      pattern_type: 'INCREMENTAL_APPEND',
      owner: 'data-team@company.com',
      created_at: '2026-01-15T10:00:00Z',
      status: 'active',
    },
    {
      plan_id: '2',
      plan_name: 'product_catalog_refresh',
      version: '1.0.0',
      pattern_type: 'FULL_REPLACE',
      owner: 'data-team@company.com',
      created_at: '2026-01-14T15:30:00Z',
      status: 'active',
    },
  ])

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">Plans</Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => navigate('/plans/new')}
        >
          New Plan
        </Button>
      </Box>

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Plan Name</TableCell>
              <TableCell>Version</TableCell>
              <TableCell>Pattern</TableCell>
              <TableCell>Owner</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {plans.map((plan) => (
              <TableRow key={plan.plan_id}>
                <TableCell>{plan.plan_name}</TableCell>
                <TableCell>{plan.version}</TableCell>
                <TableCell>
                  <Chip label={plan.pattern_type} size="small" />
                </TableCell>
                <TableCell>{plan.owner}</TableCell>
                <TableCell>{new Date(plan.created_at).toLocaleDateString()}</TableCell>
                <TableCell>
                  <Chip
                    label={plan.status}
                    color={plan.status === 'active' ? 'success' : 'default'}
                    size="small"
                  />
                </TableCell>
                <TableCell>
                  <Button size="small" onClick={() => navigate(`/plans/${plan.plan_id}`)}>
                    Edit
                  </Button>
                  <Button size="small" onClick={() => navigate(`/plans/${plan.plan_id}/preview`)}>
                    Preview
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default PlanList

