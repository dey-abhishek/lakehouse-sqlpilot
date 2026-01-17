import { useState } from 'react'
import {
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Chip,
  Button,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'

interface Execution {
  execution_id: string
  plan_name: string
  state: string
  started_at: string
  completed_at: string | null
  rows_affected: number | null
  executor_user: string
}

function ExecutionDashboard() {
  const [executions] = useState<Execution[]>([
    {
      execution_id: 'exec-001',
      plan_name: 'customer_daily_incremental',
      state: 'SUCCESS',
      started_at: '2026-01-16T02:00:00Z',
      completed_at: '2026-01-16T02:05:30Z',
      rows_affected: 15234,
      executor_user: 'system',
    },
    {
      execution_id: 'exec-002',
      plan_name: 'product_catalog_refresh',
      state: 'RUNNING',
      started_at: '2026-01-16T03:00:00Z',
      completed_at: null,
      rows_affected: null,
      executor_user: 'system',
    },
  ])

  const getStateColor = (state: string) => {
    switch (state) {
      case 'SUCCESS':
        return 'success'
      case 'RUNNING':
        return 'info'
      case 'FAILED':
        return 'error'
      case 'PENDING':
        return 'default'
      default:
        return 'default'
    }
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">Execution Dashboard</Typography>
        <Button variant="outlined" startIcon={<RefreshIcon />}>
          Refresh
        </Button>
      </Box>

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Execution ID</TableCell>
              <TableCell>Plan Name</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Started</TableCell>
              <TableCell>Duration</TableCell>
              <TableCell>Rows Affected</TableCell>
              <TableCell>Executor</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {executions.map((exec) => {
              const duration = exec.completed_at
                ? Math.round(
                    (new Date(exec.completed_at).getTime() -
                      new Date(exec.started_at).getTime()) /
                      1000
                  )
                : null

              return (
                <TableRow key={exec.execution_id}>
                  <TableCell>{exec.execution_id}</TableCell>
                  <TableCell>{exec.plan_name}</TableCell>
                  <TableCell>
                    <Chip label={exec.state} color={getStateColor(exec.state)} size="small" />
                  </TableCell>
                  <TableCell>{new Date(exec.started_at).toLocaleString()}</TableCell>
                  <TableCell>{duration ? `${duration}s` : '-'}</TableCell>
                  <TableCell>{exec.rows_affected?.toLocaleString() || '-'}</TableCell>
                  <TableCell>{exec.executor_user}</TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

export default ExecutionDashboard

