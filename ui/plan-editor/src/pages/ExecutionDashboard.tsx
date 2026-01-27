import { useState, useEffect } from 'react'
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
  CircularProgress,
  Alert,
  TextField,
  MenuItem,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline'
import { api } from '../services/api'

interface Execution {
  execution_id: number
  plan_id: string
  plan_name: string
  plan_version: string
  executor_user: string
  warehouse_id: string
  status: string
  started_at: string
  completed_at: string | null
  total_statements: number
  succeeded_statements: number
  failed_statements: number
  error_message: string | null
}

function ExecutionDashboard() {
  const [executions, setExecutions] = useState<Execution[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [userFilter, setUserFilter] = useState<string>('')
  const [errorDialogOpen, setErrorDialogOpen] = useState(false)
  const [selectedError, setSelectedError] = useState<string | null>(null)

  const fetchExecutions = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const params: any = {}
      if (statusFilter) params.status = statusFilter
      if (userFilter) params.executor_user = userFilter
      
      const response = await api.listExecutions(params)
      setExecutions(response.executions || [])
    } catch (err: any) {
      console.error('Failed to load executions:', err)
      setError(err.message || 'Failed to load executions')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchExecutions()
    // Auto-refresh every 10 seconds
    const interval = setInterval(fetchExecutions, 10000)
    return () => clearInterval(interval)
  }, [statusFilter, userFilter])

  const getStatusColor = (status: string): 'default' | 'info' | 'success' | 'error' | 'warning' => {
    switch (status) {
      case 'SUCCEEDED':
        return 'success'
      case 'RUNNING':
      case 'SUBMITTED':
        return 'info'
      case 'FAILED':
        return 'error'
      case 'PARTIAL':
        return 'warning'
      default:
        return 'default'
    }
  }

  const formatDuration = (startedAt: string, completedAt: string | null) => {
    if (!completedAt) return 'In Progress'
    
    const start = new Date(startedAt)
    const end = new Date(completedAt)
    const durationMs = end.getTime() - start.getTime()
    const seconds = Math.floor(durationMs / 1000)
    
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = seconds % 60
    return `${minutes}m ${remainingSeconds}s`
  }

  const handleErrorClick = (errorMessage: string) => {
    setSelectedError(errorMessage)
    setErrorDialogOpen(true)
  }

  const handleCloseErrorDialog = () => {
    setErrorDialogOpen(false)
    setSelectedError(null)
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">Execution Dashboard</Typography>
        <Button 
          variant="outlined" 
          startIcon={<RefreshIcon />}
          onClick={fetchExecutions}
          disabled={loading}
        >
          Refresh
        </Button>
      </Box>

      {/* Filters */}
      <Paper sx={{ p: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            select
            label="Status"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            sx={{ minWidth: 200 }}
            size="small"
          >
            <MenuItem value="">All Statuses</MenuItem>
            <MenuItem value="SUBMITTED">Submitted</MenuItem>
            <MenuItem value="RUNNING">Running</MenuItem>
            <MenuItem value="SUCCEEDED">Succeeded</MenuItem>
            <MenuItem value="FAILED">Failed</MenuItem>
            <MenuItem value="PARTIAL">Partial</MenuItem>
          </TextField>
          
          <TextField
            label="Executor User"
            value={userFilter}
            onChange={(e) => setUserFilter(e.target.value)}
            placeholder="Filter by user email"
            size="small"
            sx={{ minWidth: 300 }}
          />
        </Box>
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading && executions.length === 0 ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      ) : executions.length === 0 ? (
        <Alert severity="info">
          No executions found. Execute a plan to see results here.
        </Alert>
      ) : (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Execution ID</TableCell>
                <TableCell>Plan Name</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Started</TableCell>
                <TableCell>Duration</TableCell>
                <TableCell>Statements</TableCell>
                <TableCell>Executor</TableCell>
                <TableCell>Error</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {executions.map((exec) => (
                <TableRow key={exec.execution_id}>
                  <TableCell>
                    <Typography variant="body2" fontWeight="medium">
                      {exec.execution_id}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {exec.plan_name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      v{exec.plan_version}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip 
                      label={exec.status} 
                      color={getStatusColor(exec.status)} 
                      size="small" 
                    />
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {new Date(exec.started_at).toLocaleString()}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {formatDuration(exec.started_at, exec.completed_at)}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {exec.succeeded_statements} / {exec.total_statements}
                      {exec.failed_statements > 0 && (
                        <Typography component="span" color="error" sx={{ ml: 1 }}>
                          ({exec.failed_statements} failed)
                        </Typography>
                      )}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Tooltip title={exec.executor_user} arrow placement="top">
                      <Typography 
                        variant="body2" 
                        noWrap 
                        sx={{ 
                          maxWidth: 200,
                          cursor: 'help'
                        }}
                      >
                        {exec.executor_user}
                      </Typography>
                    </Tooltip>
                  </TableCell>
                  <TableCell>
                    {exec.error_message ? (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Tooltip title="Click to view full error" arrow>
                          <IconButton 
                            size="small" 
                            color="error"
                            onClick={() => handleErrorClick(exec.error_message!)}
                          >
                            <ErrorOutlineIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        <Typography 
                          variant="caption" 
                          color="error"
                          sx={{ 
                            maxWidth: 250, 
                            overflow: 'hidden', 
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            cursor: 'pointer',
                            '&:hover': {
                              textDecoration: 'underline'
                            }
                          }}
                          onClick={() => handleErrorClick(exec.error_message!)}
                        >
                          {exec.error_message}
                        </Typography>
                      </Box>
                    ) : (
                      <Typography variant="caption" color="text.secondary">
                        —
                      </Typography>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          
          <Box sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              Showing {executions.length} execution{executions.length !== 1 ? 's' : ''}
              {loading && ' (Refreshing...)'}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Auto-refreshes every 10 seconds
            </Typography>
          </Box>
        </TableContainer>
      )}

      {/* Error Dialog */}
      <Dialog 
        open={errorDialogOpen} 
        onClose={handleCloseErrorDialog}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <ErrorOutlineIcon color="error" />
            <Typography variant="h6">Execution Error Details</Typography>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Paper 
            variant="outlined" 
            sx={{ 
              p: 2, 
              backgroundColor: '#fff5f5',
              fontFamily: 'monospace',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              maxHeight: '400px',
              overflow: 'auto'
            }}
          >
            <Typography variant="body2" component="pre" sx={{ margin: 0 }}>
              {selectedError}
            </Typography>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseErrorDialog}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}

export default ExecutionDashboard
