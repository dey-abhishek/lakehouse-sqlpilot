import { useState, useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
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
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import RefreshIcon from '@mui/icons-material/Refresh'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import CloseIcon from '@mui/icons-material/Close'
import { api, type Plan as APIPlan } from '../services/api'

// Local interface for list view
interface Plan {
  plan_id: string
  plan_name: string
  version: string
  pattern_type: string
  owner: string
  created_at: string
  status: 'active' | 'draft' | 'archived'
}

// Transform API plan to list view plan
const transformPlan = (apiPlan: APIPlan): Plan => ({
  plan_id: apiPlan.plan_id || '',
  plan_name: apiPlan.plan_name,
  version: apiPlan.version || '1.0.0',
  pattern_type: apiPlan.pattern_type,
  owner: apiPlan.owner,
  created_at: apiPlan.created_at || new Date().toISOString(),
  status: apiPlan.status || 'active'
})

function PlanList() {
  const navigate = useNavigate()
  const location = useLocation()
  
  const [plans, setPlans] = useState<Plan[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [patternFilter, setPatternFilter] = useState<string>('')
  const [ownerFilter, setOwnerFilter] = useState<string>('')
  
  // SQL Preview Modal State
  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewSQL, setPreviewSQL] = useState('')
  const [previewPlanName, setPreviewPlanName] = useState('')
  const [previewPlanId, setPreviewPlanId] = useState('')
  const [previewWarehouseId, setPreviewWarehouseId] = useState('')
  const [copySuccess, setCopySuccess] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [tableExists, setTableExists] = useState<boolean | null>(null)
  const [checkingTable, setCheckingTable] = useState(false)
  const [creatingTable, setCreatingTable] = useState(false)

  const fetchPlans = async () => {
    try {
      console.log('[PlanList] fetchPlans called - starting fetch')
      setLoading(true)
      setError(null)
      setPlans([]) // Clear existing plans immediately
      
      const filters: { owner?: string; pattern_type?: string } = {}
      if (ownerFilter) filters.owner = ownerFilter
      if (patternFilter) filters.pattern_type = patternFilter
      
      console.log('[PlanList] Calling api.listPlans with filters:', filters)
      const response = await api.listPlans(filters)
      console.log('[PlanList] api.listPlans response:', response)
      const transformedPlans = (response.plans || []).map(transformPlan)
      console.log('[PlanList] Transformed plans count:', transformedPlans.length)
      setPlans(transformedPlans)
    } catch (err) {
      console.error('[PlanList] Failed to load plans:', err)
      setError(err instanceof Error ? err.message : 'Failed to load plans')
    } finally {
      setLoading(false)
      console.log('[PlanList] fetchPlans completed')
    }
  }

  // Fetch plans on mount and when navigating to this page
  useEffect(() => {
    console.log('[PlanList] useEffect triggered - fetching plans', {
      patternFilter,
      ownerFilter,
      locationKey: location.key,
      timestamp: new Date().toISOString()
    })
    fetchPlans()
  }, [patternFilter, ownerFilter, location.key])

  const handleEditPlan = (planId: string) => {
    navigate(`/plans/${planId}`)
  }

  const handlePreviewPlan = async (planId: string) => {
    console.log('[PlanList] Preview clicked for plan:', planId)
    try {
      // Fetch the plan first
      console.log('[PlanList] Fetching plan...')
      const plan = await api.getPlan(planId)
      console.log('[PlanList] Plan fetched:', plan)
      
      // Extract plan name from nested structure
      const planName = (plan as any).plan_metadata?.plan_name || (plan as any).plan_name || 'Unknown Plan'
      console.log('[PlanList] Plan name:', planName)
      
      const warehouseId = (plan as any).execution_config?.warehouse_id || ''
      const targetCatalog = (plan as any).target?.catalog
      const targetSchema = (plan as any).target?.schema
      const targetTable = (plan as any).target?.table
      
      console.log('[PlanList] Table check prerequisites:', {
        targetCatalog,
        targetSchema,
        targetTable,
        warehouseId,
        hasAll: !!(targetCatalog && targetSchema && targetTable && warehouseId)
      })
      
      // Remove _metadata field as it's not part of the plan schema for compilation
      const { _metadata, ...planWithoutMetadata } = plan as any
      console.log('[PlanList] Compiling plan...')
      
      // Compile it to SQL
      const result = await api.compilePlan(planWithoutMetadata)
      console.log('[PlanList] Compilation result:', result)
      
      if (result.success && result.sql) {
        console.log('[PlanList] Opening SQL preview modal')
        // Show SQL in modal dialog instead of alert
        setPreviewPlanId(planId)
        setPreviewPlanName(planName)
        setPreviewSQL(result.sql)
        setPreviewWarehouseId(warehouseId)
        setPreviewOpen(true)
        
        // Check if target table exists
        if (targetCatalog && targetSchema && targetTable && warehouseId) {
          setCheckingTable(true)
          setTableExists(null)
          try {
            console.log('[PlanList] Checking table existence:', { targetCatalog, targetSchema, targetTable, warehouseId })
            const tableCheck = await api.checkTableExists(targetCatalog, targetSchema, targetTable, warehouseId)
            console.log('[PlanList] Table check response:', tableCheck)
            setTableExists(tableCheck.exists)
            console.log('[PlanList] Table exists:', tableCheck.exists)
          } catch (err) {
            console.error('[PlanList] Failed to check table existence:', err)
            setTableExists(null)
          } finally {
            setCheckingTable(false)
          }
        }
      } else {
        console.error('[PlanList] Compilation failed:', result)
        alert('Failed to generate SQL preview')
      }
    } catch (error: any) {
      console.error('[PlanList] Preview failed with error:', error)
      alert(`Preview failed: ${error.message || 'Unknown error'}`)
    }
  }

  const handleCreateTable = async () => {
    if (!previewPlanId || !previewWarehouseId) {
      alert('Missing plan ID or warehouse ID')
      return
    }
    
    setCreatingTable(true)
    try {
      console.log('[PlanList] Creating table for plan:', previewPlanId)
      const result = await api.createTable(previewPlanId, previewWarehouseId)
      console.log('[PlanList] Create table response:', result)
      
      if (result.success) {
        alert(`✅ Table created successfully!\n\nTable: ${result.table}\nStatement ID: ${result.statement_id}`)
        
        // Force recheck of table existence
        await recheckTable()
      } else {
        alert(`Table creation failed: ${result.message}`)
      }
    } catch (error: any) {
      console.error('[PlanList] Table creation failed:', error)
      alert(`Table creation failed: ${error.message || 'Unknown error'}`)
    } finally {
      setCreatingTable(false)
    }
  }
  
  const recheckTable = async () => {
    // Re-fetch the plan to get latest table info
    if (!previewPlanId) return
    
    try {
      setCheckingTable(true)
      console.log('[PlanList] Rechecking table existence for plan:', previewPlanId)
      
      const plan = await api.getPlan(previewPlanId)
      const targetCatalog = (plan as any).target?.catalog
      const targetSchema = (plan as any).target?.schema
      const targetTable = (plan as any).target?.table
      const warehouseId = (plan as any).execution_config?.warehouse_id || previewWarehouseId
      
      if (targetCatalog && targetSchema && targetTable && warehouseId) {
        console.log('[PlanList] Rechecking table:', { targetCatalog, targetSchema, targetTable, warehouseId })
        const tableCheck = await api.checkTableExists(targetCatalog, targetSchema, targetTable, warehouseId)
        console.log('[PlanList] Recheck result:', tableCheck)
        setTableExists(tableCheck.exists)
      }
    } catch (error) {
      console.error('[PlanList] Failed to recheck table:', error)
    } finally {
      setCheckingTable(false)
    }
  }

  const handleCopySQL = () => {
    navigator.clipboard.writeText(previewSQL)
    setCopySuccess(true)
    setTimeout(() => setCopySuccess(false), 2000)
  }

  const handleClosePreview = () => {
    setPreviewOpen(false)
    setPreviewSQL('')
    setPreviewPlanName('')
    setPreviewPlanId('')
    setCopySuccess(false)
    setExecuting(false)
  }

  const handleExecuteSQL = async () => {
    console.log('[PlanList] handleExecuteSQL called')
    console.log('[PlanList] previewPlanId:', previewPlanId)
    console.log('[PlanList] previewSQL length:', previewSQL?.length)
    
    if (!previewPlanId || !previewSQL) {
      console.log('[PlanList] Missing previewPlanId or previewSQL, aborting')
      return
    }
    
    setExecuting(true)
    console.log('[PlanList] Set executing to true')
    
    try {
      console.log('[PlanList] Executing SQL for plan:', previewPlanId)
      
      // Get the plan to find warehouse_id and owner
      const plan = await api.getPlan(previewPlanId)
      console.log('[PlanList] Fetched plan:', plan)
      
      const warehouseId = (plan as any).execution_config?.warehouse_id
      const owner = (plan as any).plan_metadata?.owner
      
      console.log('[PlanList] warehouseId:', warehouseId)
      console.log('[PlanList] owner:', owner)
      
      if (!warehouseId) {
        alert('No warehouse configured for this plan. Please edit the plan and select a warehouse.')
        setExecuting(false)
        return
      }
      
      if (!owner) {
        alert('Plan owner not found. Please edit the plan.')
        setExecuting(false)
        return
      }
      
      // Execute the SQL
      console.log('[PlanList] Calling api.executePlan...')
      const result = await api.executePlan(
        previewPlanId,
        (plan as any).plan_metadata?.version || '1.0.0',
        previewSQL,
        warehouseId,
        owner
      )
      
      console.log('[PlanList] Execution result:', result)
      
      if (result.success) {
        // Handle multi-statement execution response
        if (result.execution_ids && Array.isArray(result.execution_ids)) {
          const details = result.execution_ids
            .map((exec: any) => `Statement ${exec.statement_number}: ${exec.statement_id} (${exec.status})`)
            .join('\n')
          
          alert(`✅ Successfully executed ${result.total_statements} statement(s)!\n\n${details}\n\nMonitor progress in Databricks SQL Warehouse.`)
        } else if (result.execution_id) {
          // Fallback for single execution ID
          alert(`✅ SQL execution started successfully!\n\nExecution ID: ${result.execution_id}\n\nMonitor progress in Databricks SQL Warehouse.`)
        } else {
          alert(`✅ ${result.message}`)
        }
        handleClosePreview()
      } else {
        setError(result.message || 'Execution failed.')
      }
    } catch (error: any) {
      console.error('[PlanList] Execution failed:', error)
      setError(error.detail?.message || error.message || 'Execution failed.')
    } finally {
      console.log('[PlanList] Setting executing to false')
      setExecuting(false)
    }
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">Plans</Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={fetchPlans}
            disabled={loading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => navigate('/plans/new')}
          >
            New Plan
          </Button>
        </Box>
      </Box>

      {/* Filters */}
      <Paper sx={{ p: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            select
            label="Pattern Type"
            value={patternFilter}
            onChange={(e) => setPatternFilter(e.target.value)}
            sx={{ minWidth: 200 }}
            size="small"
          >
            <MenuItem value="">All Patterns</MenuItem>
            <MenuItem value="INCREMENTAL_APPEND">Incremental Append</MenuItem>
            <MenuItem value="SCD2">SCD2</MenuItem>
            <MenuItem value="MERGE_UPSERT">Merge Upsert</MenuItem>
            <MenuItem value="FULL_REPLACE">Full Replace</MenuItem>
          </TextField>
          
          <TextField
            label="Owner (email)"
            value={ownerFilter}
            onChange={(e) => setOwnerFilter(e.target.value)}
            placeholder="Filter by owner"
            sx={{ minWidth: 250 }}
            size="small"
          />
        </Box>
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      ) : plans.length === 0 ? (
        <Paper sx={{ p: 4, textAlign: 'center' }}>
          <Typography variant="h6" color="text.secondary" gutterBottom>
            No plans found
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {patternFilter || ownerFilter
              ? 'Try adjusting your filters or create a new plan'
              : 'Get started by creating your first plan'}
          </Typography>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => navigate('/plans/new')}
          >
            Create Plan
          </Button>
        </Paper>
      ) : (
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
                <TableRow 
                  key={plan.plan_id}
                  hover
                  sx={{ cursor: 'pointer' }}
                  onClick={() => handleEditPlan(plan.plan_id)}
                >
                  <TableCell>
                    <Typography variant="body2" fontWeight="medium">
                      {plan.plan_name}
                    </Typography>
                  </TableCell>
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
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <Button 
                      size="small" 
                      onClick={() => handleEditPlan(plan.plan_id)}
                    >
                      Edit
                    </Button>
                    <Button 
                      size="small" 
                      onClick={() => handlePreviewPlan(plan.plan_id)}
                    >
                      Preview
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          
          <Box sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              Total: {plans.length} plan{plans.length !== 1 ? 's' : ''}
            </Typography>
          </Box>
        </TableContainer>
      )}

      {/* SQL Preview Modal */}
      <Dialog 
        open={previewOpen} 
        onClose={handleClosePreview}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="h6">
              SQL Preview: {previewPlanName}
            </Typography>
            <IconButton onClick={handleClosePreview} size="small">
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          {/* Table Status Alert */}
          {checkingTable && (
            <Alert severity="info" sx={{ mb: 2 }}>
              <CircularProgress size={16} sx={{ mr: 1 }} />
              Checking if target table exists...
            </Alert>
          )}
          {!checkingTable && tableExists === null && previewWarehouseId && (
            <Alert severity="info" sx={{ mb: 2 }}>
              <Typography variant="body2">
                ℹ️ Unable to check table existence. Missing plan configuration (catalog/schema/table/warehouse).
              </Typography>
            </Alert>
          )}
          {!checkingTable && tableExists === false && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              <Typography variant="body2">
                <strong>⚠️ Target table does not exist!</strong>
              </Typography>
              <Typography variant="body2" sx={{ mt: 1 }}>
                You need to create the target table before executing this SQL.
                Click "Create Table" below to create it automatically.
              </Typography>
            </Alert>
          )}
          {!checkingTable && tableExists === true && (
            <Alert severity="success" sx={{ mb: 2 }}>
              ✅ Target table exists and is ready for execution
            </Alert>
          )}
          
          <Paper 
            sx={{ 
              p: 2, 
              backgroundColor: '#1e1e1e',
              color: '#d4d4d4',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              maxHeight: '60vh',
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-all'
            }}
          >
            {previewSQL}
          </Paper>
        </DialogContent>
        <DialogActions>
          {/* Debug info - remove after testing */}
          <Typography variant="caption" sx={{ position: 'absolute', top: 8, right: 8, color: 'text.secondary' }}>
            tableExists: {String(tableExists)} | checking: {String(checkingTable)} | v2.0
          </Typography>
          
          <Button 
            startIcon={<ContentCopyIcon />}
            onClick={handleCopySQL}
            variant={copySuccess ? 'contained' : 'outlined'}
            color={copySuccess ? 'success' : 'primary'}
          >
            {copySuccess ? 'Copied!' : 'Copy to Clipboard'}
          </Button>
          {/* Always show Recheck button if we have the required info */}
          {(tableExists !== null || previewWarehouseId) && (
            <Button 
              onClick={recheckTable}
              variant="outlined"
              disabled={checkingTable}
              startIcon={checkingTable ? <CircularProgress size={20} color="inherit" /> : null}
            >
              {checkingTable ? 'Checking...' : 'Recheck Table'}
            </Button>
          )}
          {/* Show Create Table if table doesn't exist OR if we couldn't check */}
          {(tableExists === false || (tableExists === null && previewWarehouseId)) && (
            <Button 
              onClick={handleCreateTable}
              variant="contained"
              color="warning"
              disabled={creatingTable}
              startIcon={creatingTable ? <CircularProgress size={20} color="inherit" /> : null}
            >
              {creatingTable ? 'Creating Table...' : 'Create Table'}
            </Button>
          )}
          <Button 
            onClick={handleExecuteSQL}
            variant="contained"
            color="primary"
            disabled={executing || tableExists === false}
            startIcon={executing ? <CircularProgress size={20} color="inherit" /> : null}
          >
            {executing ? 'Executing...' : 'Execute SQL'}
          </Button>
          <Button onClick={handleClosePreview}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}

export default PlanList

