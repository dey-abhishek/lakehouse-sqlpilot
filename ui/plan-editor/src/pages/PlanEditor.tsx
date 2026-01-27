import { useState, useEffect } from 'react'
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
  Autocomplete,
  CircularProgress,
  Snackbar,
  Checkbox,
  FormControlLabel,
} from '@mui/material'
import SaveIcon from '@mui/icons-material/Save'
import PreviewIcon from '@mui/icons-material/Preview'
import { api } from '../services/api'
import type { Catalog, Schema, Table, Warehouse } from '../services/api'

const PATTERN_TYPES = [
  'INCREMENTAL_APPEND',
  'FULL_REPLACE',
  'MERGE_UPSERT',
  'SCD2',
  'SNAPSHOT',
]

const WRITE_MODES = ['append', 'overwrite', 'merge']

function PlanEditor() {
  const { id } = useParams()
  const navigate = useNavigate()
  const isNew = !id

  const [plan, setPlan] = useState({
    plan_id: '',
    plan_name: '',
    description: '',
    owner: '',
    version: '1.0.0',
    created_at: '',
    pattern_type: 'INCREMENTAL_APPEND',
    source_catalog: '',
    source_schema: '',
    source_table: '',
    source_columns: '',  // For SCD2/MERGE_UPSERT
    target_catalog: '',
    target_schema: '',
    target_table: '',
    write_mode: 'append',
    warehouse_id: '',
    config: {},
    // Pattern-specific fields
    watermark_column: '',
    watermark_type: 'timestamp',
    match_columns: '',
    merge_keys: '',
    business_keys: '',
    effective_date_column: 'effective_date',
    end_date_column: 'end_date',
    current_flag_column: 'is_current',
    snapshot_date_column: '',
    partition_columns: '',
    // Full Replace specific fields
    refresh_mode: 'direct',
    refresh_inplace: false,
    cluster_columns: '',
    table_format: 'delta',
    iceberg_version: '2',  // Default to v2
    enable_uniform: false,  // Default to standard Delta (no UniForm)
    filter_condition: '',
    table_properties: '',
  })

  // Track the original plan name to detect changes
  const [originalPlanName, setOriginalPlanName] = useState<string>('')

  // Unity Catalog data
  const [catalogs, setCatalogs] = useState<Catalog[]>([])
  const [sourceSchemas, setSourceSchemas] = useState<Schema[]>([])
  const [sourceTables, setSourceTables] = useState<Table[]>([])
  const [targetSchemas, setTargetSchemas] = useState<Schema[]>([])
  const [warehouses, setWarehouses] = useState<Warehouse[]>([])
  
  // Loading states
  const [loadingCatalogs, setLoadingCatalogs] = useState(false)
  const [loadingSourceSchemas, setLoadingSourceSchemas] = useState(false)
  const [loadingSourceTables, setLoadingSourceTables] = useState(false)
  const [loadingTargetSchemas, setLoadingTargetSchemas] = useState(false)
  const [loadingWarehouses, setLoadingWarehouses] = useState(false)
  const [saving, setSaving] = useState(false)
  
  // Notifications
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' as 'success' | 'error' })
  const [loadingPlan, setLoadingPlan] = useState(false)

  // Load existing plan if editing
  useEffect(() => {
    if (id && id !== 'new') {
      loadExistingPlan(id)
    }
  }, [id])

  const loadExistingPlan = async (planId: string) => {
    setLoadingPlan(true)
    try {
      console.log('[PlanEditor] Loading plan:', planId)
      const existingPlan: any = await api.getPlan(planId)
      
      console.log('[PlanEditor] Received plan from API:', {
        plan_id: existingPlan.plan_metadata?.plan_id,
        plan_name: existingPlan.plan_metadata?.plan_name,
        pattern_type: existingPlan.pattern?.type,
        owner: existingPlan.plan_metadata?.owner,
        business_keys: existingPlan.pattern_config?.business_keys,
        source_columns: existingPlan.source?.columns,
      })
      
      // Transform plan from backend format to UI format
      setPlan({
        plan_id: existingPlan.plan_metadata?.plan_id || existingPlan.plan_id || '',
        plan_name: existingPlan.plan_metadata?.plan_name || existingPlan.plan_name || '',
        description: existingPlan.plan_metadata?.description || existingPlan.description || '',
        owner: existingPlan.plan_metadata?.owner || existingPlan.owner || '',
        version: existingPlan.plan_metadata?.version || existingPlan.version || '1.0.0',
        created_at: existingPlan.plan_metadata?.created_at || existingPlan.created_at || '',
        pattern_type: existingPlan.pattern?.type || existingPlan.pattern_type || 'INCREMENTAL_APPEND',
        source_catalog: existingPlan.source?.catalog || existingPlan.source_catalog || '',
        source_schema: existingPlan.source?.schema || existingPlan.source_schema || '',
        source_table: existingPlan.source?.table || existingPlan.source_table || '',
        source_columns: existingPlan.source?.columns?.join(',') || '',
        target_catalog: existingPlan.target?.catalog || existingPlan.target_catalog || '',
        target_schema: existingPlan.target?.schema || existingPlan.target_schema || '',
        target_table: existingPlan.target?.table || existingPlan.target_table || '',
        write_mode: existingPlan.target?.write_mode || existingPlan.write_mode || 'append',
        warehouse_id: existingPlan.execution_config?.warehouse_id || existingPlan.warehouse_id || '',
        config: existingPlan.pattern_config || existingPlan.config || {},
        watermark_column: existingPlan.pattern_config?.watermark_column || '',
        watermark_type: existingPlan.pattern_config?.watermark_type || 'timestamp',
        match_columns: existingPlan.pattern_config?.match_columns?.join(',') || '',
        merge_keys: existingPlan.pattern_config?.merge_keys?.join(',') || '',
        business_keys: existingPlan.pattern_config?.business_keys?.join(',') || '',
        effective_date_column: existingPlan.pattern_config?.effective_date_column || 'effective_date',
        end_date_column: existingPlan.pattern_config?.end_date_column || 'end_date',
        current_flag_column: existingPlan.pattern_config?.current_flag_column || 'is_current',
        snapshot_date_column: existingPlan.pattern_config?.snapshot_date_column || '',
        partition_columns: existingPlan.pattern_config?.partition_columns?.join(',') || '',
        // Full Replace fields
        refresh_mode: existingPlan.pattern_config?.refresh_mode || 'direct',
        refresh_inplace: existingPlan.pattern_config?.refresh_inplace || false,
        cluster_columns: existingPlan.pattern_config?.cluster_columns?.join(',') || '',
        table_format: existingPlan.pattern_config?.table_format || 'delta',
        iceberg_version: existingPlan.pattern_config?.iceberg_version || '2',  // Default to v2
        enable_uniform: existingPlan.pattern_config?.enable_uniform || false,  // Default to false (standard Delta)
        filter_condition: existingPlan.pattern_config?.filter_condition || '',
        table_properties: existingPlan.pattern_config?.table_properties ? JSON.stringify(existingPlan.pattern_config.table_properties, null, 2) : '',
      })
      
      // Store the original plan name to detect changes
      setOriginalPlanName(existingPlan.plan_metadata?.plan_name || existingPlan.plan_name || '')
      
      console.log('[PlanEditor] Set plan state with pattern_type:', existingPlan.pattern?.type)
      
      setSnackbar({ 
        open: true, 
        message: `Loaded plan: ${existingPlan.plan_metadata?.plan_name || existingPlan.plan_name || 'Unknown'}`, 
        severity: 'success' 
      })
    } catch (error) {
      console.error('[PlanEditor] Failed to load plan:', error)
      setSnackbar({ 
        open: true, 
        message: error instanceof Error ? error.message : 'Failed to load plan', 
        severity: 'error' 
      })
      // Navigate back to list on error
      setTimeout(() => navigate('/'), 2000)
    } finally {
      setLoadingPlan(false)
    }
  }

  // Load catalogs and warehouses on mount
  useEffect(() => {
    loadCatalogs()
    loadWarehouses()
  }, [])

  // Load source schemas when source catalog changes
  useEffect(() => {
    if (plan.source_catalog) {
      loadSourceSchemas(plan.source_catalog)
    } else {
      setSourceSchemas([])
      setSourceTables([])
    }
  }, [plan.source_catalog])

  // Load source tables when source schema changes
  useEffect(() => {
    if (plan.source_catalog && plan.source_schema) {
      loadSourceTables(plan.source_catalog, plan.source_schema)
    } else {
      setSourceTables([])
    }
  }, [plan.source_catalog, plan.source_schema])

  // Load target schemas when target catalog changes
  useEffect(() => {
    if (plan.target_catalog) {
      loadTargetSchemas(plan.target_catalog)
    } else {
      setTargetSchemas([])
    }
  }, [plan.target_catalog])

  const loadCatalogs = async () => {
    setLoadingCatalogs(true)
    try {
      const response = await api.listCatalogs()
      setCatalogs(response.catalogs)
    } catch (error) {
      console.error('Failed to load catalogs:', error)
      setSnackbar({ open: true, message: 'Failed to load catalogs', severity: 'error' })
    } finally {
      setLoadingCatalogs(false)
    }
  }

  const loadSourceSchemas = async (catalog: string) => {
    setLoadingSourceSchemas(true)
    try {
      const response = await api.listSchemas(catalog)
      setSourceSchemas(response.schemas)
    } catch (error) {
      console.error('Failed to load schemas:', error)
      setSnackbar({ open: true, message: 'Failed to load schemas', severity: 'error' })
    } finally {
      setLoadingSourceSchemas(false)
    }
  }

  const loadSourceTables = async (catalog: string, schema: string) => {
    setLoadingSourceTables(true)
    try {
      const response = await api.listTables(catalog, schema)
      setSourceTables(response.tables)
    } catch (error) {
      console.error('Failed to load tables:', error)
      setSnackbar({ open: true, message: 'Failed to load tables', severity: 'error' })
    } finally {
      setLoadingSourceTables(false)
    }
  }

  const loadTargetSchemas = async (catalog: string) => {
    setLoadingTargetSchemas(true)
    try {
      const response = await api.listSchemas(catalog)
      setTargetSchemas(response.schemas)
    } catch (error) {
      console.error('Failed to load schemas:', error)
      setSnackbar({ open: true, message: 'Failed to load schemas', severity: 'error' })
    } finally {
      setLoadingTargetSchemas(false)
    }
  }

  const autoFillSourceColumns = async () => {
    if (!plan.source_catalog || !plan.source_schema || !plan.source_table) {
      setSnackbar({ 
        open: true, 
        message: 'Please select source catalog, schema, and table first', 
        severity: 'error' 
      })
      return
    }

    setLoadingSourceTables(true) // Reuse loading state
    try {
      const response = await api.getTableColumns(
        plan.source_catalog,
        plan.source_schema,
        plan.source_table
      )
      const columnNames = response.columns.map((col: any) => col.name).join(', ')
      setPlan({ ...plan, source_columns: columnNames })
      setSnackbar({ 
        open: true, 
        message: `Auto-filled ${response.columns.length} columns`, 
        severity: 'success' 
      })
    } catch (error) {
      console.error('Failed to fetch columns:', error)
      setSnackbar({ 
        open: true, 
        message: 'Failed to fetch columns from source table', 
        severity: 'error' 
      })
    } finally {
      setLoadingSourceTables(false)
    }
  }

  const loadWarehouses = async () => {
    setLoadingWarehouses(true)
    try {
      const response = await api.listWarehouses()
      setWarehouses(response.warehouses)
    } catch (error) {
      console.error('Failed to load warehouses:', error)
      setSnackbar({ open: true, message: 'Failed to load warehouses', severity: 'error' })
    } finally {
      setLoadingWarehouses(false)
    }
  }

  const handleChange = (field: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
    let value: string | boolean = event.target.value
    
    // Handle checkbox inputs
    if (event.target.type === 'checkbox') {
      value = event.target.checked
    }
    
    // Sanitize plan_name: convert to lowercase and remove invalid characters
    if (field === 'plan_name' && typeof value === 'string') {
      value = value
        .toLowerCase()                    // Convert to lowercase
        .replace(/[^a-z0-9_]/g, '_')     // Replace invalid chars with underscore
        .replace(/_+/g, '_')              // Replace multiple underscores with single
        .replace(/^_|_$/g, '')            // Remove leading/trailing underscores
    }
    
    setPlan({ ...plan, [field]: value })
  }

  const handleSave = async () => {
    // Validation
    if (!plan.plan_name) {
      setSnackbar({ open: true, message: 'Plan name is required', severity: 'error' })
      return
    }
    if (!plan.owner) {
      setSnackbar({ open: true, message: 'Owner email is required', severity: 'error' })
      return
    }
    if (!plan.source_catalog || !plan.source_schema || !plan.source_table) {
      setSnackbar({ open: true, message: 'Source table information is required', severity: 'error' })
      return
    }
    if (!plan.target_catalog || !plan.target_schema || !plan.target_table) {
      setSnackbar({ open: true, message: 'Target table information is required', severity: 'error' })
      return
    }

    setSaving(true)
    try {
      // Build pattern_config and source based on pattern type
      let patternConfig: any = {}
      let sourceConfig: any = {
        catalog: plan.source_catalog!,
        schema: plan.source_schema!,
        table: plan.source_table!,
      }
      
      // Configure based on pattern type
      switch (plan.pattern_type) {
      case 'INCREMENTAL_APPEND':
        patternConfig = {
          watermark_column: plan.watermark_column || (plan.config as any)?.watermark_column,
          watermark_type: plan.watermark_type || (plan.config as any)?.watermark_type || 'timestamp',
        }
        // Add match_columns if provided (for merge mode)
        if (plan.match_columns) {
          patternConfig.match_columns = plan.match_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        } else if (plan.write_mode === 'merge') {
          setSnackbar({ 
            open: true, 
            message: 'Incremental Append with MERGE mode requires Match Columns', 
            severity: 'error' 
          })
          setSaving(false)
          return
        }
        break
          
        case 'MERGE_UPSERT':
          patternConfig = {
            merge_keys: plan.merge_keys 
              ? plan.merge_keys.split(',').map((k: string) => k.trim()).filter((k: string) => k)
              : ((plan.config as any)?.merge_keys || []),
          }
          // Add columns if specified
          if (plan.source_columns) {
            sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          break
          
        case 'SCD2':
          patternConfig = {
            business_keys: plan.business_keys 
              ? plan.business_keys.split(',').map((k: string) => k.trim()).filter((k: string) => k)
              : ((plan.config as any)?.business_keys || []),
            effective_date_column: plan.effective_date_column || 'effective_date',
            end_date_column: plan.end_date_column || 'end_date',
            current_flag_column: plan.current_flag_column || 'is_current',
            end_date_default: '9999-12-31 23:59:59',
          }
          // SCD2 requires explicit columns
          if (plan.source_columns) {
            sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          } else {
            setSnackbar({ open: true, message: 'SCD2 requires source columns', severity: 'error' })
            setSaving(false)
            return
          }
          break
          
        case 'SNAPSHOT':
          patternConfig = {
            snapshot_date_column: plan.snapshot_date_column || (plan.config as any)?.snapshot_date_column,
          }
          // Add partition columns if specified
          if (plan.partition_columns) {
            patternConfig.partition_columns = plan.partition_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          // Add source columns if specified
          if (plan.source_columns) {
            sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          break
          
        case 'FULL_REPLACE':
          patternConfig = {
            refresh_mode: plan.refresh_mode || 'direct',
            refresh_inplace: plan.refresh_inplace || false,
            table_format: plan.table_format || 'delta',
            iceberg_version: plan.iceberg_version || '2',  // Always include version (for both Delta and Iceberg)
            enable_uniform: plan.enable_uniform || false,  // Include UniForm flag for Delta tables
          }
          // Add cluster columns if specified
          if (plan.cluster_columns) {
            patternConfig.cluster_columns = plan.cluster_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          // Add partition columns if specified
          if (plan.partition_columns) {
            patternConfig.partition_columns = plan.partition_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          // Add filter condition if specified
          if (plan.filter_condition) {
            patternConfig.filter_condition = plan.filter_condition.trim()
          }
          // Add table properties if specified
          if (plan.table_properties) {
            try {
              patternConfig.table_properties = JSON.parse(plan.table_properties)
            } catch (e) {
              setSnackbar({ 
                open: true, 
                message: 'Invalid JSON in Table Properties', 
                severity: 'error' 
              })
              setSaving(false)
              return
            }
          }
          // Add source columns if specified
          if (plan.source_columns) {
            sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
          }
          break
          
        default:
          patternConfig = plan.config || {}
      }
      
      // Determine write_mode based on pattern
      let writeMode = plan.write_mode || 'append'
      if (plan.pattern_type === 'SCD2' || plan.pattern_type === 'MERGE_UPSERT') {
        writeMode = 'merge'
      } else if (plan.pattern_type === 'FULL_REPLACE') {
        writeMode = 'overwrite'
      }
      
      // Transform flat plan structure to nested structure expected by backend
      const transformedPlan = {
        schema_version: '1.0',
        plan_metadata: {
          // If plan name changed, generate new plan_id to create a new plan
          plan_id: plan.plan_name !== originalPlanName && originalPlanName ? crypto.randomUUID() : (plan.plan_id || crypto.randomUUID()),
          plan_name: plan.plan_name,
          description: plan.description || '',
          owner: plan.owner,
          created_at: plan.plan_name !== originalPlanName && originalPlanName ? new Date().toISOString() : (plan.created_at || new Date().toISOString()),
          version: plan.version || '1.0.0',
        },
        pattern: {
          type: plan.pattern_type,
        },
        source: sourceConfig,
        target: {
          catalog: plan.target_catalog!,
          schema: plan.target_schema!,
          table: plan.target_table!,
          write_mode: writeMode,
        },
        pattern_config: patternConfig,
        execution_config: {
          warehouse_id: plan.warehouse_id || '',
        },
      }

      const response = await api.savePlan(transformedPlan as any, plan.owner)
      console.log('[PlanEditor] Plan saved successfully:', response)
      
      // Show appropriate message based on whether it's update or create
      const action = plan.plan_name !== originalPlanName && originalPlanName ? 'created as a new plan' : 'saved'
      setSnackbar({ open: true, message: response.message || `Plan ${action} successfully!`, severity: 'success' })
      
      // Navigate to plan list immediately (no delay)
      console.log('[PlanEditor] Navigating to /')
      navigate('/', { replace: true })
    } catch (error: any) {
      console.error('Failed to save plan:', error)
      console.error('Plan data:', plan)
      
      // Extract error message from various error formats
      let errorMessage = 'Unknown error'
      if (error?.detail) {
        // FastAPI HTTPException
        if (typeof error.detail === 'string') {
          errorMessage = error.detail
        } else if (error.detail?.message) {
          errorMessage = error.detail.message
          if (error.detail?.errors && Array.isArray(error.detail.errors)) {
            errorMessage += ': ' + error.detail.errors.join(', ')
          }
        } else {
          errorMessage = JSON.stringify(error.detail)
        }
      } else if (error?.message) {
        errorMessage = error.message
      } else if (typeof error === 'string') {
        errorMessage = error
      } else {
        errorMessage = JSON.stringify(error)
      }
      
      setSnackbar({ 
        open: true, 
        message: `Failed to save plan: ${errorMessage}`, 
        severity: 'error' 
      })
    } finally {
      setSaving(false)
    }
  }

  const handlePreview = async () => {
    // Build the current plan from form state (same as handleSave)
    let patternConfig: any = {}
    let sourceConfig: any = {
      catalog: plan.source_catalog!,
      schema: plan.source_schema!,
      table: plan.source_table!,
    }
    
    // Configure based on pattern type
    switch (plan.pattern_type) {
      case 'INCREMENTAL_APPEND':
        patternConfig = {
          watermark_column: plan.watermark_column || (plan.config as any)?.watermark_column,
          watermark_type: plan.watermark_type || (plan.config as any)?.watermark_type || 'timestamp',
        }
        // Add match_columns if provided (for merge mode)
        if (plan.match_columns) {
          patternConfig.match_columns = plan.match_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        } else if (plan.write_mode === 'merge') {
          setSnackbar({ 
            open: true, 
            message: 'Incremental Append with MERGE mode requires Match Columns', 
            severity: 'error' 
          })
          return
        }
        break
        
      case 'MERGE_UPSERT':
        patternConfig = {
          merge_keys: plan.merge_keys 
            ? plan.merge_keys.split(',').map((k: string) => k.trim()).filter((k: string) => k)
            : ((plan.config as any)?.merge_keys || []),
        }
        if (plan.source_columns) {
          sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        }
        break
        
      case 'SCD2':
        patternConfig = {
          business_keys: plan.business_keys 
            ? plan.business_keys.split(',').map((k: string) => k.trim()).filter((k: string) => k)
            : ((plan.config as any)?.business_keys || []),
          effective_date_column: plan.effective_date_column || 'effective_date',
          end_date_column: plan.end_date_column || 'end_date',
          current_flag_column: plan.current_flag_column || 'is_current',
          end_date_default: '9999-12-31 23:59:59',
        }
        if (plan.source_columns) {
          sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        }
        break
        
      case 'FULL_REPLACE':
        patternConfig = {
          refresh_mode: plan.refresh_mode || 'direct',
          refresh_inplace: plan.refresh_inplace || false,
          table_format: plan.table_format || 'delta',
          iceberg_version: plan.iceberg_version || '2',  // Always include version (for both Delta and Iceberg)
          enable_uniform: plan.enable_uniform || false,  // Include UniForm flag for Delta tables
        }
        // Add cluster columns if specified
        if (plan.cluster_columns) {
          patternConfig.cluster_columns = plan.cluster_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        }
        // Add partition columns if specified
        if (plan.partition_columns) {
          patternConfig.partition_columns = plan.partition_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        }
        // Add filter condition if specified
        if (plan.filter_condition) {
          patternConfig.filter_condition = plan.filter_condition.trim()
        }
        // Add table properties if specified
        if (plan.table_properties) {
          try {
            patternConfig.table_properties = JSON.parse(plan.table_properties)
          } catch (e) {
            setSnackbar({ 
              open: true, 
              message: 'Invalid JSON in Table Properties', 
              severity: 'error' 
            })
            return
          }
        }
        // Add source columns if specified
        if (plan.source_columns) {
          sourceConfig.columns = plan.source_columns.split(',').map((c: string) => c.trim()).filter((c: string) => c)
        }
        break
        
      default:
        patternConfig = plan.config || {}
    }
    
    // Determine write_mode based on pattern
    let writeMode = plan.write_mode || 'append'
    if (plan.pattern_type === 'SCD2' || plan.pattern_type === 'MERGE_UPSERT') {
      writeMode = 'merge'
    } else if (plan.pattern_type === 'FULL_REPLACE') {
      writeMode = 'overwrite'
    }
    
    // Build the plan structure
    const currentPlan = {
      schema_version: '1.0',
      plan_metadata: {
        plan_id: plan.plan_id || crypto.randomUUID(),
        plan_name: plan.plan_name,
        description: plan.description || '',
        owner: plan.owner,
        created_at: plan.created_at || new Date().toISOString(),
        version: plan.version || '1.0.0',
      },
      pattern: {
        type: plan.pattern_type,
      },
      source: sourceConfig,
      target: {
        catalog: plan.target_catalog!,
        schema: plan.target_schema!,
        table: plan.target_table!,
        write_mode: writeMode,
      },
      pattern_config: patternConfig,
      execution_config: {
        warehouse_id: plan.warehouse_id || '',
      },
    }

    try {
      // Compile the current plan to SQL
      const result = await api.compilePlan(currentPlan as any)
      
      // Show SQL in an alert for now (could be a modal later)
      if (result.success && result.sql) {
        // Create a modal or navigate with SQL in state
        alert(`Generated SQL:\n\n${result.sql}`)
      } else {
        setSnackbar({ 
          open: true, 
          message: 'Failed to generate SQL preview', 
          severity: 'error' 
        })
      }
    } catch (error: any) {
      console.error('Preview failed:', error)
      setSnackbar({ 
        open: true, 
        message: `Preview failed: ${error.message || 'Unknown error'}`, 
        severity: 'error' 
      })
    }
  }

  const handleCloseSnackbar = () => {
    setSnackbar({ ...snackbar, open: false })
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h4">{isNew ? 'New Plan' : `Edit: ${plan.plan_name || 'Loading...'}`}</Typography>
        {loadingPlan && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CircularProgress size={20} />
            <Typography variant="body2" color="text.secondary">Loading plan...</Typography>
          </Box>
        )}
        <Box>
          <Button
            variant="outlined"
            startIcon={<PreviewIcon />}
            onClick={handlePreview}
            sx={{ mr: 1 }}
            disabled={saving}
          >
            Preview
          </Button>
          <Button 
            variant="contained" 
            startIcon={saving ? <CircularProgress size={20} color="inherit" /> : <SaveIcon />} 
            onClick={handleSave}
            disabled={saving}
          >
            {saving ? 'Saving...' : 'Save'}
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
              helperText="Auto-converted: lowercase, numbers, underscores only (e.g., my_sales_plan_001)"
              placeholder="e.g., customer_incremental_load"
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
          
          {/* Pattern Description */}
          <Grid item xs={12}>
            <Alert severity="info">
              {plan.pattern_type === 'INCREMENTAL_APPEND' && 
                'Appends only new or changed records based on a watermark column (e.g., timestamp, date, or sequence number).'}
              {plan.pattern_type === 'FULL_REPLACE' && 
                'Atomically replaces the entire target table with source data. Best for smaller dimensions or complete refreshes.'}
              {plan.pattern_type === 'MERGE_UPSERT' && 
                'Updates existing records and inserts new ones based on merge keys. Efficient for maintaining current state.'}
              {plan.pattern_type === 'SCD2' && 
                'Tracks historical changes with versioning. Maintains full history of dimension changes over time.'}
              {plan.pattern_type === 'SNAPSHOT' && 
                'Captures a complete point-in-time snapshot of the source table. Perfect for daily inventory snapshots, end-of-period balances, or historical reporting.'}
              {!['INCREMENTAL_APPEND', 'FULL_REPLACE', 'MERGE_UPSERT', 'SCD2', 'SNAPSHOT'].includes(plan.pattern_type) && 
                'Select a pattern type to see its description.'}
            </Alert>
          </Grid>

          {/* Source Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Source Table
              {plan.pattern_type === 'FULL_REPLACE' && plan.refresh_inplace && (
                <Typography component="span" variant="body2" sx={{ ml: 2, fontStyle: 'italic', color: 'primary.main' }}>
                  (For format conversion, use different table name than Target)
                </Typography>
              )}
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={4}>
            <Autocomplete
              options={catalogs.map(c => c.name)}
              value={plan.source_catalog || null}
              onChange={(_, newValue) => setPlan({ ...plan, source_catalog: newValue || '', source_schema: '', source_table: '' })}
              loading={loadingCatalogs}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Catalog"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingCatalogs ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <Autocomplete
              options={sourceSchemas.map(s => s.name)}
              value={plan.source_schema || null}
              onChange={(_, newValue) => setPlan({ ...plan, source_schema: newValue || '', source_table: '' })}
              loading={loadingSourceSchemas}
              disabled={!plan.source_catalog}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Schema"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingSourceSchemas ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <Autocomplete
              options={sourceTables.map(t => t.name)}
              value={plan.source_table || null}
              onChange={(_, newValue) => setPlan({ ...plan, source_table: newValue || '' })}
              loading={loadingSourceTables}
              disabled={!plan.source_schema}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Table"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingSourceTables ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>

          {/* Target Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Target Table
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={4}>
            <Autocomplete
              options={catalogs.map(c => c.name)}
              value={plan.target_catalog || null}
              onChange={(_, newValue) => {
                setPlan({ ...plan, target_catalog: newValue || '', target_schema: '' });
              }}
              loading={loadingCatalogs}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Catalog"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingCatalogs ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <Autocomplete
              options={targetSchemas.map(s => s.name)}
              value={plan.target_schema || null}
              onChange={(_, newValue) => {
                setPlan({ ...plan, target_schema: newValue || '' });
              }}
              loading={loadingTargetSchemas}
              disabled={!plan.target_catalog}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Schema"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingTargetSchemas ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>
          
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label={
                plan.pattern_type === 'FULL_REPLACE' && plan.refresh_inplace
                  ? "Table (existing table for in-place refresh)"
                  : "Table (will be created)"
              }
              value={plan.target_table}
              onChange={(e) => {
                const newValue = e.target.value;
                setPlan({ ...plan, target_table: newValue });
              }}
              helperText={
                plan.pattern_type === 'FULL_REPLACE' && plan.refresh_inplace
                  ? "Name of existing table to refresh in-place"
                  : "Target table name (SQLPilot will create it)"
              }
            />
          </Grid>
          
          {/* Write Mode - Only show for INCREMENTAL_APPEND */}
          {plan.pattern_type === 'INCREMENTAL_APPEND' && (
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                select
                label="Write Mode"
                value={plan.write_mode}
                onChange={handleChange('write_mode')}
                helperText="How to write data to the target table"
              >
                {WRITE_MODES.map((mode) => (
                  <MenuItem key={mode} value={mode}>
                    {mode}
                  </MenuItem>
                ))}
              </TextField>
            </Grid>
          )}
          
          {/* WARNING: Overwrite mode for Incremental Append */}
          {plan.pattern_type === 'INCREMENTAL_APPEND' && plan.write_mode === 'overwrite' && (
            <Grid item xs={12}>
              <Alert severity="warning" sx={{ mt: 1 }}>
                <strong>⚠️ DATA LOSS WARNING: OVERWRITE Mode</strong>
                <br />
                This mode will <strong>REPLACE the entire table</strong> with <strong>ONLY the latest batch</strong> of incremental data.
                <br />
                <strong>All historical data will be deleted</strong> on each run!
                <br /><br />
                <strong>Example:</strong>
                <ul style={{ margin: '8px 0', paddingLeft: '20px' }}>
                  <li>Run 1: Load 100 records → Table has 100 rows</li>
                  <li>Run 2: Load 20 new records → Table has <strong>20 rows</strong> (lost 100!)</li>
                  <li>Run 3: Load 10 new records → Table has <strong>10 rows</strong> (lost 20!)</li>
                </ul>
                <strong>Use cases:</strong> Staging tables, temporary processing, real-time dashboards showing only "what changed today"
                <br /><br />
                ✅ <strong>To keep ALL history:</strong> Use write_mode='<strong>append</strong>'
                <br />
                ✅ <strong>To replace with FULL source:</strong> Use the '<strong>Full Replace</strong>' pattern
              </Alert>
            </Grid>
          )}
          
          {/* Source Columns - Required for certain patterns (NOT for Incremental Append MERGE) */}
          {(plan.pattern_type === 'MERGE_UPSERT' || 
            plan.pattern_type === 'SCD2' || 
            plan.pattern_type === 'SNAPSHOT') && (
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Source Columns *"
                value={plan.source_columns}
                onChange={handleChange('source_columns')}
                helperText="Comma-separated column names from source table"
                placeholder="customer_id, name, email"
                required
                multiline
                maxRows={4}
              />
            </Grid>
          )}

          {/* Pattern-Specific Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>
              Pattern Configuration
            </Typography>
          </Grid>

          {/* INCREMENTAL_APPEND Configuration */}
          {plan.pattern_type === 'INCREMENTAL_APPEND' && (
            <>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Watermark Column *"
                  value={plan.watermark_column}
                  onChange={handleChange('watermark_column')}
                  helperText="Column for tracking changes (e.g., updated_at, created_at)"
                  placeholder="updated_at"
                  required
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  select
                  label="Watermark Type *"
                  value={plan.watermark_type}
                  onChange={handleChange('watermark_type')}
                  helperText="Data type of watermark column"
                  required
                >
                  <MenuItem value="timestamp">Timestamp</MenuItem>
                  <MenuItem value="date">Date</MenuItem>
                  <MenuItem value="integer">Integer</MenuItem>
                </TextField>
              </Grid>
              
              {plan.write_mode === 'merge' && (
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Match Columns *"
                    value={plan.match_columns}
                    onChange={handleChange('match_columns')}
                    helperText="Comma-separated columns to match on for MERGE (e.g., customer_id, order_id)"
                    placeholder="customer_id"
                    required
                  />
                </Grid>
              )}
            </>
          )}

          {/* MERGE_UPSERT Configuration */}
          {plan.pattern_type === 'MERGE_UPSERT' && (
            <>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Merge Keys *"
                  value={plan.merge_keys}
                  onChange={handleChange('merge_keys')}
                  helperText="Comma-separated columns for matching (e.g., customer_id, order_id)"
                  placeholder="customer_id"
                  required
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Source Columns (optional)"
                  value={plan.source_columns}
                  onChange={handleChange('source_columns')}
                  helperText="Columns to update/insert (leave empty for all)"
                  placeholder="customer_id,name,email"
                />
              </Grid>
            </>
          )}

          {/* SCD2 Configuration */}
          {plan.pattern_type === 'SCD2' && (
            <>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Business Keys *"
                  value={plan.business_keys}
                  onChange={handleChange('business_keys')}
                  helperText="Comma-separated unique identifiers (e.g., customer_id)"
                  placeholder="customer_id"
                  required
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Source Columns *"
                  value={plan.source_columns}
                  onChange={handleChange('source_columns')}
                  helperText="All source columns (comma-separated)"
                  placeholder="customer_id,name,email,segment,updated_at"
                  required
                />
              </Grid>
              
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Effective Date Column"
                  value={plan.effective_date_column}
                  onChange={handleChange('effective_date_column')}
                  helperText="Column for record start date"
                  placeholder="effective_date"
                />
              </Grid>
              
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="End Date Column"
                  value={plan.end_date_column}
                  onChange={handleChange('end_date_column')}
                  helperText="Column for record end date"
                  placeholder="end_date"
                />
              </Grid>
              
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Current Flag Column"
                  value={plan.current_flag_column}
                  onChange={handleChange('current_flag_column')}
                  helperText="Column for current record indicator"
                  placeholder="is_current"
                />
              </Grid>
            </>
          )}

          {/* FULL_REPLACE Configuration */}
          {plan.pattern_type === 'FULL_REPLACE' && (
            <>
              <Grid item xs={12}>
                <Alert severity="info">
                  <strong>Full Replace Pattern:</strong> Advanced table maintenance and optimization.
                  <br />
                  Use for repartitioning, liquid clustering, schema evolution, or applying table properties.
                </Alert>
              </Grid>

              {/* Refresh Mode */}
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  select
                  label="Refresh Mode"
                  value={plan.refresh_mode}
                  onChange={handleChange('refresh_mode')}
                  helperText="Direct: immediate replacement. Staging: zero-downtime with validation"
                >
                  <MenuItem value="direct">Direct Replace (Immediate)</MenuItem>
                  <MenuItem value="staging">Staging Table (Recommended for Production)</MenuItem>
                </TextField>
              </Grid>

              {/* Table Format */}
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  select
                  label="Table Format (Target)"
                  value={plan.table_format}
                  onChange={handleChange('table_format')}
                  helperText="Delta: Keep as Delta. Iceberg: Convert to Managed Iceberg (use different table name)"
                >
                  <MenuItem value="delta">Delta Lake</MenuItem>
                  <MenuItem value="iceberg">Apache Iceberg (Managed)</MenuItem>
                </TextField>
              </Grid>

              {/* Show format conversion guidance */}
              {plan.refresh_inplace && plan.source_table && plan.target_table && plan.source_table !== plan.target_table && (
                <Grid item xs={12}>
                  <Alert severity="info">
                    <strong>💡 Format Conversion Detected</strong>
                    <br />
                    Source: <code>{plan.source_catalog}.{plan.source_schema}.{plan.source_table}</code>
                    <br />
                    Target: <code>{plan.target_catalog}.{plan.target_schema}.{plan.target_table}</code> (as <strong>{plan.table_format.toUpperCase()}</strong>)
                    <br /><br />
                    This will use <code>DROP TABLE IF EXISTS + CREATE TABLE</code> to convert formats.
                  </Alert>
                </Grid>
              )}

              {/* Format Enhancement - show for both Delta and Iceberg */}
              {plan.table_format === 'delta' && (
                <>
                  <Grid item xs={12}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={plan.enable_uniform || false}
                          onChange={handleChange('enable_uniform')}
                        />
                      }
                      label={
                        <span>
                          Enable UniForm (Iceberg Compatibility)
                          <br />
                          <em style={{ fontSize: '0.875rem', color: '#666' }}>
                            Add Iceberg read compatibility while keeping Delta format
                          </em>
                        </span>
                      }
                    />
                  </Grid>

                  {plan.enable_uniform && (
                    <Grid item xs={12} md={6}>
                      <TextField
                        fullWidth
                        select
                        label="UniForm Version"
                        value={plan.iceberg_version}
                        onChange={handleChange('iceberg_version')}
                        helperText="v2: No deletion vectors (basic). v3: Deletion vectors + row tracking (enhanced)"
                      >
                        <MenuItem value="2">UniForm v2 (Iceberg v2 compatible)</MenuItem>
                        <MenuItem value="3">UniForm v3 (Iceberg v3 compatible)</MenuItem>
                      </TextField>
                    </Grid>
                  )}

                  {plan.enable_uniform && (
                    <Grid item xs={12}>
                      <Alert severity="info">
                        <strong>ℹ️ Delta + UniForm Clarification</strong>
                        <br />
                        • Table stays <strong>DELTA format</strong> but adds Iceberg metadata for dual-format reads
                        <br />
                        • For <strong>full Managed Iceberg conversion</strong> (Delta → Iceberg), select "Apache Iceberg (Managed)" in the "Table Format (Target)" dropdown above
                      </Alert>
                    </Grid>
                  )}
                </>
              )}

              {plan.table_format === 'iceberg' && (
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    select
                    label="Iceberg Version"
                    value={plan.iceberg_version}
                    onChange={handleChange('iceberg_version')}
                    helperText="v2: No deletion vectors. v3: Deletion vectors + row tracking"
                  >
                    <MenuItem value="2">Iceberg v2</MenuItem>
                    <MenuItem value="3">Iceberg v3</MenuItem>
                  </TextField>
                </Grid>
              )}


              {/* In-place Refresh Checkbox */}
              <Grid item xs={12}>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={plan.refresh_inplace}
                      onChange={(e) => {
                        const isChecked = e.target.checked;
                        setPlan({
                          ...plan, 
                          refresh_inplace: isChecked,
                          // When checking INITIALLY, copy target to source as a convenience
                          // But user can change source table name afterwards for format conversions
                          source_catalog: isChecked && !plan.source_catalog ? plan.target_catalog : plan.source_catalog,
                          source_schema: isChecked && !plan.source_schema ? plan.target_schema : plan.source_schema,
                          source_table: isChecked && !plan.source_table ? plan.target_table : plan.source_table
                        });
                      }}
                    />
                  }
                  label={
                    <span>
                      Refresh table in-place (source = target)
                      <br />
                      <em style={{ fontSize: '0.875rem', color: '#666' }}>
                        {plan.refresh_inplace ? (
                          <>
                            ✓ <strong>In-place mode:</strong> If source = target (same table name), uses ALTER TABLE for upgrades.
                            <br />
                            • Same name: Iceberg v2→v3, Delta→UniForm (ALTER TABLE)
                            <br />
                            • Different names: Delta→Iceberg conversion (DROP + CREATE)
                          </>
                        ) : (
                          <>
                            Load from one table (Source) and write to a different table (Target).
                            <br />
                            Check this box if Source and Target are related (same data, different format/structure).
                          </>
                        )}
                      </em>
                    </span>
                  }
                />
              </Grid>

              {/* Alert when in-place mode is active */}
              {plan.refresh_inplace && (
                <Grid item xs={12}>
                  <Alert severity="info">
                    <strong>ℹ️ In-Place Refresh Mode</strong>
                    <br />
                    <br />
                    <strong>If source = target (same table name):</strong>
                    <br />
                    • Uses <code>ALTER TABLE</code> statements to upgrade properties without recreating the table
                    <br />
                    • Examples: Iceberg v2→v3, Delta→Delta+UniForm, adding Liquid Clustering
                    <br />
                    • Table data remains unchanged, only metadata/properties are modified
                    <br />
                    <br />
                    <strong>If source ≠ target (different table names):</strong>
                    <br />
                    • Uses <code>DROP TABLE IF EXISTS + CREATE TABLE</code> for format conversion or table recreation
                    <br />
                    • Examples: <strong>Delta→Iceberg</strong>, <strong>Iceberg→Delta</strong>, or creating new table with different config
                    <br />
                    • Data is copied from source to newly created target table
                    <br />
                    • <strong>💡 TIP:</strong> For format conversion, select target format in "Table Format (Target)" dropdown above
                  </Alert>
                </Grid>
              )}

              {/* Partition Columns */}
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Partition Columns (optional)"
                  value={plan.partition_columns}
                  onChange={handleChange('partition_columns')}
                  helperText="Comma-separated columns for PARTITIONED BY (e.g., year, month, region)"
                  placeholder="year, month, region"
                  disabled={!!plan.cluster_columns}
                />
              </Grid>

              {/* Cluster Columns (Liquid Clustering) */}
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Clustering Columns (optional)"
                  value={plan.cluster_columns}
                  onChange={handleChange('cluster_columns')}
                  helperText="Comma-separated columns for CLUSTER BY / Liquid Clustering (e.g., customer_id, order_date)"
                  placeholder="customer_id, order_date"
                  disabled={!!plan.partition_columns}
                />
              </Grid>

              {/* Warning if both are disabled */}
              {plan.partition_columns && plan.cluster_columns && (
                <Grid item xs={12}>
                  <Alert severity="warning">
                    Cannot specify both partitioning and clustering. Please choose one.
                  </Alert>
                </Grid>
              )}

              {/* Warning for Iceberg v2 + Liquid Clustering */}
              {plan.table_format === 'iceberg' && plan.iceberg_version === '2' && plan.cluster_columns && (
                <Grid item xs={12}>
                  <Alert severity="warning">
                    <strong>⚠️ Iceberg v2 + Liquid Clustering Limitation</strong>
                    <br />
                    Row-level concurrency is NOT supported on Iceberg v2 with Liquid Clustering.
                    <br />
                    This may cause write conflicts for concurrent operations (DELETE, MERGE, UPDATE).
                    <br />
                    <strong>Recommendation:</strong> Use Iceberg v3 if you need row-level concurrency.
                  </Alert>
                </Grid>
              )}

              {/* Filter Condition */}
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Filter Condition (optional)"
                  value={plan.filter_condition}
                  onChange={handleChange('filter_condition')}
                  helperText="WHERE clause to filter data during refresh (e.g., active = true AND created_date >= '2020-01-01')"
                  placeholder="active = true AND created_date >= '2020-01-01'"
                  multiline
                  rows={2}
                />
              </Grid>

              {/* Table Properties */}
              <Grid item xs={12}>
                <Typography variant="subtitle2" gutterBottom sx={{ mt: 1 }}>
                  Table Properties (JSON - optional)
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={8}
                  value={plan.table_properties}
                  onChange={handleChange('table_properties')}
                  helperText={`JSON object with ${plan.table_format === 'delta' ? 'Delta Lake' : 'Apache Iceberg'} table properties. See Databricks documentation for available properties.`}
                  placeholder={plan.table_format === 'delta' ? `{
  "delta.autoOptimize.optimizeWrite": "true",
  "delta.autoOptimize.autoCompact": "auto",
  "delta.enableDeletionVectors": "true",
  "delta.columnMapping.mode": "name",
  "delta.targetFileSize": "128mb",
  "delta.parquet.compression.codec": "ZSTD"
}` : `{
  "iceberg.autoOptimize.optimizeWrite": "true",
  "iceberg.enableDeletionVectors": "true",
  "iceberg.targetFileSize": "128mb"
}`}
                  sx={{ fontFamily: 'monospace' }}
                />
              </Grid>

              {/* Staging Mode Info */}
              {plan.refresh_mode === 'staging' && (
                <Grid item xs={12}>
                  <Alert severity="info">
                    <strong>Staging Table Approach:</strong>
                    <ul style={{ margin: '8px 0', paddingLeft: '20px' }}>
                      <li>Creates a staging table with suffix <code>_staging</code></li>
                      <li>Allows validation before promoting to production</li>
                      <li>Zero downtime - readers use old table during transition</li>
                      <li>SQL includes commented-out swap commands for manual execution</li>
                    </ul>
                  </Alert>
                </Grid>
              )}
            </>
          )}

          {/* SNAPSHOT Configuration */}
          {plan.pattern_type === 'SNAPSHOT' && (
            <>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Snapshot Date Column *"
                  value={plan.snapshot_date_column}
                  onChange={handleChange('snapshot_date_column')}
                  helperText="Column name for snapshot date/timestamp (e.g., snapshot_date)"
                  placeholder="snapshot_date"
                  required
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Source Columns (optional)"
                  value={plan.source_columns}
                  onChange={handleChange('source_columns')}
                  helperText="Comma-separated columns to snapshot (leave empty for all)"
                  placeholder="customer_id,name,email"
                />
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Partition Columns (optional)"
                  value={plan.partition_columns}
                  onChange={handleChange('partition_columns')}
                  helperText="Comma-separated columns for partitioning (e.g., snapshot_date)"
                  placeholder="snapshot_date"
                />
              </Grid>
            </>
          )}

          {/* Execution Configuration */}
          <Grid item xs={12}>
            <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>
              Execution Configuration
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Autocomplete
              options={warehouses}
              getOptionLabel={(option) => typeof option === 'string' ? option : `${option.name} (${option.id})`}
              value={warehouses.find(w => w.id === plan.warehouse_id) || null}
              onChange={(_, newValue) => setPlan({ ...plan, warehouse_id: newValue?.id || '' })}
              loading={loadingWarehouses}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="SQL Warehouse"
                  helperText="Select Databricks SQL Warehouse"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loadingWarehouses ? <CircularProgress color="inherit" size={20} /> : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
            />
          </Grid>
        </Grid>
      </Paper>

      {/* Snackbar for notifications */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseSnackbar} severity={snackbar.severity} sx={{ width: '100%' }}>
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  )
}

export default PlanEditor

