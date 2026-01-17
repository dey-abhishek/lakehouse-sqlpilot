import { useState, useEffect } from 'react'
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  Grid,
  Divider,
  Alert,
  IconButton,
  Chip,
  Stack,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import SaveIcon from '@mui/icons-material/Save'
import RefreshIcon from '@mui/icons-material/Refresh'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import WarningIcon from '@mui/icons-material/Warning'
import { useConfig, WorkspaceConfig, defaultConfig } from '../context/ConfigContext'

export default function Settings() {
  const { config: globalConfig, updateConfig, isConfigured } = useConfig()
  const [config, setConfig] = useState<WorkspaceConfig>(globalConfig)
  const [originalConfig, setOriginalConfig] = useState<WorkspaceConfig>(globalConfig)
  const [saved, setSaved] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testResults, setTestResults] = useState<{ [key: string]: boolean }>({})
  const [hasChanges, setHasChanges] = useState(false)

  useEffect(() => {
    // Sync with global config
    setConfig(globalConfig)
    setOriginalConfig(globalConfig)
  }, [globalConfig])

  useEffect(() => {
    // Check if config has changed
    setHasChanges(JSON.stringify(config) !== JSON.stringify(originalConfig))
  }, [config, originalConfig])

  const handleChange = (field: keyof WorkspaceConfig, value: any) => {
    setConfig({ ...config, [field]: value })
    setSaved(false)
  }

  const handleSave = () => {
    // Save to global config context (which saves to localStorage)
    updateConfig(config)
    setOriginalConfig(config)
    setSaved(true)
    setHasChanges(false)
    
    setTimeout(() => setSaved(false), 3000)
  }

  const handleReset = () => {
    setConfig(originalConfig)
    setHasChanges(false)
  }

  const testConnection = async () => {
    setTesting(true)
    setTestResults({})
    
    // Simulate connection tests (replace with actual API calls)
    const tests = {
      workspace: async () => {
        // Test workspace connection
        await new Promise(resolve => setTimeout(resolve, 500))
        return config.workspaceUrl && config.token
      },
      catalog: async () => {
        // Test catalog access
        await new Promise(resolve => setTimeout(resolve, 500))
        return config.defaultCatalog !== ''
      },
      warehouse: async () => {
        // Test warehouse access
        await new Promise(resolve => setTimeout(resolve, 500))
        return config.defaultWarehouseId !== ''
      },
      genie: async () => {
        // Test Genie Space access
        await new Promise(resolve => setTimeout(resolve, 500))
        return !config.genieEnabled || config.genieSpaceId !== ''
      },
    }

    const results: { [key: string]: boolean } = {}
    for (const [key, testFn] of Object.entries(tests)) {
      const result = await testFn()
      results[key] = typeof result === 'boolean' ? result : Boolean(result)
    }
    
    setTestResults(results)
    setTesting(false)
  }

  const isConfigComplete = () => {
    return (
      config.workspaceUrl &&
      config.workspaceId &&
      config.token &&
      config.defaultCatalog &&
      config.defaultSchema &&
      config.defaultWarehouseId &&
      config.ownerEmail
    )
  }

  return (
    <Box sx={{ maxWidth: 1200, mx: 'auto' }}>
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" sx={{ fontWeight: 700 }}>
          Workspace Configuration
        </Typography>
        
        {saved && (
          <Alert severity="success" icon={<CheckCircleIcon />}>
            Configuration saved successfully!
          </Alert>
        )}
      </Stack>

      {!isConfigComplete() && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Please complete all required configuration fields to use SQLPilot
        </Alert>
      )}

      <Paper sx={{ p: 3, mb: 3 }}>
        <Accordion defaultExpanded>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Databricks Connection
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Workspace URL"
                  placeholder="https://your-workspace.cloud.databricks.com"
                  value={config.workspaceUrl}
                  onChange={(e) => handleChange('workspaceUrl', e.target.value)}
                  required
                  helperText="Your Databricks workspace URL"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Workspace ID"
                  placeholder="1234567890123456"
                  value={config.workspaceId}
                  onChange={(e) => handleChange('workspaceId', e.target.value)}
                  required
                  helperText="Numeric workspace identifier"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Personal Access Token"
                  type="password"
                  placeholder="dapi..."
                  value={config.token}
                  onChange={(e) => handleChange('token', e.target.value)}
                  required
                  helperText="Your Databricks PAT (never shared)"
                />
              </Grid>
            </Grid>
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Unity Catalog
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Default Catalog"
                  placeholder="my_catalog"
                  value={config.defaultCatalog}
                  onChange={(e) => handleChange('defaultCatalog', e.target.value)}
                  required
                  helperText="Unity Catalog to use for plans"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Default Schema"
                  placeholder="my_schema"
                  value={config.defaultSchema}
                  onChange={(e) => handleChange('defaultSchema', e.target.value)}
                  required
                  helperText="Default schema within catalog"
                />
              </Grid>
              <Grid item xs={12}>
                <Alert severity="info">
                  <strong>Full Path:</strong> {config.defaultCatalog || '(catalog)'}.{config.defaultSchema || '(schema)'}
                </Alert>
              </Grid>
            </Grid>
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              SQL Warehouse
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Default SQL Warehouse ID"
                  placeholder="abc123def456"
                  value={config.defaultWarehouseId}
                  onChange={(e) => handleChange('defaultWarehouseId', e.target.value)}
                  required
                  helperText="SQL Warehouse ID for plan execution"
                />
              </Grid>
              <Grid item xs={12}>
                <Alert severity="info">
                  Find your warehouse ID: SQL → Warehouses → Select warehouse → Connection Details
                </Alert>
              </Grid>
            </Grid>
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Genie Integration (Optional)
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <Stack direction="row" spacing={2} alignItems="center">
                  <Typography>Enable Genie Handoff</Typography>
                  <Chip
                    label={config.genieEnabled ? 'Enabled' : 'Disabled'}
                    color={config.genieEnabled ? 'success' : 'default'}
                    onClick={() => handleChange('genieEnabled', !config.genieEnabled)}
                    sx={{ cursor: 'pointer' }}
                  />
                </Stack>
              </Grid>
              {config.genieEnabled && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Genie Space ID"
                      placeholder="space-..."
                      value={config.genieSpaceId}
                      onChange={(e) => handleChange('genieSpaceId', e.target.value)}
                      helperText="Genie Space ID for handoff workflow"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <Alert severity="info">
                      Create a Genie Space: AI/BI → Genie → Create New Space
                    </Alert>
                  </Grid>
                </>
              )}
            </Grid>
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              User Preferences
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Owner Email"
                  type="email"
                  placeholder="user@company.com"
                  value={config.ownerEmail}
                  onChange={(e) => handleChange('ownerEmail', e.target.value)}
                  required
                  helperText="Your email (used in plan metadata)"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="Default Timeout (seconds)"
                  type="number"
                  value={config.defaultTimeout}
                  onChange={(e) => handleChange('defaultTimeout', parseInt(e.target.value))}
                  helperText="Execution timeout"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="Default Max Retries"
                  type="number"
                  value={config.defaultMaxRetries}
                  onChange={(e) => handleChange('defaultMaxRetries', parseInt(e.target.value))}
                  helperText="Max retry attempts"
                />
              </Grid>
            </Grid>
          </AccordionDetails>
        </Accordion>
      </Paper>

      {Object.keys(testResults).length > 0 && (
        <Paper sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
            Connection Test Results
          </Typography>
          <Stack spacing={1}>
            {Object.entries(testResults).map(([key, success]) => (
              <Stack key={key} direction="row" alignItems="center" spacing={1}>
                {success ? (
                  <CheckCircleIcon color="success" />
                ) : (
                  <WarningIcon color="error" />
                )}
                <Typography>
                  {key.charAt(0).toUpperCase() + key.slice(1)}: {success ? 'Connected' : 'Failed'}
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      )}

      <Stack direction="row" spacing={2}>
        <Button
          variant="contained"
          startIcon={<SaveIcon />}
          onClick={handleSave}
          disabled={!hasChanges || !isConfigComplete()}
          size="large"
        >
          Save Configuration
        </Button>
        <Button
          variant="outlined"
          onClick={testConnection}
          disabled={testing || !isConfigComplete()}
          size="large"
        >
          Test Connection
        </Button>
        {hasChanges && (
          <Button
            variant="text"
            onClick={handleReset}
            size="large"
          >
            Reset Changes
          </Button>
        )}
      </Stack>

      <Box sx={{ mt: 4, p: 2, bgcolor: 'background.default', borderRadius: 1 }}>
        <Typography variant="body2" color="text.secondary">
          <strong>Security Note:</strong> Your configuration is stored locally in your browser.
          Credentials are never sent to external servers. Always use secure tokens and follow
          your organization's security policies.
        </Typography>
      </Box>
    </Box>
  )
}

