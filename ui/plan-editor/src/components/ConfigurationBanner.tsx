import { Alert, Button, Box } from '@mui/material'
import SettingsIcon from '@mui/icons-material/Settings'
import { useNavigate } from 'react-router-dom'
import { useConfig } from '../context/ConfigContext'

export default function ConfigurationBanner() {
  const { isConfigured } = useConfig()
  const navigate = useNavigate()

  if (isConfigured) {
    return null
  }

  return (
    <Box sx={{ mb: 3 }}>
      <Alert 
        severity="warning" 
        action={
          <Button 
            color="inherit" 
            size="small" 
            startIcon={<SettingsIcon />}
            onClick={() => navigate('/settings')}
          >
            Configure Now
          </Button>
        }
      >
        <strong>Configuration Required:</strong> Please configure your Databricks workspace connection, 
        Unity Catalog, and SQL Warehouse in Settings to use SQLPilot.
      </Alert>
    </Box>
  )
}

