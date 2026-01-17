import { Routes, Route, useNavigate, useLocation } from 'react-router-dom'
import { Box, AppBar, Toolbar, Typography, Container, Button, Stack, Alert } from '@mui/material'
import StorageIcon from '@mui/icons-material/Storage'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import DescriptionIcon from '@mui/icons-material/Description'
import SettingsIcon from '@mui/icons-material/Settings'
import { useEffect, useState } from 'react'
import PlanList from './pages/PlanList'
import PlanEditor from './pages/PlanEditor'
import ExecutionDashboard from './pages/ExecutionDashboard'
import PreviewPane from './pages/PreviewPane'
import Settings from './pages/Settings'
import { enforceHttps, validateSecureConnection } from './utils/https'

function App() {
  const navigate = useNavigate()
  const location = useLocation()
  const [securityWarnings, setSecurityWarnings] = useState<string[]>([])

  useEffect(() => {
    // Enforce HTTPS in production
    enforceHttps()
    
    // Validate secure connection
    const { isSecure, warnings } = validateSecureConnection()
    if (warnings.length > 0) {
      setSecurityWarnings(warnings)
    }
  }, [])

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh', bgcolor: 'background.default' }}>
      {/* Security Warnings */}
      {securityWarnings.length > 0 && (
        <Alert severity="warning" onClose={() => setSecurityWarnings([])}>
          <strong>Security Warning:</strong>
          {securityWarnings.map((warning, idx) => (
            <div key={idx}>{warning}</div>
          ))}
        </Alert>
      )}
      
      <AppBar 
        position="static" 
        color="secondary"
        elevation={0}
        sx={{ 
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
      >
        <Toolbar>
          <StorageIcon sx={{ mr: 2, fontSize: 28 }} />
          <Typography 
            variant="h6" 
            component="div" 
            sx={{ 
              flexGrow: 1,
              fontWeight: 700,
              letterSpacing: '-0.01em',
            }}
          >
            Lakehouse SQLPilot
          </Typography>
          
          <Stack direction="row" spacing={1}>
            <Button
              color="inherit"
              startIcon={<DescriptionIcon />}
              onClick={() => navigate('/')}
              sx={{
                color: location.pathname === '/' ? 'primary.main' : 'inherit',
                fontWeight: location.pathname === '/' ? 600 : 400,
              }}
            >
              Plans
            </Button>
            <Button
              color="inherit"
              startIcon={<PlayCircleIcon />}
              onClick={() => navigate('/executions')}
              sx={{
                color: location.pathname === '/executions' ? 'primary.main' : 'inherit',
                fontWeight: location.pathname === '/executions' ? 600 : 400,
              }}
            >
              Executions
            </Button>
            <Button
              color="inherit"
              startIcon={<SettingsIcon />}
              onClick={() => navigate('/settings')}
              sx={{
                color: location.pathname === '/settings' ? 'primary.main' : 'inherit',
                fontWeight: location.pathname === '/settings' ? 600 : 400,
              }}
            >
              Settings
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>
      
      <Container 
        maxWidth="xl" 
        sx={{ 
          mt: 4, 
          mb: 4, 
          flexGrow: 1,
          px: 3,
        }}
      >
        <Routes>
          <Route path="/" element={<PlanList />} />
          <Route path="/plans/new" element={<PlanEditor />} />
          <Route path="/plans/:id" element={<PlanEditor />} />
          <Route path="/plans/:id/preview" element={<PreviewPane />} />
          <Route path="/executions" element={<ExecutionDashboard />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </Container>
      
      <Box 
        component="footer" 
        sx={{ 
          py: 2, 
          px: 3, 
          mt: 'auto',
          backgroundColor: 'background.paper',
          borderTop: '1px solid',
          borderColor: 'divider',
        }}
      >
        <Typography variant="body2" color="text.secondary" align="center">
          Lakehouse SQLPilot v1.0 • Governed SQL Execution for Databricks
        </Typography>
      </Box>
    </Box>
  )
}

export default App

