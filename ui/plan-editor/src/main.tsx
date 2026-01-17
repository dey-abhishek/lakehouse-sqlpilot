import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider, createTheme } from '@mui/material/styles'
import CssBaseline from '@mui/material/CssBaseline'
import { ConfigProvider } from './context/ConfigContext'

// Import Inter font
import '@fontsource/inter/300.css'
import '@fontsource/inter/400.css'
import '@fontsource/inter/500.css'
import '@fontsource/inter/600.css'
import '@fontsource/inter/700.css'

import './index.css'
import App from './App'

// Databricks Design System Colors
const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#FF3621', // Databricks Red
      light: '#FF6B59',
      dark: '#CC2B1A',
      contrastText: '#FFFFFF',
    },
    secondary: {
      main: '#1B3139', // Databricks Dark Blue/Charcoal
      light: '#2C4A56',
      dark: '#0F1C22',
      contrastText: '#FFFFFF',
    },
    success: {
      main: '#00A972', // Databricks Green
      light: '#33BA8E',
      dark: '#00875B',
    },
    info: {
      main: '#0099E0', // Databricks Blue
      light: '#33ADE6',
      dark: '#007AB3',
    },
    warning: {
      main: '#FF9F3A', // Databricks Orange
      light: '#FFB261',
      dark: '#CC7F2E',
    },
    error: {
      main: '#FF3621',
    },
    background: {
      default: '#F6F6F6',
      paper: '#FFFFFF',
    },
    text: {
      primary: '#1B3139',
      secondary: '#6B7B83',
    },
  },
  typography: {
    fontFamily: '"Inter", "Segoe UI", "Roboto", "Helvetica Neue", Arial, sans-serif',
    h1: {
      fontWeight: 700,
      fontSize: '2.5rem',
      letterSpacing: '-0.02em',
    },
    h2: {
      fontWeight: 700,
      fontSize: '2rem',
      letterSpacing: '-0.01em',
    },
    h3: {
      fontWeight: 600,
      fontSize: '1.75rem',
    },
    h4: {
      fontWeight: 600,
      fontSize: '1.5rem',
    },
    h5: {
      fontWeight: 600,
      fontSize: '1.25rem',
    },
    h6: {
      fontWeight: 600,
      fontSize: '1rem',
    },
    button: {
      textTransform: 'none',
      fontWeight: 600,
    },
  },
  shape: {
    borderRadius: 4,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 4,
          padding: '8px 16px',
          fontSize: '0.875rem',
          fontWeight: 600,
        },
        contained: {
          boxShadow: 'none',
          '&:hover': {
            boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
        },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: {
          boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          fontWeight: 500,
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: {
          fontWeight: 600,
          backgroundColor: '#F6F6F6',
        },
      },
    },
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </ThemeProvider>
    </ConfigProvider>
  </React.StrictMode>,
)

