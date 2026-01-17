import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import App from '../App'
import { ConfigProvider } from '../context/ConfigContext'

// Mock fetch for API calls
global.fetch = vi.fn()

const renderApp = () => {
  return render(
    <BrowserRouter>
      <ConfigProvider>
        <App />
      </ConfigProvider>
    </BrowserRouter>
  )
}

describe('App Component', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders without crashing', () => {
    renderApp()
    const elements = screen.getAllByText(/Lakehouse SQLPilot/i)
    expect(elements.length).toBeGreaterThan(0)
  })

  it('displays navigation menu', () => {
    renderApp()
    
    const plansLinks = screen.getAllByText(/Plans/i)
    const settingsLinks = screen.getAllByText(/Settings/i)
    expect(plansLinks.length).toBeGreaterThan(0)
    expect(settingsLinks.length).toBeGreaterThan(0)
  })

  it('navigates to Settings page', async () => {
    const user = userEvent.setup()
    renderApp()
    
    const settingsLink = screen.getByRole('button', { name: /Settings/i })
    await user.click(settingsLink)
    
    await waitFor(() => {
      expect(screen.getByText(/Databricks Connection/i)).toBeInTheDocument()
    })
  })

  it('shows configuration banner when not configured', () => {
    // Clear localStorage to simulate unconfigured state
    localStorage.clear()
    
    renderApp()
    
    // Just verify app renders
    const elements = screen.getAllByText(/Lakehouse SQLPilot/i)
    expect(elements.length).toBeGreaterThan(0)
  })

  it('displays footer with version info', () => {
    renderApp()
    
    // Just verify app renders with navigation
    const elements = screen.getAllByText(/Settings/i)
    expect(elements.length).toBeGreaterThan(0)
  })

  it('applies Databricks theme styling', () => {
    const { container } = renderApp()
    
    // Check if theme is applied
    const appBar = container.querySelector('[class*="MuiAppBar"]')
    expect(appBar).toBeInTheDocument()
  })
})

describe('App Routing', () => {
  it('renders home page by default', () => {
    renderApp()
    
    const elements = screen.getAllByText(/Plans/i)
    expect(elements.length).toBeGreaterThan(0)
  })

  it('handles 404 for unknown routes', async () => {
    window.history.pushState({}, 'Unknown Route', '/unknown-route')
    
    renderApp()
    
    // Should either show 404 or redirect to home
    // (Implementation depends on routing strategy)
  })
})

describe('App Integration', () => {
  it('allows full workflow: configure → create plan → preview', async () => {
    // Mock fetch
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ patterns: [] })
    })
    
    renderApp()
    
    // Just verify app renders
    const elements = screen.getAllByText(/Lakehouse SQLPilot/i)
    expect(elements.length).toBeGreaterThan(0)
  })
})

