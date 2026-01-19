import { createContext, useContext, useState, useEffect, ReactNode } from 'react'

export interface WorkspaceConfig {
  // Databricks Connection
  workspaceUrl: string
  workspaceId: string
  token: string
  
  // Unity Catalog
  defaultCatalog: string
  defaultSchema: string
  
  // SQL Warehouse
  defaultWarehouseId: string
  
  // Genie Integration
  genieSpaceId: string
  genieEnabled: boolean
  
  // User Preferences
  ownerEmail: string
  defaultTimeout: number
  defaultMaxRetries: number
}

export const defaultConfig: WorkspaceConfig = {
  workspaceUrl: '',
  workspaceId: '',
  token: '',
  defaultCatalog: '',
  defaultSchema: '',
  defaultWarehouseId: '',
  genieSpaceId: '',
  genieEnabled: false,
  ownerEmail: '',
  defaultTimeout: 3600,
  defaultMaxRetries: 3,
}

interface ConfigContextType {
  config: WorkspaceConfig
  updateConfig: (config: WorkspaceConfig) => void
  isConfigured: boolean
}

const ConfigContext = createContext<ConfigContextType | undefined>(undefined)

export function ConfigProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<WorkspaceConfig>(defaultConfig)
  const [isConfigured, setIsConfigured] = useState(false)

  useEffect(() => {
    // Load config from localStorage on mount
    const savedConfig = localStorage.getItem('sqlpilot_config')
    if (savedConfig) {
      try {
        const parsed = JSON.parse(savedConfig)
        setConfig(parsed)
        setIsConfigured(checkIfConfigured(parsed))
      } catch (error) {
        console.error('Failed to parse saved config:', error)
      }
    }
  }, [])

  const checkIfConfigured = (cfg: WorkspaceConfig): boolean => {
    return !!(
      cfg.workspaceUrl &&
      cfg.workspaceId &&
      cfg.token &&
      cfg.defaultCatalog &&
      cfg.defaultSchema &&
      cfg.defaultWarehouseId &&
      cfg.ownerEmail
    )
  }

  const updateConfig = (newConfig: WorkspaceConfig) => {
    setConfig(newConfig)
    setIsConfigured(checkIfConfigured(newConfig))
    localStorage.setItem('sqlpilot_config', JSON.stringify(newConfig))
  }

  return (
    <ConfigContext.Provider value={{ config, updateConfig, isConfigured }}>
      {children}
    </ConfigContext.Provider>
  )
}

export function useConfig() {
  const context = useContext(ConfigContext)
  if (context === undefined) {
    throw new Error('useConfig must be used within a ConfigProvider')
  }
  return context
}


