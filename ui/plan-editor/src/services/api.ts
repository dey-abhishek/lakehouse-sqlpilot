/**
 * API Service Layer for Lakehouse SQLPilot
 * Handles all backend API communication
 */

// Use environment variable or default to relative path (works for both dev and prod)
// In production, this will use the same protocol/host as the frontend
// In development, set VITE_API_URL to http://localhost:8080/api/v1 if needed
const getApiBaseUrl = () => {
  // Check for environment variable first
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL
  }
  
  // In production (deployed), use relative path which will use HTTPS automatically
  if (import.meta.env.PROD) {
    return '/api/v1'
  }
  
  // In development, use localhost backend (default port 8001)
  // This allows local dev with http while prod uses https
  const protocol = window.location.protocol
  return `${protocol}//localhost:8001/api/v1`
}

const API_BASE_URL = getApiBaseUrl()

export interface Plan {
  plan_id?: string
  plan_name: string
  version?: string
  pattern_type: string
  owner: string
  description?: string
  source_catalog?: string
  source_schema?: string
  source_table?: string
  target_catalog?: string
  target_schema?: string
  target_table?: string
  warehouse_id?: string
  created_at?: string
  status?: 'active' | 'draft' | 'archived'
  config?: Record<string, any>
}

export interface ValidationResult {
  is_valid: boolean
  errors: string[]
}

export interface CompilationResult {
  success: boolean
  sql: string
}

export interface PreviewResult {
  is_valid: boolean
  sql: string
  warnings: string[]
  permissions: {
    has_permissions: boolean
    violations: string[]
  }
  impact_analysis: {
    source_table: string
    target_table: string
    operation: string
    operation_type?: string
    is_destructive: boolean
    estimated_risk: 'low' | 'medium' | 'high'
  }
  sample_data?: {
    columns: string[]
    rows: any[][]
  }
}

export interface ExecutionResult {
  success: boolean
  execution_id?: string // Single statement execution (legacy)
  execution_ids?: Array<{
    statement_number: number
    statement_id: string
    status: string
  }> // Multi-statement execution
  total_statements?: number
  message: string
}

export interface ExecutionStatus {
  execution_id: string
  state: string
  query_id?: string
  error_message?: string
  rows_affected?: number
  started_at?: string
  completed_at?: string
}

export interface AgentSuggestion {
  success: boolean
  suggested_plan: Plan
  confidence: number
  explanation: string
  warnings: string[]
}

export interface Catalog {
  name: string
  comment?: string
  owner?: string
}

export interface Schema {
  name: string
  catalog: string
  owner?: string
}

export interface Table {
  name: string
  catalog: string
  schema: string
  table_type?: string
}

export interface Warehouse {
  id: string
  name: string
  state?: string
  size?: string
}

class APIService {
  private baseURL: string

  constructor(baseURL: string = API_BASE_URL) {
    this.baseURL = baseURL
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseURL}${endpoint}`
    const config: RequestInit = {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0',
        ...options.headers,
      },
    }

    try {
      const response = await fetch(url, config)
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: response.statusText }))
        // Preserve the full error structure for better error messages
        const error: any = new Error(
          typeof errorData.detail === 'string' 
            ? errorData.detail 
            : JSON.stringify(errorData.detail)
        )
        error.detail = errorData.detail
        error.status = response.status
        throw error
      }

      return await response.json()
    } catch (error) {
      console.error('API request failed:', error)
      throw error
    }
  }

  // Plan Management
  async listPlans(filters?: { owner?: string; pattern_type?: string }): Promise<{ plans: Plan[]; total: number }> {
    const params = new URLSearchParams()
    if (filters?.owner) params.append('owner', filters.owner)
    if (filters?.pattern_type) params.append('pattern_type', filters.pattern_type)
    
    const query = params.toString() ? `?${params.toString()}` : ''
    return this.request<{ plans: Plan[]; total: number }>(`/plans${query}`)
  }

  async getPlan(planId: string): Promise<Plan> {
    // Add cache-busting query parameter to ensure fresh data
    const cacheBuster = `_=${Date.now()}`
    return this.request<Plan>(`/plans/${planId}?${cacheBuster}`)
  }

  async savePlan(plan: Plan, user: string): Promise<{ success: boolean; plan_id: string; message: string }> {
    return this.request<{ success: boolean; plan_id: string; message: string }>('/plans', {
      method: 'POST',
      body: JSON.stringify({ plan, user }),
    })
  }

  // Plan Validation & Compilation
  async validatePlan(plan: Plan): Promise<ValidationResult> {
    return this.request<ValidationResult>('/plans/validate', {
      method: 'POST',
      body: JSON.stringify({ plan }),
    })
  }

  async compilePlan(plan: Plan, context?: Record<string, any>): Promise<CompilationResult> {
    return this.request<CompilationResult>('/plans/compile', {
      method: 'POST',
      body: JSON.stringify({ plan, context }),
    })
  }

  async previewPlan(
    plan: Plan,
    user: string,
    warehouseId: string,
    includeSampleData: boolean = true
  ): Promise<PreviewResult> {
    return this.request<PreviewResult>('/plans/preview', {
      method: 'POST',
      body: JSON.stringify({
        plan,
        user,
        warehouse_id: warehouseId,
        include_sample_data: includeSampleData,
      }),
    })
  }

  // Execution
  async executePlan(
    planId: string,
    planVersion: string,
    sql: string,
    warehouseId: string,
    executorUser: string,
    timeoutSeconds: number = 3600
  ): Promise<ExecutionResult> {
    return this.request<ExecutionResult>('/plans/execute', {
      method: 'POST',
      body: JSON.stringify({
        plan_id: planId,
        plan_version: planVersion,
        sql,
        warehouse_id: warehouseId,
        executor_user: executorUser,
        timeout_seconds: timeoutSeconds,
      }),
    })
  }

  async getExecutionStatus(executionId: string): Promise<ExecutionStatus> {
    return this.request<ExecutionStatus>(`/executions/${executionId}`)
  }

  // Agent Integration
  async getAgentSuggestion(
    intent: string,
    user: string,
    context?: Record<string, any>
  ): Promise<AgentSuggestion> {
    return this.request<AgentSuggestion>('/agent/suggest', {
      method: 'POST',
      body: JSON.stringify({ intent, user, context }),
    })
  }

  // Unity Catalog Discovery
  async listCatalogs(): Promise<{ catalogs: Catalog[] }> {
    return this.request<{ catalogs: Catalog[] }>('/catalogs')
  }

  async listSchemas(catalog: string): Promise<{ schemas: Schema[] }> {
    return this.request<{ schemas: Schema[] }>(`/catalogs/${catalog}/schemas`)
  }

  async listTables(catalog: string, schema: string): Promise<{ tables: Table[] }> {
    return this.request<{ tables: Table[] }>(`/catalogs/${catalog}/schemas/${schema}/tables`)
  }

  // Warehouses
  async listWarehouses(): Promise<{ warehouses: Warehouse[] }> {
    return this.request<{ warehouses: Warehouse[] }>('/warehouses')
  }

  // Patterns
  async listPatterns(): Promise<{ patterns: string[] }> {
    return this.request<{ patterns: string[] }>('/patterns')
  }

  // Table Operations
  async checkTableExists(catalog: string, schema: string, table: string, warehouseId: string): Promise<{ exists: boolean; table: string }> {
    return this.request<{ exists: boolean; table: string }>('/tables/check', {
      method: 'POST',
      body: JSON.stringify({ catalog, schema, table, warehouse_id: warehouseId }),
    })
  }

  async createTable(planId: string, warehouseId: string): Promise<{ success: boolean; table: string; statement_id: string; message: string }> {
    return this.request<{ success: boolean; table: string; statement_id: string; message: string }>('/tables/create', {
      method: 'POST',
      body: JSON.stringify({ plan_id: planId, warehouse_id: warehouseId }),
    })
  }

  async deleteTable(catalog: string, schema: string, table: string, warehouseId: string): Promise<{ success: boolean; table: string; statement_id: string; message: string }> {
    return this.request<{ success: boolean; table: string; statement_id: string; message: string }>('/tables/delete', {
      method: 'POST',
      body: JSON.stringify({ catalog, schema, table, warehouse_id: warehouseId }),
    })
  }

  // Execution History
  async listExecutions(params?: { status?: string; executor_user?: string; limit?: number; offset?: number }): Promise<{ executions: any[]; total: number }> {
    const queryParams = new URLSearchParams()
    if (params?.status) queryParams.append('status', params.status)
    if (params?.executor_user) queryParams.append('executor_user', params.executor_user)
    if (params?.limit) queryParams.append('limit', params.limit.toString())
    if (params?.offset) queryParams.append('offset', params.offset.toString())
    
    const url = `/executions${queryParams.toString() ? `?${queryParams.toString()}` : ''}`
    return this.request<{ executions: any[]; total: number }>(url)
  }

  async getExecution(executionId: number): Promise<any> {
    return this.request(`/executions/${executionId}`)
  }

  // Health Check
  async healthCheck(): Promise<{ status: string; service: string; version: string }> {
    return this.request<{ status: string; service: string; version: string }>(
      '/health',
      { method: 'GET' }
    )
  }
}

// Export singleton instance
export const api = new APIService()

// Export class for testing
export default APIService

