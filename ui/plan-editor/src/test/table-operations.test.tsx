/**
 * Test cases for table creation and coordinated execution UI
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { vi, describe, it, expect, beforeEach } from 'vitest'
import { BrowserRouter } from 'react-router-dom'
import PlanList from '../pages/PlanList'
import * as api from '../services/api'

// Mock the API module
vi.mock('../services/api', () => ({
  api: {
    listPlans: vi.fn(),
    getPlan: vi.fn(),
    compilePlan: vi.fn(),
    checkTableExists: vi.fn(),
    createTable: vi.fn(),
    executePlan: vi.fn(),
  }
}))

// Mock react-router-dom navigate
const mockNavigate = vi.fn()
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  }
})

describe('Table Existence Checking', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('should automatically check table existence when preview is opened', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })
    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false, table: '`cat`.`sch`.`tbl`' })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => {
      expect(api.api.listPlans).toHaveBeenCalled()
    })

    // Click preview button
    const previewButton = await screen.findByText('Preview')
    fireEvent.click(previewButton)

    // Wait for table check to be called
    await waitFor(() => {
      expect(api.api.checkTableExists).toHaveBeenCalledWith('cat', 'sch', 'tbl', 'wh_123')
    })
  })

  it('should show warning when table does not exist', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    // Should show warning alert
    await waitFor(() => {
      expect(screen.getByText(/Target table does not exist/i)).toBeInTheDocument()
    })
  })

  it('should show success message when table exists', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    // Should show success alert
    await waitFor(() => {
      expect(screen.getByText(/Target table exists and is ready/i)).toBeInTheDocument()
    })
  })
})

describe('Table Creation UI', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('should show Create Table button when table does not exist', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    // Should show Create Table button
    await waitFor(() => {
      expect(screen.getByText('Create Table')).toBeInTheDocument()
    })
  })

  it('should NOT show Create Table button when table exists', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => {
      expect(screen.queryByText('Create Table')).not.toBeInTheDocument()
    })
  })

  it('should call createTable API when Create Table button is clicked', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false })
    api.api.createTable.mockResolvedValue({
      success: true,
      table: '`cat`.`sch`.`tbl`',
      statement_id: 'stmt_123',
      message: 'Table created successfully'
    })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => screen.findByText('Create Table'))
    fireEvent.click(screen.getByText('Create Table'))

    await waitFor(() => {
      expect(api.api.createTable).toHaveBeenCalledWith('plan_001', 'wh_123')
    })
  })

  it('should update table status after successful creation', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false })
    api.api.createTable.mockResolvedValue({
      success: true,
      table: '`cat`.`sch`.`tbl`',
      statement_id: 'stmt_123'
    })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    // Mock window.alert
    global.alert = vi.fn()

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => screen.findByText('Create Table'))
    fireEvent.click(screen.getByText('Create Table'))

    // Should show success alert
    await waitFor(() => {
      expect(global.alert).toHaveBeenCalledWith(expect.stringContaining('Table created successfully'))
    })

    // Create Table button should disappear
    await waitFor(() => {
      expect(screen.queryByText('Create Table')).not.toBeInTheDocument()
    })
  })
})

describe('Execute Button State', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('should disable Execute button when table does not exist', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: false })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    // Wait for modal to render
    await waitFor(() => {
      const executeButton = screen.getByText('Execute SQL')
      expect(executeButton).toBeDisabled()
    })
  })

  it('should enable Execute button when table exists', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: { plan_name: 'test_plan', owner: 'test@example.com' },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'SELECT 1;' })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => {
      const executeButton = screen.getByText('Execute SQL')
      expect(executeButton).not.toBeDisabled()
    })
  })
})

describe('Coordinated Execution', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('should call executePlan with correct parameters', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: {
        plan_name: 'test_plan',
        owner: 'test@example.com',
        version: '1.0.0'
      },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    const mockSQL = 'MERGE INTO table; INSERT INTO table;'

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: mockSQL })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.executePlan.mockResolvedValue({
      success: true,
      execution_ids: [
        { statement_number: 1, statement_id: 'stmt_1', status: 'SUCCEEDED' },
        { statement_number: 2, statement_id: 'stmt_2', status: 'SUCCEEDED' }
      ],
      total_statements: 2
    })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    global.alert = vi.fn()

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => screen.findByText('Execute SQL'))
    const executeButton = screen.getByText('Execute SQL')
    fireEvent.click(executeButton)

    await waitFor(() => {
      expect(api.api.executePlan).toHaveBeenCalledWith(
        'plan_001',
        '1.0.0',
        mockSQL,
        'wh_123',
        'test@example.com'
      )
    })
  })

  it('should display multi-statement execution results', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: {
        plan_name: 'test_plan',
        owner: 'test@example.com',
        version: '1.0.0'
      },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'MERGE; INSERT;' })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.executePlan.mockResolvedValue({
      success: true,
      execution_ids: [
        { statement_number: 1, statement_id: 'merge_123', status: 'SUCCEEDED' },
        { statement_number: 2, statement_id: 'insert_456', status: 'SUCCEEDED' }
      ],
      total_statements: 2
    })
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    global.alert = vi.fn()

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => screen.findByText('Execute SQL'))
    fireEvent.click(screen.getByText('Execute SQL'))

    await waitFor(() => {
      expect(global.alert).toHaveBeenCalledWith(
        expect.stringContaining('Successfully executed 2 statement(s)')
      )
      expect(global.alert).toHaveBeenCalledWith(
        expect.stringContaining('merge_123')
      )
      expect(global.alert).toHaveBeenCalledWith(
        expect.stringContaining('insert_456')
      )
    })
  })

  it('should handle execution failure', async () => {
    const mockPlan = {
      plan_id: 'plan_001',
      plan_metadata: {
        plan_name: 'test_plan',
        owner: 'test@example.com',
        version: '1.0.0'
      },
      target: { catalog: 'cat', schema: 'sch', table: 'tbl' },
      execution_config: { warehouse_id: 'wh_123' }
    }

    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({ success: true, sql: 'MERGE; INSERT;' })
    api.api.checkTableExists.mockResolvedValue({ exists: true })
    api.api.executePlan.mockRejectedValue(new Error('Statement 1 failed: syntax error'))
    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    await waitFor(() => screen.findByText('Execute SQL'))
    fireEvent.click(screen.getByText('Execute SQL'))

    // Should display error
    await waitFor(() => {
      expect(screen.getByText(/Statement 1 failed/i)).toBeInTheDocument()
    })
  })
})

describe('Full Integration Flow', () => {
  it('should complete full flow: check → create → execute', async () => {
    const mockPlan = {
      plan_id: 'plan_scd2',
      plan_metadata: {
        plan_name: 'scd2_plan',
        owner: 'test@example.com',
        version: '1.0.0'
      },
      target: { catalog: 'cat', schema: 'sch', table: 'customer_dim' },
      execution_config: { warehouse_id: 'wh_scd2' }
    }

    api.api.listPlans.mockResolvedValue({ plans: [mockPlan], total: 1 })
    api.api.getPlan.mockResolvedValue(mockPlan)
    api.api.compilePlan.mockResolvedValue({
      success: true,
      sql: 'MERGE INTO customer_dim; INSERT INTO customer_dim;'
    })

    // First check: table doesn't exist
    api.api.checkTableExists.mockResolvedValueOnce({ exists: false })

    api.api.createTable.mockResolvedValue({
      success: true,
      table: '`cat`.`sch`.`customer_dim`',
      statement_id: 'create_123'
    })

    api.api.executePlan.mockResolvedValue({
      success: true,
      execution_ids: [
        { statement_number: 1, statement_id: 'merge_001', status: 'SUCCEEDED' },
        { statement_number: 2, statement_id: 'insert_001', status: 'SUCCEEDED' }
      ],
      total_statements: 2
    })

    global.alert = vi.fn()

    render(
      <BrowserRouter>
        <PlanList />
      </BrowserRouter>
    )

    // Step 1: Open preview
    await waitFor(() => screen.findByText('Preview'))
    fireEvent.click(screen.getByText('Preview'))

    // Step 2: See table doesn't exist, create it
    await waitFor(() => screen.findByText('Create Table'))
    expect(screen.getByText('Execute SQL')).toBeDisabled()

    fireEvent.click(screen.getByText('Create Table'))

    // Step 3: Table created, execute button enabled
    await waitFor(() => {
      expect(api.api.createTable).toHaveBeenCalled()
    })

    // Step 4: Execute SQL
    await waitFor(() => {
      const executeButton = screen.getByText('Execute SQL')
      expect(executeButton).not.toBeDisabled()
    })

    fireEvent.click(screen.getByText('Execute SQL'))

    // Step 5: Verify coordinated execution
    await waitFor(() => {
      expect(api.api.executePlan).toHaveBeenCalled()
      expect(global.alert).toHaveBeenCalledWith(
        expect.stringContaining('Successfully executed 2 statement(s)')
      )
    })
  })
})

