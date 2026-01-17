/**
 * PlanEditor API Integration Tests
 * Tests the integration between PlanEditor and backend APIs
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { api } from '../services/api'

// Mock fetch for testing
global.fetch = vi.fn()

describe('PlanEditor API Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Plan Validation', () => {
    it('should validate a plan successfully', async () => {
      const mockPlan = {
        plan_name: 'test_plan',
        version: '1.0.0',
        owner: 'test@databricks.com',
        pattern_type: 'INCREMENTAL_APPEND',
        source_table: 'catalog.schema.source',
        target_table: 'catalog.schema.target'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ is_valid: true, errors: [] })
      })

      const result = await api.validatePlan(mockPlan)
      
      expect(result.is_valid).toBe(true)
      expect(result.errors).toEqual([])
      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining('/plans/validate'),
        expect.objectContaining({ method: 'POST' })
      )
    })

    it('should return validation errors for invalid plan', async () => {
      const mockPlan = {
        plan_name: 'Invalid Plan!',  // Invalid characters
        version: '1.0.0',
        owner: 'invalid-email',  // Invalid email
        pattern_type: 'INCREMENTAL_APPEND'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          is_valid: false,
          errors: [
            'plan_name must be lowercase with underscores only',
            'owner must be a valid email address'
          ]
        })
      })

      const result = await api.validatePlan(mockPlan)
      
      expect(result.is_valid).toBe(false)
      expect(result.errors).toHaveLength(2)
    })
  })

  describe('Plan Compilation', () => {
    it('should compile a plan to SQL', async () => {
      const mockPlan = {
        plan_name: 'test_compile',
        version: '1.0.0',
        owner: 'test@databricks.com',
        pattern_type: 'INCREMENTAL_APPEND',
        source_table: 'catalog.schema.source',
        target_table: 'catalog.schema.target'
      }

      const mockSQL = 'INSERT INTO catalog.schema.target SELECT * FROM catalog.schema.source'

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true, sql: mockSQL })
      })

      const result = await api.compilePlan(mockPlan)
      
      expect(result.success).toBe(true)
      expect(result.sql).toBe(mockSQL)
    })

    it('should handle compilation errors', async () => {
      const mockPlan = {
        plan_name: 'test_compile_error',
        pattern_type: 'INVALID_PATTERN'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        statusText: 'Bad Request',
        json: async () => ({ detail: 'Unknown pattern type' })
      })

      await expect(api.compilePlan(mockPlan)).rejects.toThrow('Unknown pattern type')
    })
  })

  describe('Plan CRUD Operations', () => {
    it('should save a new plan', async () => {
      const mockPlan = {
        plan_name: 'new_plan',
        version: '1.0.0',
        owner: 'test@databricks.com',
        pattern_type: 'SCD2'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          success: true,
          plan_id: 'generated-plan-id',
          message: 'Plan saved successfully'
        })
      })

      const result = await api.savePlan(mockPlan, 'test@databricks.com')
      
      expect(result.success).toBe(true)
      expect(result.plan_id).toBe('generated-plan-id')
    })

    it('should retrieve a plan by ID', async () => {
      const mockPlanId = 'plan-123'
      const mockPlan = {
        plan_id: mockPlanId,
        plan_name: 'existing_plan',
        version: '1.0.0',
        pattern_type: 'MERGE_UPSERT'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPlan
      })

      const result = await api.getPlan(mockPlanId)
      
      expect(result.plan_id).toBe(mockPlanId)
      expect(result.plan_name).toBe('existing_plan')
    })

    it('should list plans with filters', async () => {
      const mockPlans = [
        { plan_id: '1', plan_name: 'plan_1', pattern_type: 'INCREMENTAL_APPEND' },
        { plan_id: '2', plan_name: 'plan_2', pattern_type: 'INCREMENTAL_APPEND' }
      ]

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ plans: mockPlans, total: 2 })
      })

      const result = await api.listPlans({ pattern_type: 'INCREMENTAL_APPEND' })
      
      expect(result.plans).toHaveLength(2)
      expect(result.total).toBe(2)
    })
  })

  describe('Unity Catalog Discovery', () => {
    it('should list available catalogs', async () => {
      const mockCatalogs = [
        { name: 'catalog1', owner: 'admin' },
        { name: 'catalog2', owner: 'admin' }
      ]

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ catalogs: mockCatalogs })
      })

      const result = await api.listCatalogs()
      
      expect(result.catalogs).toHaveLength(2)
      expect(result.catalogs[0].name).toBe('catalog1')
    })

    it('should list schemas for a catalog', async () => {
      const mockSchemas = [
        { name: 'schema1', catalog: 'catalog1' },
        { name: 'schema2', catalog: 'catalog1' }
      ]

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ schemas: mockSchemas })
      })

      const result = await api.listSchemas('catalog1')
      
      expect(result.schemas).toHaveLength(2)
      expect(result.schemas[0].catalog).toBe('catalog1')
    })

    it('should list tables for a schema', async () => {
      const mockTables = [
        { name: 'table1', catalog: 'catalog1', schema: 'schema1' },
        { name: 'table2', catalog: 'catalog1', schema: 'schema1' }
      ]

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ tables: mockTables })
      })

      const result = await api.listTables('catalog1', 'schema1')
      
      expect(result.tables).toHaveLength(2)
      expect(result.tables[0].schema).toBe('schema1')
    })
  })

  describe('Pattern Discovery', () => {
    it('should list supported patterns', async () => {
      const mockPatterns = ['INCREMENTAL_APPEND', 'SCD2', 'MERGE_UPSERT', 'FULL_REPLACE']

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ patterns: mockPatterns })
      })

      const result = await api.listPatterns()
      
      expect(result.patterns).toHaveLength(4)
      expect(result.patterns).toContain('SCD2')
    })
  })

  describe('Warehouse Discovery', () => {
    it('should list SQL warehouses', async () => {
      const mockWarehouses = [
        { id: 'wh-1', name: 'Warehouse 1', state: 'RUNNING' },
        { id: 'wh-2', name: 'Warehouse 2', state: 'STOPPED' }
      ]

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({ warehouses: mockWarehouses })
      })

      const result = await api.listWarehouses()
      
      expect(result.warehouses).toHaveLength(2)
      expect(result.warehouses[0].state).toBe('RUNNING')
    })
  })

  describe('Error Handling', () => {
    it('should handle network errors', async () => {
      global.fetch = vi.fn().mockRejectedValueOnce(new Error('Network error'))

      await expect(api.listPlans()).rejects.toThrow('Network error')
    })

    it('should handle HTTP errors with detail message', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: 'Bad Request',
        json: async () => ({ detail: 'Invalid request format' })
      })

      await expect(api.validatePlan({})).rejects.toThrow('Invalid request format')
    })

    it('should handle HTTP errors without JSON response', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: async () => { throw new Error('No JSON') }
      })

      await expect(api.listPlans()).rejects.toThrow('Internal Server Error')
    })
  })
})

