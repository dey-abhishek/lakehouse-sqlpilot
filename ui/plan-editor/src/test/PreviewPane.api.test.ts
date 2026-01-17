/**
 * PreviewPane API Integration Tests
 * Tests the integration between PreviewPane and backend preview/execution APIs
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { api } from '../services/api'

// Mock fetch for testing
global.fetch = vi.fn()

describe('PreviewPane API Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Plan Preview', () => {
    it('should generate preview for a valid plan', async () => {
      const mockPlan = {
        plan_name: 'test_preview',
        version: '1.0.0',
        owner: 'test@databricks.com',
        pattern_type: 'INCREMENTAL_APPEND',
        source_table: 'catalog.schema.source',
        target_table: 'catalog.schema.target'
      }

      const mockPreview = {
        is_valid: true,
        sql: 'INSERT INTO catalog.schema.target SELECT * FROM catalog.schema.source',
        warnings: [],
        permissions: {
          has_permissions: true,
          violations: []
        },
        impact_analysis: {
          source_table: 'catalog.schema.source',
          target_table: 'catalog.schema.target',
          operation: 'APPEND',
          operation_type: 'INCREMENTAL_APPEND',
          is_destructive: false,
          estimated_risk: 'low' as const
        },
        sample_data: {
          columns: ['id', 'name', 'timestamp'],
          rows: [
            ['1', 'Test 1', '2026-01-16 10:00:00'],
            ['2', 'Test 2', '2026-01-16 11:00:00']
          ]
        }
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPreview
      })

      const result = await api.previewPlan(
        mockPlan,
        'test@databricks.com',
        'wh-123',
        true
      )
      
      expect(result.is_valid).toBe(true)
      expect(result.sql).toContain('INSERT INTO')
      expect(result.impact_analysis.is_destructive).toBe(false)
      expect(result.sample_data?.rows).toHaveLength(2)
    })

    it('should show permission violations in preview', async () => {
      const mockPlan = {
        plan_name: 'test_no_permissions',
        pattern_type: 'FULL_REPLACE'
      }

      const mockPreview = {
        is_valid: false,
        sql: '',
        warnings: ['Missing permissions'],
        permissions: {
          has_permissions: false,
          violations: [
            'User lacks SELECT permission on source',
            'User lacks INSERT permission on target'
          ]
        },
        impact_analysis: {
          source_table: '',
          target_table: '',
          operation: '',
          is_destructive: false,
          estimated_risk: 'high' as const
        }
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPreview
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123')
      
      expect(result.is_valid).toBe(false)
      expect(result.permissions.has_permissions).toBe(false)
      expect(result.permissions.violations).toHaveLength(2)
    })

    it('should show high risk for destructive operations', async () => {
      const mockPlan = {
        plan_name: 'test_full_replace',
        pattern_type: 'FULL_REPLACE',
        target_table: 'prod_catalog.critical.customer_data'
      }

      const mockPreview = {
        is_valid: true,
        sql: 'CREATE OR REPLACE TABLE prod_catalog.critical.customer_data AS SELECT * FROM source',
        warnings: [
          'This is a FULL REPLACE operation',
          'This affects a PRODUCTION catalog',
          'All existing data will be replaced'
        ],
        permissions: {
          has_permissions: true,
          violations: []
        },
        impact_analysis: {
          source_table: 'dev_catalog.staging.customer_data',
          target_table: 'prod_catalog.critical.customer_data',
          operation: 'REPLACE',
          operation_type: 'FULL_REPLACE',
          is_destructive: true,
          estimated_risk: 'high' as const
        }
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPreview
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123')
      
      expect(result.impact_analysis.is_destructive).toBe(true)
      expect(result.impact_analysis.estimated_risk).toBe('high')
      expect(result.warnings.length).toBeGreaterThan(0)
    })

    it('should include sample data when requested', async () => {
      const mockPlan = {
        plan_name: 'test_with_sample',
        pattern_type: 'INCREMENTAL_APPEND'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          is_valid: true,
          sql: 'INSERT INTO target SELECT * FROM source',
          warnings: [],
          permissions: { has_permissions: true, violations: [] },
          impact_analysis: {
            source_table: 'source',
            target_table: 'target',
            operation: 'APPEND',
            is_destructive: false,
            estimated_risk: 'low' as const
          },
          sample_data: {
            columns: ['col1', 'col2'],
            rows: [['val1', 'val2']]
          }
        })
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123', true)
      
      expect(result.sample_data).toBeDefined()
      expect(result.sample_data?.columns).toHaveLength(2)
    })

    it('should work without sample data', async () => {
      const mockPlan = {
        plan_name: 'test_no_sample',
        pattern_type: 'MERGE_UPSERT'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          is_valid: true,
          sql: 'MERGE INTO target USING source',
          warnings: [],
          permissions: { has_permissions: true, violations: [] },
          impact_analysis: {
            source_table: 'source',
            target_table: 'target',
            operation: 'MERGE',
            is_destructive: false,
            estimated_risk: 'low' as const
          }
        })
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123', false)
      
      expect(result.sample_data).toBeUndefined()
    })
  })

  describe('Plan Execution', () => {
    it('should execute a plan successfully', async () => {
      const mockExecutionResult = {
        success: true,
        execution_id: 'exec-12345',
        message: 'Execution started successfully'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockExecutionResult
      })

      const result = await api.executePlan(
        'plan-123',
        '1.0.0',
        'INSERT INTO target SELECT * FROM source',
        'wh-123',
        'test@databricks.com',
        3600
      )
      
      expect(result.success).toBe(true)
      expect(result.execution_id).toBe('exec-12345')
      expect(result.message).toContain('successfully')
    })

    it('should handle execution failures', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        statusText: 'Internal Server Error',
        json: async () => ({ detail: 'Warehouse not available' })
      })

      await expect(
        api.executePlan('plan-123', '1.0.0', 'SELECT 1', 'wh-123', 'test@databricks.com')
      ).rejects.toThrow('Warehouse not available')
    })
  })

  describe('Execution Status Tracking', () => {
    it('should get execution status - running', async () => {
      const mockStatus = {
        execution_id: 'exec-123',
        state: 'RUNNING',
        query_id: 'query-456',
        error_message: null,
        rows_affected: null,
        started_at: '2026-01-16T10:00:00Z',
        completed_at: null
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockStatus
      })

      const result = await api.getExecutionStatus('exec-123')
      
      expect(result.state).toBe('RUNNING')
      expect(result.started_at).toBeDefined()
      expect(result.completed_at).toBeNull()
    })

    it('should get execution status - succeeded', async () => {
      const mockStatus = {
        execution_id: 'exec-123',
        state: 'SUCCEEDED',
        query_id: 'query-456',
        error_message: null,
        rows_affected: 1500,
        started_at: '2026-01-16T10:00:00Z',
        completed_at: '2026-01-16T10:05:30Z'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockStatus
      })

      const result = await api.getExecutionStatus('exec-123')
      
      expect(result.state).toBe('SUCCEEDED')
      expect(result.rows_affected).toBe(1500)
      expect(result.completed_at).toBeDefined()
    })

    it('should get execution status - failed', async () => {
      const mockStatus = {
        execution_id: 'exec-123',
        state: 'FAILED',
        query_id: 'query-456',
        error_message: 'Table not found: catalog.schema.missing_table',
        rows_affected: null,
        started_at: '2026-01-16T10:00:00Z',
        completed_at: '2026-01-16T10:00:15Z'
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockStatus
      })

      const result = await api.getExecutionStatus('exec-123')
      
      expect(result.state).toBe('FAILED')
      expect(result.error_message).toContain('Table not found')
    })

    it('should handle execution not found', async () => {
      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: 'Not Found',
        json: async () => ({ detail: 'Execution not found' })
      })

      await expect(api.getExecutionStatus('nonexistent')).rejects.toThrow('Execution not found')
    })
  })

  describe('Preview Validation Edge Cases', () => {
    it('should handle SCD2 preview with historical tracking', async () => {
      const mockPlan = {
        plan_name: 'customer_scd2',
        pattern_type: 'SCD2',
        source_table: 'catalog.raw.customers',
        target_table: 'catalog.curated.customer_history'
      }

      const mockPreview = {
        is_valid: true,
        sql: 'MERGE INTO catalog.curated.customer_history AS target...',
        warnings: ['SCD2 will maintain full history'],
        permissions: { has_permissions: true, violations: [] },
        impact_analysis: {
          source_table: 'catalog.raw.customers',
          target_table: 'catalog.curated.customer_history',
          operation: 'MERGE',
          operation_type: 'SCD2_HISTORY_TRACKING',
          is_destructive: false,
          estimated_risk: 'medium' as const
        },
        sample_data: {
          columns: ['id', 'name', 'valid_from', 'valid_to', 'is_current'],
          rows: [
            ['1', 'Customer A', '2026-01-01', '9999-12-31', 'true'],
            ['2', 'Customer B', '2026-01-01', '2026-01-15', 'false'],
            ['2', 'Customer B Updated', '2026-01-15', '9999-12-31', 'true']
          ]
        }
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPreview
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123', true)
      
      expect(result.impact_analysis.operation_type).toBe('SCD2_HISTORY_TRACKING')
      expect(result.sample_data?.columns).toContain('valid_from')
      expect(result.sample_data?.columns).toContain('is_current')
    })

    it('should validate preview with empty result set', async () => {
      const mockPlan = {
        plan_name: 'empty_result',
        pattern_type: 'INCREMENTAL_APPEND'
      }

      const mockPreview = {
        is_valid: true,
        sql: 'INSERT INTO target SELECT * FROM source WHERE false',
        warnings: ['No new records to insert'],
        permissions: { has_permissions: true, violations: [] },
        impact_analysis: {
          source_table: 'source',
          target_table: 'target',
          operation: 'APPEND',
          is_destructive: false,
          estimated_risk: 'low' as const
        },
        sample_data: {
          columns: [],
          rows: []
        }
      }

      global.fetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: async () => mockPreview
      })

      const result = await api.previewPlan(mockPlan, 'test@databricks.com', 'wh-123', true)
      
      expect(result.warnings).toContain('No new records to insert')
      expect(result.sample_data?.rows).toHaveLength(0)
    })
  })
})

