import { test, expect } from '@playwright/test'

test.describe('Settings Page E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.click('text=Settings')
    await expect(page).toHaveURL(/.*settings/)
  })

  test('user can configure Databricks connection', async ({ page }) => {
    // Fill in Databricks connection details
    await page.fill('[name="workspaceUrl"]', 'https://test.databricks.com')
    await page.fill('[name="workspaceId"]', '1234567890')
    await page.fill('[name="token"]', 'dapi1234567890abcdef')
    
    // Save configuration
    await page.click('text=Save Configuration')
    
    // Verify success message
    await expect(page.locator('text=Configuration saved successfully')).toBeVisible()
  })

  test('user can configure Unity Catalog settings', async ({ page }) => {
    // Expand Unity Catalog accordion
    await page.click('text=Unity Catalog')
    
    // Fill in catalog details
    await page.fill('[name="defaultCatalog"]', 'lakehouse-sqlpilot')
    await page.fill('[name="defaultSchema"]', 'lakehouse-sqlpilot-schema')
    
    // Save
    await page.click('text=Save Configuration')
    
    await expect(page.locator('text=saved successfully')).toBeVisible()
  })

  test('user can configure SQL Warehouse', async ({ page }) => {
    // Expand SQL Warehouse accordion
    await page.click('text=SQL Warehouse')
    
    // Fill in warehouse ID
    await page.fill('[name="warehouseId"]', '592f1f39793f7795')
    
    // Save
    await page.click('text=Save Configuration')
    
    await expect(page.locator('text=saved successfully')).toBeVisible()
  })

  test('user can configure Genie integration', async ({ page }) => {
    // Expand Genie Integration accordion
    await page.click('text=Genie Integration')
    
    // Fill in Genie Space ID
    await page.fill('[name="genieSpaceId"]', '01abc123-4567-89de-f012-3456789abcde')
    
    // Save
    await page.click('text=Save Configuration')
    
    await expect(page.locator('text=saved successfully')).toBeVisible()
  })

  test('user can test connection', async ({ page }) => {
    // Fill in required fields
    await page.fill('[name="workspaceUrl"]', 'https://test.databricks.com')
    await page.fill('[name="token"]', 'dapi1234567890abcdef')
    await page.fill('[name="warehouseId"]', '592f1f39793f7795')
    
    // Click Test Connection
    await page.click('text=Test Connection')
    
    // Wait for result (success or error)
    await expect(page.locator('[role="alert"]')).toBeVisible({ timeout: 10000 })
  })

  test('user can reset configuration', async ({ page }) => {
    // Fill in some fields
    await page.fill('[name="workspaceUrl"]', 'https://test.databricks.com')
    await page.fill('[name="defaultCatalog"]', 'test_catalog')
    
    // Reset
    await page.click('text=Reset to Defaults')
    
    // Confirm reset
    await page.click('text=Confirm')
    
    // Verify fields are cleared
    await expect(page.locator('[name="workspaceUrl"]')).toHaveValue('')
  })

  test('configuration persists after page reload', async ({ page }) => {
    // Fill and save configuration
    await page.fill('[name="workspaceUrl"]', 'https://persistent.databricks.com')
    await page.fill('[name="defaultCatalog"]', 'persistent_catalog')
    await page.click('text=Save Configuration')
    
    await expect(page.locator('text=saved successfully')).toBeVisible()
    
    // Reload page
    await page.reload()
    
    // Navigate back to settings
    await page.click('text=Settings')
    
    // Verify values persisted
    await expect(page.locator('[name="workspaceUrl"]')).toHaveValue('https://persistent.databricks.com')
    await expect(page.locator('[name="defaultCatalog"]')).toHaveValue('persistent_catalog')
  })

  test('shows validation errors for invalid input', async ({ page }) => {
    // Enter invalid workspace URL
    await page.fill('[name="workspaceUrl"]', 'not-a-valid-url')
    
    // Try to save
    await page.click('text=Save Configuration')
    
    // Should show validation error
    await expect(page.locator('text=Invalid URL')).toBeVisible()
  })

  test('accordion sections expand and collapse', async ({ page }) => {
    // Unity Catalog should be collapsed initially
    await expect(page.locator('[name="defaultCatalog"]')).not.toBeVisible()
    
    // Click to expand
    await page.click('text=Unity Catalog')
    
    // Now should be visible
    await expect(page.locator('[name="defaultCatalog"]')).toBeVisible()
    
    // Click again to collapse
    await page.click('text=Unity Catalog')
    
    // Should be hidden again
    await expect(page.locator('[name="defaultCatalog"]')).not.toBeVisible()
  })
})

