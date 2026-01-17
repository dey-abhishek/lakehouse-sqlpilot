import { test, expect } from '@playwright/test'

test.describe('Full User Workflow E2E', () => {
  test('complete workflow: configure → create plan → preview → execute', async ({ page }) => {
    // Step 1: Initial page load
    await page.goto('/')
    await expect(page).toHaveTitle(/SQLPilot/)
    
    // Step 2: Configure settings
    await page.click('text=Settings')
    await expect(page).toHaveURL(/.*settings/)
    
    // Fill in Databricks connection
    await page.fill('[name="workspaceUrl"]', 'https://e2-dogfood.staging.cloud.databricks.com/?o=6051921418418893')
    await page.fill('[name="token"]', process.env.DATABRICKS_TOKEN || 'test-token')
    await page.fill('[name="warehouseId"]', '592f1f39793f7795')
    
    // Fill in Unity Catalog
    await page.click('text=Unity Catalog')
    await page.fill('[name="defaultCatalog"]', 'lakehouse-sqlpilot')
    await page.fill('[name="defaultSchema"]', 'lakehouse-sqlpilot-schema')
    
    // Save configuration
    await page.click('text=Save Configuration')
    await expect(page.locator('text=saved successfully')).toBeVisible()
    
    // Step 3: Navigate to Plans
    await page.click('text=Plans')
    await expect(page).toHaveURL(/.*plans/)
    
    // Step 4: Create new plan
    await page.click('text=Create New Plan')
    
    // Fill in plan details
    await page.fill('[name="planName"]', 'test_customer_scd2')
    await page.fill('[name="owner"]', 'test@databricks.com')
    await page.selectOption('[name="pattern"]', 'scd2')
    
    // Fill in SCD2 configuration
    await page.fill('[name="sourceTable"]', 'lakehouse-sqlpilot.lakehouse-sqlpilot-schema.customers')
    await page.fill('[name="targetTable"]', 'lakehouse-sqlpilot.lakehouse-sqlpilot-schema.customers_scd2')
    await page.fill('[name="businessKeys"]', 'customer_id')
    await page.fill('[name="trackedColumns"]', 'customer_name,email,customer_segment')
    
    // Step 5: Validate plan
    await page.click('text=Validate Plan')
    await expect(page.locator('text=Validation successful')).toBeVisible({ timeout: 10000 })
    
    // Step 6: Preview generated SQL
    await page.click('text=Preview SQL')
    await expect(page.locator('text=MERGE INTO')).toBeVisible({ timeout: 10000 })
    
    // Verify SQL contains expected elements
    const sqlContent = await page.locator('[data-testid="sql-preview"]').textContent()
    expect(sqlContent).toContain('MERGE INTO')
    expect(sqlContent).toContain('lakehouse-sqlpilot')
    expect(sqlContent).toContain('customer_id')
    
    // Step 7: Review impact analysis
    await expect(page.locator('text=Impact Analysis')).toBeVisible()
    await expect(page.locator('text=estimated')).toBeVisible()
    
    // Step 8: Execute plan (optional, requires real credentials)
    if (process.env.RUN_REAL_EXECUTION === 'true') {
      await page.click('text=Execute Plan')
      await expect(page.locator('text=Execution started')).toBeVisible()
      
      // Wait for completion
      await expect(page.locator('text=Execution completed')).toBeVisible({ timeout: 60000 })
      
      // Verify execution results
      await expect(page.locator('text=rows affected')).toBeVisible()
    }
  })

  test('plan validation workflow with errors', async ({ page }) => {
    await page.goto('/')
    
    // Navigate to Plans
    await page.click('text=Plans')
    await page.click('text=Create New Plan')
    
    // Fill in incomplete/invalid plan
    await page.fill('[name="planName"]', 'invalid_plan')
    await page.fill('[name="owner"]', 'invalid-email')  // Invalid email
    await page.selectOption('[name="pattern"]', 'scd2')
    
    // Leave source table empty (validation error)
    
    // Try to validate
    await page.click('text=Validate Plan')
    
    // Should show validation errors
    await expect(page.locator('text=Validation failed')).toBeVisible()
    await expect(page.locator('text=source_table is required')).toBeVisible()
    await expect(page.locator('text=Invalid email')).toBeVisible()
  })

  test('plan list and management', async ({ page }) => {
    await page.goto('/')
    await page.click('text=Plans')
    
    // Should show list of plans
    await expect(page.locator('[data-testid="plan-list"]')).toBeVisible()
    
    // Filter plans
    await page.fill('[placeholder="Search plans"]', 'scd2')
    await expect(page.locator('text=scd2')).toBeVisible()
    
    // Sort plans
    await page.click('text=Sort by Date')
    
    // View plan details
    await page.click('[data-testid="plan-item"]:first-child')
    await expect(page.locator('text=Plan Details')).toBeVisible()
  })

  test('navigation between pages', async ({ page }) => {
    await page.goto('/')
    
    // Home -> Settings
    await page.click('text=Settings')
    await expect(page).toHaveURL(/.*settings/)
    
    // Settings -> Plans
    await page.click('text=Plans')
    await expect(page).toHaveURL(/.*plans/)
    
    // Plans -> Home
    await page.click('text=Lakehouse SQLPilot')
    await expect(page).toHaveURL(/^\/$/)
  })

  test('responsive design on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
    
    // Mobile menu should be visible
    await expect(page.locator('[aria-label="menu"]')).toBeVisible()
    
    // Click mobile menu
    await page.click('[aria-label="menu"]')
    
    // Navigation drawer should open
    await expect(page.locator('text=Plans')).toBeVisible()
    await expect(page.locator('text=Settings')).toBeVisible()
  })

  test('configuration banner shows when not configured', async ({ page }) => {
    // Clear localStorage
    await page.goto('/')
    await page.evaluate(() => localStorage.clear())
    await page.reload()
    
    // Should show configuration banner
    await expect(page.locator('text=Please configure')).toBeVisible()
    await expect(page.locator('text=Go to Settings')).toBeVisible()
    
    // Click to go to settings
    await page.click('text=Go to Settings')
    await expect(page).toHaveURL(/.*settings/)
  })

  test('pattern selection changes form fields', async ({ page }) => {
    await page.goto('/')
    await page.click('text=Plans')
    await page.click('text=Create New Plan')
    
    // Select SCD2 pattern
    await page.selectOption('[name="pattern"]', 'scd2')
    
    // Should show SCD2-specific fields
    await expect(page.locator('[name="businessKeys"]')).toBeVisible()
    await expect(page.locator('[name="trackedColumns"]')).toBeVisible()
    await expect(page.locator('[name="validFromColumn"]')).toBeVisible()
    
    // Switch to incremental append
    await page.selectOption('[name="pattern"]', 'incremental_append')
    
    // Should show different fields
    await expect(page.locator('[name="watermarkColumn"]')).toBeVisible()
    await expect(page.locator('[name="watermarkType"]')).toBeVisible()
    
    // SCD2 fields should be hidden
    await expect(page.locator('[name="businessKeys"]')).not.toBeVisible()
  })

  test('SQL preview updates when plan changes', async ({ page }) => {
    await page.goto('/')
    await page.click('text=Plans')
    await page.click('text=Create New Plan')
    
    // Fill in basic plan
    await page.fill('[name="planName"]', 'dynamic_test')
    await page.selectOption('[name="pattern"]', 'scd2')
    await page.fill('[name="sourceTable"]', 'source_table_1')
    
    // Generate preview
    await page.click('text=Preview SQL')
    let sqlContent = await page.locator('[data-testid="sql-preview"]').textContent()
    expect(sqlContent).toContain('source_table_1')
    
    // Change source table
    await page.fill('[name="sourceTable"]', 'source_table_2')
    
    // Regenerate preview
    await page.click('text=Preview SQL')
    sqlContent = await page.locator('[data-testid="sql-preview"]').textContent()
    expect(sqlContent).toContain('source_table_2')
    expect(sqlContent).not.toContain('source_table_1')
  })

  test('error handling for network failures', async ({ page }) => {
    // Intercept API calls and make them fail
    await page.route('**/api/**', route => route.abort())
    
    await page.goto('/')
    await page.click('text=Plans')
    
    // Should show error message
    await expect(page.locator('text=Failed to load')).toBeVisible()
    await expect(page.locator('text=Retry')).toBeVisible()
    
    // Click retry
    await page.unroute('**/api/**')
    await page.click('text=Retry')
    
    // Should attempt to reload
    await expect(page.locator('text=Loading')).toBeVisible()
  })
})

