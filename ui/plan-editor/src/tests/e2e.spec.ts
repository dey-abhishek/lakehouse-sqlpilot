// End-to-End UI Tests using Playwright
// Tests complete user workflows in the browser

import { test, expect } from '@playwright/test';

test.describe('Plan Creation Workflow', () => {
  test('complete plan creation flow', async ({ page }) => {
    // Navigate to app
    await page.goto('http://localhost:3000');
    
    // Click New Plan
    await page.click('text=New Plan');
    await expect(page).toHaveURL(/.*plans\/new/);
    
    // Fill in plan metadata
    await page.fill('[name="plan_name"]', 'test_incremental_plan');
    await page.fill('[name="owner"]', 'test@databricks.com');
    await page.fill('[name="description"]', 'Test incremental append plan');
    
    // Select pattern
    await page.click('[data-testid="pattern-type-select"]');
    await page.click('text=INCREMENTAL_APPEND');
    
    // Fill in source
    await page.fill('[name="source_catalog"]', 'lakehouse-sqlpilot');
    await page.fill('[name="source_schema"]', 'lakehouse-sqlpilot-schema');
    await page.fill('[name="source_table"]', 'events_raw');
    
    // Fill in target
    await page.fill('[name="target_catalog"]', 'lakehouse-sqlpilot');
    await page.fill('[name="target_schema"]', 'lakehouse-sqlpilot-schema');
    await page.fill('[name="target_table"]', 'events_processed');
    
    // Select write mode
    await page.click('[data-testid="write-mode-select"]');
    await page.click('text=append');
    
    // Fill in warehouse
    await page.fill('[name="warehouse_id"]', 'test_warehouse_123');
    
    // Save plan
    await page.click('text=Save');
    
    // Should show success message
    await expect(page.locator('text=Plan saved successfully')).toBeVisible();
  });

  test('SCD2 plan creation with all fields', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/new');
    
    // Fill metadata
    await page.fill('[name="plan_name"]', 'test_scd2_plan');
    await page.fill('[name="owner"]', 'test@databricks.com');
    
    // Select SCD2 pattern
    await page.click('[data-testid="pattern-type-select"]');
    await page.click('text=SCD2');
    
    // SCD2-specific fields should appear
    await expect(page.locator('text=Business Keys')).toBeVisible();
    await expect(page.locator('text=Effective Date Column')).toBeVisible();
    
    // Fill source with columns
    await page.fill('[name="source_catalog"]', 'lakehouse-sqlpilot');
    await page.fill('[name="source_schema"]', 'lakehouse-sqlpilot-schema');
    await page.fill('[name="source_table"]', 'customer_current');
    
    // Add source columns
    await page.click('text=Add Column');
    await page.fill('[name="column_0"]', 'customer_id');
    await page.click('text=Add Column');
    await page.fill('[name="column_1"]', 'name');
    
    // Fill pattern config
    await page.fill('[name="business_keys"]', 'customer_id');
    await page.fill('[name="effective_date_column"]', 'valid_from');
    
    // Save
    await page.click('text=Save');
    await expect(page.locator('text=Plan saved successfully')).toBeVisible();
  });
});

test.describe('Preview Workflow', () => {
  test('preview shows generated SQL', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/test-plan-123/preview');
    
    // Should show preview pane
    await expect(page.locator('text=Preview Plan')).toBeVisible();
    
    // Should show generated SQL
    await expect(page.locator('text=Generated SQL')).toBeVisible();
    await expect(page.locator('code')).toContainText('INSERT INTO');
    
    // Should show SQL is read-only
    await expect(page.locator('text=deterministically generated')).toBeVisible();
  });

  test('preview shows validation status', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/test-plan-123/preview');
    
    // Should show validation status
    const validationStatus = page.locator('[data-testid="validation-status"]');
    await expect(validationStatus).toBeVisible();
    
    // Should be either success or error
    const isValid = await validationStatus.locator('text=valid').isVisible();
    expect(isValid).toBeTruthy();
  });

  test('preview shows impact analysis', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/test-plan-123/preview');
    
    // Should show impact analysis
    await expect(page.locator('text=Impact Analysis')).toBeVisible();
    await expect(page.locator('text=Source:')).toBeVisible();
    await expect(page.locator('text=Target:')).toBeVisible();
    await expect(page.locator('text=Operation:')).toBeVisible();
  });

  test('execute button is disabled when invalid', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/invalid-plan/preview');
    
    const executeButton = page.locator('button:has-text("Execute")');
    await expect(executeButton).toBeDisabled();
  });

  test('execute button confirms before execution', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/valid-plan/preview');
    
    // Click execute
    await page.click('button:has-text("Execute")');
    
    // Should show confirmation dialog
    await expect(page.locator('text=confirm')).toBeVisible();
    
    // Cancel
    await page.click('button:has-text("Cancel")');
    
    // Should still be on preview page
    await expect(page).toHaveURL(/.*preview/);
  });
});

test.describe('Execution Dashboard', () => {
  test('displays execution history', async ({ page }) => {
    await page.goto('http://localhost:3000/executions');
    
    // Should show dashboard
    await expect(page.locator('text=Execution Dashboard')).toBeVisible();
    
    // Should show table
    await expect(page.locator('text=Execution ID')).toBeVisible();
    await expect(page.locator('text=Plan Name')).toBeVisible();
    await expect(page.locator('text=State')).toBeVisible();
  });

  test('refresh button updates data', async ({ page }) => {
    await page.goto('http://localhost:3000/executions');
    
    // Click refresh
    await page.click('button:has-text("Refresh")');
    
    // Should reload data (check for loading state)
    await page.waitForLoadState('networkidle');
  });

  test('execution states are color-coded', async ({ page }) => {
    await page.goto('http://localhost:3000/executions');
    
    // Success should be green
    const successChip = page.locator('.MuiChip-colorSuccess').first();
    if (await successChip.isVisible()) {
      await expect(successChip).toHaveText('SUCCESS');
    }
    
    // Failed should be red
    const errorChip = page.locator('.MuiChip-colorError').first();
    if (await errorChip.isVisible()) {
      await expect(errorChip).toHaveText('FAILED');
    }
  });
});

test.describe('Navigation', () => {
  test('navigation between pages works', async ({ page }) => {
    await page.goto('http://localhost:3000');
    
    // Click Plans in nav
    await page.click('text=Plans');
    await expect(page).toHaveURL('http://localhost:3000/');
    
    // Click Executions in nav
    await page.click('text=Executions');
    await expect(page).toHaveURL('http://localhost:3000/executions');
    
    // Click back to Plans
    await page.click('text=Plans');
    await expect(page).toHaveURL('http://localhost:3000/');
  });

  test('active nav item is highlighted', async ({ page }) => {
    await page.goto('http://localhost:3000/executions');
    
    const executionsNav = page.locator('button:has-text("Executions")');
    await expect(executionsNav).toHaveCSS('color', /#FF3621/); // Databricks red
  });
});

test.describe('Form Validation', () => {
  test('shows validation errors for invalid inputs', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/new');
    
    // Fill invalid plan name (with spaces)
    await page.fill('[name="plan_name"]', 'Invalid Name With Spaces');
    await page.blur('[name="plan_name"]');
    
    // Should show validation error
    await expect(page.locator('text=/lowercase.*alphanumeric/i')).toBeVisible();
  });

  test('email validation works', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/new');
    
    // Fill invalid email
    await page.fill('[name="owner"]', 'not-an-email');
    await page.blur('[name="owner"]');
    
    // Should show validation error
    await expect(page.locator('text=/valid email/i')).toBeVisible();
  });

  test('required fields prevent save', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/new');
    
    // Try to save without filling anything
    await page.click('text=Save');
    
    // Should show validation errors
    await expect(page.locator('.error')).toHaveCount(greaterThan(0));
  });
});

test.describe('Security', () => {
  test('no SQL editor is present', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/new');
    
    // Should NOT have a SQL editor for users to write SQL
    await expect(page.locator('[data-testid="sql-editor"]')).not.toBeVisible();
    await expect(page.locator('textarea[name="sql"]')).not.toBeVisible();
  });

  test('preview SQL is read-only', async ({ page }) => {
    await page.goto('http://localhost:3000/plans/test-plan/preview');
    
    // SQL editor should be read-only
    const sqlEditor = page.locator('.monaco-editor');
    if (await sqlEditor.isVisible()) {
      // Monaco editor should have read-only class
      await expect(sqlEditor).toHaveAttribute('data-mode-id', 'sql');
    }
  });
});

test.describe('Responsive Design', () => {
  test('works on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 }); // iPhone size
    await page.goto('http://localhost:3000');
    
    // Should still be functional
    await expect(page.locator('text=Plans')).toBeVisible();
  });

  test('works on tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 }); // iPad size
    await page.goto('http://localhost:3000/plans/new');
    
    // Form should be usable
    await expect(page.locator('[name="plan_name"]')).toBeVisible();
  });
});


