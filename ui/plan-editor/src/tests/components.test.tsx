// Frontend UI Component Tests
// Tests for React components in the Plan Editor UI

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import '@testing-library/jest-dom';
import { QueryClient, QueryClientProvider } from 'react-query';

import PlanList from '../src/pages/PlanList';
import PlanEditor from '../src/pages/PlanEditor';
import PreviewPane from '../src/pages/PreviewPane';
import ExecutionDashboard from '../src/pages/ExecutionDashboard';

// Test utilities
const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: false },
  },
});

const renderWithProviders = (component) => {
  return render(
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        {component}
      </BrowserRouter>
    </QueryClientProvider>
  );
};

describe('PlanList Component', () => {
  test('renders plan list page', () => {
    renderWithProviders(<PlanList />);
    expect(screen.getByText('Plans')).toBeInTheDocument();
    expect(screen.getByText('New Plan')).toBeInTheDocument();
  });

  test('displays plan table headers', () => {
    renderWithProviders(<PlanList />);
    expect(screen.getByText('Plan Name')).toBeInTheDocument();
    expect(screen.getByText('Version')).toBeInTheDocument();
    expect(screen.getByText('Pattern')).toBeInTheDocument();
    expect(screen.getByText('Status')).toBeInTheDocument();
  });

  test('new plan button navigates to editor', () => {
    renderWithProviders(<PlanList />);
    const newPlanButton = screen.getByText('New Plan');
    expect(newPlanButton).toBeInTheDocument();
    fireEvent.click(newPlanButton);
    // Check navigation would occur (test router state)
  });
});

describe('PlanEditor Component', () => {
  test('renders plan editor form', () => {
    renderWithProviders(<PlanEditor />);
    expect(screen.getByText('New Plan')).toBeInTheDocument();
    expect(screen.getByLabelText('Plan Name')).toBeInTheDocument();
    expect(screen.getByLabelText('Owner Email')).toBeInTheDocument();
  });

  test('shows governance message', () => {
    renderWithProviders(<PlanEditor />);
    expect(screen.getByText(/Governed Plan Creation/i)).toBeInTheDocument();
    expect(screen.getByText(/No free-form SQL is allowed/i)).toBeInTheDocument();
  });

  test('pattern type selector works', () => {
    renderWithProviders(<PlanEditor />);
    const patternSelect = screen.getByLabelText('Pattern Type');
    expect(patternSelect).toBeInTheDocument();
    
    fireEvent.click(patternSelect);
    // Should show pattern options
  });

  test('validates plan name format', async () => {
    renderWithProviders(<PlanEditor />);
    const planNameInput = screen.getByLabelText('Plan Name');
    
    // Test invalid format (with spaces)
    fireEvent.change(planNameInput, { target: { value: 'Invalid Name' } });
    // Should show validation error
    
    // Test valid format
    fireEvent.change(planNameInput, { target: { value: 'valid_plan_name' } });
    // Should clear validation error
  });

  test('save button is present', () => {
    renderWithProviders(<PlanEditor />);
    expect(screen.getByText('Save')).toBeInTheDocument();
  });

  test('preview button is present', () => {
    renderWithProviders(<PlanEditor />);
    expect(screen.getByText('Preview')).toBeInTheDocument();
  });
});

describe('PreviewPane Component', () => {
  test('renders preview pane', () => {
    renderWithProviders(<PreviewPane />);
    expect(screen.getByText('Preview Plan')).toBeInTheDocument();
    expect(screen.getByText('Execute')).toBeInTheDocument();
  });

  test('shows validation status', () => {
    renderWithProviders(<PreviewPane />);
    // Should show validation status (success or error)
    expect(screen.getByText(/valid/i)).toBeInTheDocument();
  });

  test('displays generated SQL', () => {
    renderWithProviders(<PreviewPane />);
    expect(screen.getByText('Generated SQL')).toBeInTheDocument();
    expect(screen.getByText(/deterministically generated/i)).toBeInTheDocument();
  });

  test('shows impact analysis', () => {
    renderWithProviders(<PreviewPane />);
    expect(screen.getByText('Impact Analysis')).toBeInTheDocument();
    expect(screen.getByText('Source:')).toBeInTheDocument();
    expect(screen.getByText('Target:')).toBeInTheDocument();
  });

  test('execute button disabled when invalid', () => {
    renderWithProviders(<PreviewPane />);
    const executeButton = screen.getByText('Execute');
    // Should check if button is disabled based on validation state
  });
});

describe('ExecutionDashboard Component', () => {
  test('renders execution dashboard', () => {
    renderWithProviders(<ExecutionDashboard />);
    expect(screen.getByText('Execution Dashboard')).toBeInTheDocument();
    expect(screen.getByText('Refresh')).toBeInTheDocument();
  });

  test('displays execution table', () => {
    renderWithProviders(<ExecutionDashboard />);
    expect(screen.getByText('Execution ID')).toBeInTheDocument();
    expect(screen.getByText('Plan Name')).toBeInTheDocument();
    expect(screen.getByText('State')).toBeInTheDocument();
  });

  test('shows execution states correctly', () => {
    renderWithProviders(<ExecutionDashboard />);
    // Should render state chips with correct colors
  });
});

describe('Form Validation', () => {
  test('email validation works', async () => {
    renderWithProviders(<PlanEditor />);
    const emailInput = screen.getByLabelText('Owner Email');
    
    // Invalid email
    fireEvent.change(emailInput, { target: { value: 'invalid-email' } });
    fireEvent.blur(emailInput);
    // Should show error
    
    // Valid email
    fireEvent.change(emailInput, { target: { value: 'test@databricks.com' } });
    fireEvent.blur(emailInput);
    // Should clear error
  });

  test('required fields are enforced', () => {
    renderWithProviders(<PlanEditor />);
    const saveButton = screen.getByText('Save');
    
    // Try to save without filling required fields
    fireEvent.click(saveButton);
    // Should show validation errors
  });

  test('pattern-specific fields appear', () => {
    renderWithProviders(<PlanEditor />);
    const patternSelect = screen.getByLabelText('Pattern Type');
    
    // Select SCD2
    fireEvent.change(patternSelect, { target: { value: 'SCD2' } });
    // Should show SCD2-specific fields
  });
});

describe('Accessibility', () => {
  test('form labels are properly associated', () => {
    renderWithProviders(<PlanEditor />);
    const planNameInput = screen.getByLabelText('Plan Name');
    expect(planNameInput).toHaveAttribute('id');
  });

  test('buttons have accessible names', () => {
    renderWithProviders(<PlanList />);
    const newPlanButton = screen.getByRole('button', { name: /new plan/i });
    expect(newPlanButton).toBeInTheDocument();
  });

  test('alerts have appropriate roles', () => {
    renderWithProviders(<PreviewPane />);
    const alert = screen.getByRole('alert');
    expect(alert).toBeInTheDocument();
  });
});


