"""
Explanation Agent - Explains what a plan will do
"""

from typing import Dict, Any, List
from .base_agent import BaseAgent


class ExplanationAgent(BaseAgent):
    """Explains plan execution logic in natural language"""
    
    def __init__(self):
        super().__init__(
            name="ExplanationAgent",
            description="Explains what a plan will do in natural language"
        )
    
    def process(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process explanation request
        
        Input format:
        {
            'request_type': 'explain_plan',
            'plan': {...},  # Plan dictionary
            'sql': '...',   # Compiled SQL (optional)
            'detail_level': 'summary' | 'detailed'
        }
        
        Output format:
        {
            'success': True/False,
            'explanation': '...',
            'data_flow': [...],
            'risk_assessment': {...},
            'business_impact': '...'
        }
        """
        # Validate input
        is_valid, errors = self.validate_input(input_data)
        if not is_valid:
            return {'success': False, 'errors': errors}
        
        plan = input_data.get('plan', {})
        sql = input_data.get('sql', '')
        detail_level = input_data.get('detail_level', 'summary')
        
        # Generate explanation
        explanation = self._generate_explanation(plan, sql, detail_level)
        
        # Generate data flow
        data_flow = self._generate_data_flow(plan)
        
        # Assess risk
        risk_assessment = self._assess_risk(plan)
        
        # Describe business impact
        business_impact = self._describe_business_impact(plan)
        
        result = {
            'success': True,
            'explanation': explanation,
            'data_flow': data_flow,
            'risk_assessment': risk_assessment,
            'business_impact': business_impact
        }
        
        self.log_interaction(input_data, result)
        
        return result
    
    def get_allowed_inputs(self) -> List[str]:
        return ['explain_plan', 'explain_sql', 'explain_pattern']
    
    def get_allowed_outputs(self) -> List[str]:
        return ['explanation', 'data_flow', 'risk_assessment', 'business_impact']
    
    def _generate_explanation(self, plan: Dict, sql: str, detail_level: str) -> str:
        """Generate natural language explanation"""
        pattern_type = plan.get('pattern', {}).get('type', 'UNKNOWN')
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_config = plan.get('pattern_config', {})
        
        source_fqn = f"{source.get('catalog')}.{source.get('schema')}.{source.get('table')}"
        target_fqn = f"{target.get('catalog')}.{target.get('schema')}.{target.get('table')}"
        
        explanations = {
            'INCREMENTAL_APPEND': f"""
This plan performs an INCREMENTAL APPEND operation.

What it does:
- Reads new data from {source_fqn}
- Identifies rows where {pattern_config.get('watermark_column', 'watermark')} is greater than the maximum value already in {target_fqn}
- Appends only the new rows to {target_fqn}

The operation is idempotent: running it multiple times with the same data will not create duplicates.
""",
            'FULL_REPLACE': f"""
This plan performs a FULL REPLACE operation.

What it does:
- Reads all data from {source_fqn}
- REPLACES the entire contents of {target_fqn} with the source data
- All existing data in the target will be deleted

⚠️ WARNING: This is a destructive operation. Ensure you have backups if needed.
""",
            'MERGE_UPSERT': f"""
This plan performs a MERGE/UPSERT operation.

What it does:
- Reads data from {source_fqn}
- Matches rows in {target_fqn} using keys: {pattern_config.get('merge_keys', [])}
- Updates existing rows when a match is found
- Inserts new rows when no match is found

The operation is idempotent: running it multiple times will produce the same result.
""",
            'SCD2': f"""
This plan implements SLOWLY CHANGING DIMENSION TYPE 2 (SCD2).

What it does:
- Reads data from {source_fqn}
- Compares with current records in {target_fqn} using business keys: {pattern_config.get('business_keys', [])}
- When changes are detected:
  * Closes out the old record (sets end_date and current_flag=FALSE)
  * Inserts a new version with current_flag=TRUE
- Preserves full history of changes over time

This maintains a complete audit trail of all changes to the data.
"""
        }
        
        return explanations.get(pattern_type, f"This plan uses the {pattern_type} pattern.")
    
    def _generate_data_flow(self, plan: Dict) -> List[Dict[str, str]]:
        """Generate data flow description"""
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        flow = [
            {
                'step': 1,
                'action': 'Read Source Data',
                'details': f"Read from {source.get('catalog')}.{source.get('schema')}.{source.get('table')}"
            }
        ]
        
        # Add filters if present
        filters = source.get('filters', [])
        if filters:
            flow.append({
                'step': 2,
                'action': 'Apply Filters',
                'details': f"Filter data with {len(filters)} condition(s)"
            })
        
        # Add pattern-specific steps
        if pattern_type == 'INCREMENTAL_APPEND':
            flow.append({
                'step': len(flow) + 1,
                'action': 'Identify New Records',
                'details': 'Compare watermark with existing data'
            })
        elif pattern_type == 'MERGE_UPSERT':
            flow.append({
                'step': len(flow) + 1,
                'action': 'Match and Merge',
                'details': 'Update existing, insert new'
            })
        
        flow.append({
            'step': len(flow) + 1,
            'action': 'Write to Target',
            'details': f"Write to {target.get('catalog')}.{target.get('schema')}.{target.get('table')}"
        })
        
        return flow
    
    def _assess_risk(self, plan: Dict) -> Dict[str, Any]:
        """Assess execution risk"""
        target = plan.get('target', {})
        write_mode = target.get('write_mode')
        pattern_type = plan.get('pattern', {}).get('type')
        
        risk_level = 'low'
        risk_factors = []
        
        # Check for destructive operations
        if write_mode == 'overwrite':
            risk_level = 'high'
            risk_factors.append('Destructive operation (overwrite)')
        
        # Check for production catalog
        if 'prod' in target.get('catalog', '').lower():
            risk_level = 'medium' if risk_level == 'low' else 'high'
            risk_factors.append('Targets production catalog')
        
        # SCD2 is complex
        if pattern_type == 'SCD2':
            risk_factors.append('Complex multi-step operation')
        
        return {
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'mitigation': 'Always preview before executing' if risk_level == 'high' else 'Standard validation recommended'
        }
    
    def _describe_business_impact(self, plan: Dict) -> str:
        """Describe business impact"""
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        impact_descriptions = {
            'INCREMENTAL_APPEND': 'New data will be added to existing data. Historical data remains unchanged.',
            'FULL_REPLACE': 'All existing data will be replaced. Use when you need a complete refresh.',
            'MERGE_UPSERT': 'Existing records will be updated, new records added. Ideal for synchronization.',
            'SCD2': 'Full change history will be maintained. Enables point-in-time analysis.',
        }
        
        return impact_descriptions.get(pattern_type, 'Data will be transformed according to the specified pattern.')

