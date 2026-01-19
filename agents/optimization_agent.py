"""
Optimization Agent - Suggests performance improvements
"""

from typing import Dict, Any, List
from .base_agent import BaseAgent


class OptimizationAgent(BaseAgent):
    """Suggests performance optimizations for plans"""
    
    def __init__(self):
        super().__init__(
            name="OptimizationAgent",
            description="Suggests performance improvements for plans"
        )
    
    def process(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process optimization request
        
        Input format:
        {
            'request_type': 'optimize_plan',
            'plan': {...},
            'execution_history': [...],
            'table_statistics': {...}
        }
        
        Output format:
        {
            'success': True/False,
            'recommendations': [...],
            'suggested_plan_modifications': {...},  # YAML modifications
            'cost_estimate': {...}
        }
        """
        # Validate input
        is_valid, errors = self.validate_input(input_data)
        if not is_valid:
            return {'success': False, 'errors': errors}
        
        # Enforce boundary: cannot modify plans directly
        self.enforce_boundaries('modify_plans_directly')
        
        plan = input_data.get('plan', {})
        execution_history = input_data.get('execution_history', [])
        table_stats = input_data.get('table_statistics', {})
        
        # Generate recommendations
        recommendations = self._generate_recommendations(plan, execution_history, table_stats)
        
        # Suggest plan modifications (YAML only)
        modifications = self._suggest_modifications(plan, recommendations)
        
        # Estimate costs
        cost_estimate = self._estimate_costs(plan, table_stats)
        
        result = {
            'success': True,
            'recommendations': recommendations,
            'suggested_plan_modifications': modifications,
            'cost_estimate': cost_estimate
        }
        
        self.log_interaction(input_data, result)
        
        return result
    
    def get_allowed_inputs(self) -> List[str]:
        return ['optimize_plan', 'estimate_cost']
    
    def get_allowed_outputs(self) -> List[str]:
        return ['recommendations', 'suggested_plan_modifications', 'cost_estimate']
    
    def _generate_recommendations(self,
                                  plan: Dict,
                                  execution_history: List,
                                  table_stats: Dict) -> List[Dict[str, str]]:
        """Generate performance recommendations"""
        recommendations = []
        
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Recommendation 1: Partitioning
        if not target.get('partition_by'):
            recommendations.append({
                'category': 'Partitioning',
                'recommendation': 'Add partitioning to improve query performance',
                'impact': 'High',
                'reasoning': 'Partitioned tables enable partition pruning and faster queries'
            })
        
        # Recommendation 2: Clustering
        if pattern_type in ['INCREMENTAL_APPEND', 'MERGE_UPSERT'] and not target.get('cluster_by'):
            recommendations.append({
                'category': 'Clustering',
                'recommendation': 'Consider adding clustering keys',
                'impact': 'Medium',
                'reasoning': 'Clustering improves merge and lookup performance'
            })
        
        # Recommendation 3: Execution timing
        if execution_history:
            avg_duration = sum(h.get('duration', 0) for h in execution_history) / len(execution_history)
            if avg_duration > 3600:  # > 1 hour
                recommendations.append({
                    'category': 'Performance',
                    'recommendation': 'Execution time is high - consider optimizing query or using larger warehouse',
                    'impact': 'High',
                    'reasoning': f'Average execution time: {avg_duration / 60:.1f} minutes'
                })
        
        # Recommendation 4: Incremental vs Full
        if pattern_type == 'FULL_REPLACE' and table_stats.get('row_count', 0) > 1000000:
            recommendations.append({
                'category': 'Pattern Selection',
                'recommendation': 'Consider using INCREMENTAL_APPEND instead of FULL_REPLACE',
                'impact': 'High',
                'reasoning': 'Large tables benefit from incremental loads'
            })
        
        return recommendations
    
    def _suggest_modifications(self, plan: Dict, recommendations: List[Dict]) -> Dict[str, Any]:
        """Suggest YAML modifications to plan"""
        modifications = {}
        
        for rec in recommendations:
            if rec['category'] == 'Partitioning':
                modifications['target.partition_by'] = ['date']  # Example
                modifications['_note_partitioning'] = 'Adjust partition column based on query patterns'
            
            elif rec['category'] == 'Clustering':
                modifications['target.cluster_by'] = ['id']  # Example
                modifications['_note_clustering'] = 'Cluster by frequently filtered columns'
            
            elif rec['category'] == 'Performance':
                modifications['execution_config.warehouse_id'] = 'LARGER_WAREHOUSE_ID'
                modifications['_note_warehouse'] = 'Consider using a larger warehouse'
        
        return modifications
    
    def _estimate_costs(self, plan: Dict, table_stats: Dict) -> Dict[str, Any]:
        """Estimate execution costs"""
        pattern_type = plan.get('pattern', {}).get('type')
        source_rows = table_stats.get('source_row_count', 0)
        target_rows = table_stats.get('target_row_count', 0)
        
        # Simple cost estimation (in reality, this would be more complex)
        if pattern_type == 'FULL_REPLACE':
            compute_cost = 'High'
            reasoning = 'Full table scan and replace'
        elif pattern_type == 'INCREMENTAL_APPEND':
            compute_cost = 'Low to Medium'
            reasoning = 'Only processes new data'
        elif pattern_type == 'MERGE_UPSERT':
            compute_cost = 'Medium'
            reasoning = 'Requires merge operation'
        else:
            compute_cost = 'Medium'
            reasoning = 'Standard operation'
        
        return {
            'compute_cost': compute_cost,
            'reasoning': reasoning,
            'estimated_rows_processed': source_rows,
            'note': 'Actual costs depend on warehouse size and execution time'
        }


