"""
Supply Chain Analysis Module

This module provides comprehensive analysis functions for supply chain data,
including supplier performance, delivery optimization, quality control, and
sustainability metrics.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupplyChainAnalyzer:
    """
    A comprehensive analyzer for supply chain data providing insights on
    supplier performance, delivery optimization, quality control, and sustainability.
    """
    
    def __init__(self, data: pd.DataFrame):
        """
        Initialize the analyzer with supply chain data.
        
        Args:
            data: DataFrame containing supply chain data with required columns
        """
        self.data = data.copy()
        self._validate_data()
        self._preprocess_data()
    
    def _validate_data(self):
        """Validate that the data contains required columns."""
        required_columns = [
            'date', 'supplier', 'order_id', 'order_quantity', 'order_value',
            'expected_delivery', 'actual_delivery', 'on_time_delivery',
            'quality_issues', 'defect_quantity', 'supplier_reliability',
            'sustainability_rating'
        ]
        
        missing_columns = [col for col in required_columns if col not in self.data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
    
    def _preprocess_data(self):
        """Preprocess the data for analysis."""
        # Ensure date columns are datetime
        date_columns = ['date', 'expected_delivery', 'actual_delivery']
        for col in date_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_datetime(self.data[col])
        
        # Calculate additional metrics if not present
        if 'unit_cost' not in self.data.columns:
            self.data['unit_cost'] = self.data['order_value'] / self.data['order_quantity']
        
        if 'delivery_variance_days' not in self.data.columns:
            self.data['delivery_variance_days'] = (
                self.data['actual_delivery'] - self.data['expected_delivery']
            ).dt.days
        
        if 'defect_rate_pct' not in self.data.columns:
            self.data['defect_rate_pct'] = (
                self.data['defect_quantity'] / self.data['order_quantity'] * 100
            )

        if 'quality_status' not in self.data.columns:
            self.data['quality_status'] = pd.cut(
                self.data['defect_rate_pct'],
                bins=[-np.inf, 2, 5, np.inf],
                labels=['Excellent', 'Acceptable', 'Poor']
            )

        if 'delivery_performance' not in self.data.columns:
            # We calculate an on_time_rate proxy if not present to reuse categories logic
            if 'on_time_delivery' in self.data.columns:
                self.data['delivery_performance'] = pd.cut(
                    self.data['on_time_delivery'] * 100,
                    bins=[-np.inf, 70, 85, 95, np.inf],
                    labels=['Poor', 'Fair', 'Good', 'Excellent']
                )
            else:
                 self.data['delivery_performance'] = 'Good'

        if 'sustainability_category' not in self.data.columns:
            self.data['sustainability_category'] = pd.cut(
                self.data['sustainability_rating'],
                bins=[-np.inf, 3.0, 4.0, 4.5, np.inf],
                labels=['Poor', 'Fair', 'Good', 'Excellent']
            )
    
    def get_supplier_performance_summary(self) -> pd.DataFrame:
        """
        Get comprehensive supplier performance summary.
        
        Returns:
            DataFrame with supplier performance metrics
        """
        summary = self.data.groupby('supplier').agg({
            'order_id': 'count',
            'order_value': ['sum', 'mean'],
            'order_quantity': 'sum',
            'unit_cost': 'mean',
            'on_time_delivery': 'mean',
            'delivery_variance_days': 'mean',
            'quality_issues': 'sum',
            'defect_quantity': 'sum',
            'defect_rate_pct': 'mean',
            'supplier_reliability': 'mean',
            'sustainability_rating': 'mean'
        }).round(3)
        
        # Flatten column names
        summary.columns = [
            'total_orders', 'total_order_value', 'avg_order_value',
            'total_quantity', 'avg_unit_cost', 'on_time_delivery_rate',
            'avg_delivery_variance', 'orders_with_quality_issues',
            'total_defect_quantity', 'avg_defect_rate', 'avg_reliability',
            'avg_sustainability_rating'
        ]
        
        # Calculate additional metrics
        summary['on_time_delivery_rate_pct'] = summary['on_time_delivery_rate'] * 100
        summary['quality_issue_rate_pct'] = (
            summary['orders_with_quality_issues'] / summary['total_orders'] * 100
        )
        
        # Performance categories
        summary['delivery_performance'] = pd.cut(
            summary['on_time_delivery_rate_pct'],
            bins=[0, 70, 85, 95, 100],
            labels=['Poor', 'Fair', 'Good', 'Excellent']
        )
        
        summary['quality_performance'] = pd.cut(
            summary['avg_defect_rate'],
            bins=[0, 2, 5, 10, float('inf')],
            labels=['Excellent', 'Good', 'Fair', 'Poor']
        )
        
        summary['reliability_level'] = pd.cut(
            summary['avg_reliability'],
            bins=[0, 0.90, 0.95, 1.0],
            labels=['Low', 'Medium', 'High']
        )
        
        summary['sustainability_level'] = pd.cut(
            summary['avg_sustainability_rating'],
            bins=[0, 3.0, 4.0, 4.5, 5.0],
            labels=['Poor', 'Fair', 'Good', 'Excellent']
        )
        
        return summary.reset_index()
    
    def get_delivery_performance_analysis(self) -> Dict[str, Any]:
        """
        Analyze delivery performance trends and patterns.
        
        Returns:
            Dictionary containing delivery performance insights
        """
        # Overall delivery performance
        total_orders = len(self.data)
        on_time_orders = self.data['on_time_delivery'].sum()
        on_time_rate = on_time_orders / total_orders * 100
        
        # Delivery variance analysis
        variance_stats = self.data['delivery_variance_days'].describe()
        
        # Delivery performance by supplier
        supplier_delivery = self.data.groupby('supplier').agg({
            'on_time_delivery': 'mean',
            'delivery_variance_days': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        supplier_delivery['on_time_rate_pct'] = supplier_delivery['on_time_delivery'] * 100
        
        # Monthly delivery trends
        monthly_delivery = self.data.groupby(
            self.data['date'].dt.to_period('M')
        ).agg({
            'on_time_delivery': 'mean',
            'delivery_variance_days': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        monthly_delivery['on_time_rate_pct'] = monthly_delivery['on_time_delivery'] * 100
        
        return {
            'overall_on_time_rate': on_time_rate,
            'total_orders': total_orders,
            'on_time_orders': on_time_orders,
            'variance_statistics': variance_stats,
            'supplier_delivery_performance': supplier_delivery.reset_index(),
            'monthly_delivery_trends': monthly_delivery.reset_index(),
            'delivery_performance_categories': self._get_delivery_categories()
        }
    
    def _get_delivery_categories(self) -> pd.DataFrame:
        """Get breakdown of delivery performance categories."""
        categories = self.data.groupby('delivery_performance').agg({
            'order_id': 'count',
            'order_value': 'sum',
            'delivery_variance_days': 'mean'
        }).round(2)
        
        categories['percentage'] = categories['order_id'] / categories['order_id'].sum() * 100
        return categories.reset_index()
    
    def get_quality_control_analysis(self) -> Dict[str, Any]:
        """
        Analyze quality control metrics and defect patterns.
        
        Returns:
            Dictionary containing quality control insights
        """
        # Overall quality metrics
        total_orders = len(self.data)
        orders_with_issues = self.data['quality_issues'].sum()
        total_defects = self.data['defect_quantity'].sum()
        avg_defect_rate = self.data['defect_rate_pct'].mean()
        
        # Quality performance by supplier
        supplier_quality = self.data.groupby('supplier').agg({
            'quality_issues': 'sum',
            'defect_quantity': 'sum',
            'defect_rate_pct': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        supplier_quality['quality_issue_rate'] = (
            supplier_quality['quality_issues'] / 
            self.data.groupby('supplier')['order_id'].count() * 100
        )
        
        # Monthly quality trends
        monthly_quality = self.data.groupby(
            self.data['date'].dt.to_period('M')
        ).agg({
            'quality_issues': 'sum',
            'defect_quantity': 'sum',
            'defect_rate_pct': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        # Quality categories
        quality_categories = self.data.groupby('quality_status').agg({
            'order_id': 'count',
            'order_value': 'sum',
            'defect_quantity': 'sum'
        }).round(2)
        
        quality_categories['percentage'] = (
            quality_categories['order_id'] / quality_categories['order_id'].sum() * 100
        )
        
        return {
            'overall_quality_metrics': {
                'total_orders': total_orders,
                'orders_with_issues': orders_with_issues,
                'quality_issue_rate': orders_with_issues / total_orders * 100,
                'total_defects': total_defects,
                'avg_defect_rate': avg_defect_rate
            },
            'supplier_quality_performance': supplier_quality.reset_index(),
            'monthly_quality_trends': monthly_quality.reset_index(),
            'quality_categories': quality_categories.reset_index()
        }
    
    def get_sustainability_analysis(self) -> Dict[str, Any]:
        """
        Analyze sustainability metrics and supplier sustainability performance.
        
        Returns:
            Dictionary containing sustainability insights
        """
        # Overall sustainability metrics
        avg_sustainability = self.data['sustainability_rating'].mean()
        sustainability_distribution = self.data['sustainability_rating'].value_counts().sort_index()
        
        # Supplier sustainability performance
        supplier_sustainability = self.data.groupby('supplier').agg({
            'sustainability_rating': ['mean', 'std', 'count'],
            'supplier_reliability': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        supplier_sustainability.columns = [
            'avg_sustainability_rating', 'sustainability_std', 'order_count',
            'avg_reliability', 'total_order_value'
        ]
        
        # Sustainability categories
        sustainability_categories = self.data.groupby('sustainability_category').agg({
            'order_id': 'count',
            'order_value': 'sum',
            'supplier_reliability': 'mean'
        }).round(2)
        
        sustainability_categories['percentage'] = (
            sustainability_categories['order_id'] / 
            sustainability_categories['order_id'].sum() * 100
        )
        
        # Monthly sustainability trends
        monthly_sustainability = self.data.groupby(
            self.data['date'].dt.to_period('M')
        ).agg({
            'sustainability_rating': 'mean',
            'supplier_reliability': 'mean',
            'order_value': 'sum'
        }).round(3)
        
        return {
            'overall_sustainability_metrics': {
                'avg_sustainability_rating': avg_sustainability,
                'sustainability_distribution': sustainability_distribution
            },
            'supplier_sustainability_performance': supplier_sustainability.reset_index(),
            'sustainability_categories': sustainability_categories.reset_index(),
            'monthly_sustainability_trends': monthly_sustainability.reset_index()
        }
    
    def get_cost_analysis(self) -> Dict[str, Any]:
        """
        Analyze cost trends and supplier cost performance.
        
        Returns:
            Dictionary containing cost analysis insights
        """
        # Overall cost metrics
        total_order_value = self.data['order_value'].sum()
        total_quantity = self.data['order_quantity'].sum()
        avg_unit_cost = total_order_value / total_quantity
        
        # Cost trends over time
        monthly_costs = self.data.groupby(
            self.data['date'].dt.to_period('M')
        ).agg({
            'order_value': 'sum',
            'order_quantity': 'sum',
            'unit_cost': 'mean'
        }).round(2)
        
        monthly_costs['effective_unit_cost'] = (
            monthly_costs['order_value'] / monthly_costs['order_quantity']
        )
        
        # Supplier cost performance
        supplier_costs = self.data.groupby('supplier').agg({
            'order_value': 'sum',
            'order_quantity': 'sum',
            'unit_cost': ['mean', 'std']
        }).round(3)
        
        supplier_costs.columns = [
            'total_order_value', 'total_quantity', 'avg_unit_cost', 'unit_cost_std'
        ]
        
        supplier_costs['effective_unit_cost'] = (
            supplier_costs['total_order_value'] / supplier_costs['total_quantity']
        )
        
        # Cost vs quality correlation
        cost_quality_correlation = self.data[['unit_cost', 'defect_rate_pct']].corr().iloc[0, 1]
        
        return {
            'overall_cost_metrics': {
                'total_order_value': total_order_value,
                'total_quantity': total_quantity,
                'avg_unit_cost': avg_unit_cost
            },
            'monthly_cost_trends': monthly_costs.reset_index(),
            'supplier_cost_performance': supplier_costs.reset_index(),
            'cost_quality_correlation': cost_quality_correlation
        }
    
    def get_supplier_risk_assessment(self) -> pd.DataFrame:
        """
        Assess supplier risk based on multiple factors.
        
        Returns:
            DataFrame with supplier risk scores and categories
        """
        supplier_metrics = self.get_supplier_performance_summary()
        
        # Calculate risk scores (lower is better)
        risk_scores = supplier_metrics.copy()
        
        # Delivery risk (inverse of on-time rate)
        risk_scores['delivery_risk'] = 100 - risk_scores['on_time_delivery_rate_pct']
        
        # Quality risk (defect rate)
        risk_scores['quality_risk'] = risk_scores['avg_defect_rate']
        
        # Reliability risk (inverse of reliability score)
        risk_scores['reliability_risk'] = (1 - risk_scores['avg_reliability']) * 100
        
        # Cost risk (deviation from average unit cost)
        avg_unit_cost = risk_scores['avg_unit_cost'].mean()
        risk_scores['cost_risk'] = abs(risk_scores['avg_unit_cost'] - avg_unit_cost) / avg_unit_cost * 100
        
        # Overall risk score (weighted average)
        risk_scores['overall_risk_score'] = (
            risk_scores['delivery_risk'] * 0.3 +
            risk_scores['quality_risk'] * 0.25 +
            risk_scores['reliability_risk'] * 0.25 +
            risk_scores['cost_risk'] * 0.2
        )
        
        # Risk categories
        risk_scores['risk_category'] = pd.cut(
            risk_scores['overall_risk_score'],
            bins=[0, 20, 40, 60, float('inf')],
            labels=['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk']
        )
        
        return risk_scores[['supplier', 'overall_risk_score', 'risk_category', 
                           'delivery_risk', 'quality_risk', 'reliability_risk', 'cost_risk']]
    
    def get_key_insights(self) -> List[Dict[str, Any]]:
        """
        Generate key insights from the supply chain data.
        
        Returns:
            List of insight dictionaries
        """
        insights = []
        
        # Delivery performance insights
        delivery_analysis = self.get_delivery_performance_analysis()
        on_time_rate = delivery_analysis['overall_on_time_rate']
        
        if on_time_rate < 80:
            insights.append({
                'category': 'Delivery Performance',
                'insight': f'On-time delivery rate is {on_time_rate:.1f}%, below the target of 80%',
                'impact': 'High',
                'recommendation': 'Review delivery processes and supplier performance'
            })
        elif on_time_rate > 95:
            insights.append({
                'category': 'Delivery Performance',
                'insight': f'Excellent on-time delivery rate of {on_time_rate:.1f}%',
                'impact': 'Positive',
                'recommendation': 'Maintain current delivery standards'
            })
        
        # Quality insights
        quality_analysis = self.get_quality_control_analysis()
        avg_defect_rate = quality_analysis['overall_quality_metrics']['avg_defect_rate']
        
        if avg_defect_rate > 5:
            insights.append({
                'category': 'Quality Control',
                'insight': f'Average defect rate of {avg_defect_rate:.2f}% is above acceptable threshold',
                'impact': 'High',
                'recommendation': 'Implement quality improvement initiatives'
            })
        
        # Cost insights
        cost_analysis = self.get_cost_analysis()
        cost_quality_corr = cost_analysis['cost_quality_correlation']
        
        if abs(cost_quality_corr) > 0.3:
            insights.append({
                'category': 'Cost Management',
                'insight': f'Strong correlation ({cost_quality_corr:.2f}) between cost and quality',
                'impact': 'Medium',
                'recommendation': 'Balance cost optimization with quality requirements'
            })
        
        # Supplier risk insights
        risk_assessment = self.get_supplier_risk_assessment()
        high_risk_suppliers = risk_assessment[risk_assessment['risk_category'].isin(['High Risk', 'Critical Risk'])]
        
        if len(high_risk_suppliers) > 0:
            insights.append({
                'category': 'Supplier Risk',
                'insight': f'{len(high_risk_suppliers)} suppliers identified as high or critical risk',
                'impact': 'High',
                'recommendation': 'Develop risk mitigation strategies for high-risk suppliers'
            })
        
        return insights
    
    def get_recommendations(self) -> List[Dict[str, Any]]:
        """
        Generate actionable recommendations based on analysis.
        
        Returns:
            List of recommendation dictionaries
        """
        recommendations = []
        
        # Get analysis results
        supplier_performance = self.get_supplier_performance_summary()
        risk_assessment = self.get_supplier_risk_assessment()
        
        # Delivery performance recommendations
        poor_delivery_suppliers = supplier_performance[
            supplier_performance['delivery_performance'] == 'Poor'
        ]
        
        if len(poor_delivery_suppliers) > 0:
            recommendations.append({
                'category': 'Delivery Optimization',
                'priority': 'High',
                'action': 'Review delivery processes with suppliers',
                'suppliers': poor_delivery_suppliers['supplier'].tolist(),
                'expected_impact': 'Improve on-time delivery rates'
            })
        
        # Quality improvement recommendations
        poor_quality_suppliers = supplier_performance[
            supplier_performance['quality_performance'] == 'Poor'
        ]
        
        if len(poor_quality_suppliers) > 0:
            recommendations.append({
                'category': 'Quality Improvement',
                'priority': 'High',
                'action': 'Implement quality control measures',
                'suppliers': poor_quality_suppliers['supplier'].tolist(),
                'expected_impact': 'Reduce defect rates and quality issues'
            })
        
        # Cost optimization recommendations
        high_cost_suppliers = supplier_performance[
            supplier_performance['avg_unit_cost'] > 
            supplier_performance['avg_unit_cost'].quantile(0.75)
        ]
        
        if len(high_cost_suppliers) > 0:
            recommendations.append({
                'category': 'Cost Optimization',
                'priority': 'Medium',
                'action': 'Negotiate better pricing or explore alternative suppliers',
                'suppliers': high_cost_suppliers['supplier'].tolist(),
                'expected_impact': 'Reduce procurement costs'
            })
        
        # Risk mitigation recommendations
        critical_risk_suppliers = risk_assessment[
            risk_assessment['risk_category'] == 'Critical Risk'
        ]
        
        if len(critical_risk_suppliers) > 0:
            recommendations.append({
                'category': 'Risk Mitigation',
                'priority': 'Critical',
                'action': 'Develop contingency plans and alternative suppliers',
                'suppliers': critical_risk_suppliers['supplier'].tolist(),
                'expected_impact': 'Reduce supply chain disruption risk'
            })
        
        return recommendations


def analyze_supply_chain_data(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Convenience function to perform comprehensive supply chain analysis.
    
    Args:
        data: Supply chain DataFrame
        
    Returns:
        Dictionary containing all analysis results
    """
    analyzer = SupplyChainAnalyzer(data)
    
    return {
        'supplier_performance': analyzer.get_supplier_performance_summary(),
        'delivery_analysis': analyzer.get_delivery_performance_analysis(),
        'quality_analysis': analyzer.get_quality_control_analysis(),
        'sustainability_analysis': analyzer.get_sustainability_analysis(),
        'cost_analysis': analyzer.get_cost_analysis(),
        'risk_assessment': analyzer.get_supplier_risk_assessment(),
        'key_insights': analyzer.get_key_insights(),
        'recommendations': analyzer.get_recommendations()
    }


def generate_supply_chain_report(data: pd.DataFrame) -> str:
    """
    Generate a comprehensive supply chain analysis report.
    
    Args:
        data: Supply chain DataFrame
        
    Returns:
        Formatted report string
    """
    analysis = analyze_supply_chain_data(data)
    
    report = []
    report.append("=" * 60)
    report.append("SUPPLY CHAIN ANALYSIS REPORT")
    report.append("=" * 60)
    report.append("")
    
    # Executive Summary
    report.append("EXECUTIVE SUMMARY")
    report.append("-" * 20)
    
    analyzer = SupplyChainAnalyzer(data)
    total_orders = len(analyzer.data)
    total_value = analyzer.data['order_value'].sum()
    on_time_rate = analyzer.data['on_time_delivery'].mean() * 100
    avg_defect_rate = analyzer.data['defect_rate_pct'].mean()
    
    report.append(f"Total Orders: {total_orders:,}")
    report.append(f"Total Order Value: ${total_value:,.0f}")
    report.append(f"On-Time Delivery Rate: {on_time_rate:.1f}%")
    report.append(f"Average Defect Rate: {avg_defect_rate:.2f}%")
    report.append("")
    
    # Key Insights
    report.append("KEY INSIGHTS")
    report.append("-" * 20)
    for insight in analysis['key_insights']:
        report.append(f"• {insight['insight']}")
    report.append("")
    
    # Recommendations
    report.append("RECOMMENDATIONS")
    report.append("-" * 20)
    for rec in analysis['recommendations']:
        report.append(f"• {rec['action']} (Priority: {rec['priority']})")
    report.append("")
    
    # Supplier Performance Summary
    report.append("SUPPLIER PERFORMANCE SUMMARY")
    report.append("-" * 30)
    supplier_summary = analysis['supplier_performance']
    for _, row in supplier_summary.iterrows():
        report.append(f"{row['supplier']}:")
        report.append(f"  - Orders: {row['total_orders']}")
        report.append(f"  - On-Time Rate: {row['on_time_delivery_rate_pct']:.1f}%")
        report.append(f"  - Defect Rate: {row['avg_defect_rate']:.2f}%")
        report.append(f"  - Reliability: {row['avg_reliability']:.3f}")
        report.append("")
    
    return "\n".join(report) 