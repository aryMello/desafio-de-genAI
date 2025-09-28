import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.tools.metrics_tool import MetricsCalculatorTool

class TestMetricsCalculatorTool:
    """Testes para a ferramenta de cálculo de métricas."""
    
    @pytest.fixture
    def metrics_tool(self):
        """Fixture para instância do MetricsCalculatorTool."""
        return MetricsCalculatorTool()
    
    @pytest.fixture
    def metrics_test_data(self):
        """Dados específicos para testar métricas."""
        # Criar dados com padrões conhecidos para validar cálculos
        dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
        n_records = len(dates)
        
        data = pd.DataFrame({
            'DT_NOTIFIC': dates,
            'EVOLUCAO': ['1'] * int(n_records * 0.8) + ['2'] * int(n_records * 0.2),  # 20% óbitos
            'UTI': ['2'] * int(n_records * 0.7) + ['1'] * int(n_records * 0.3),  # 30% UTI
            'DOSE_1_COV': ['1'] * int(n_records * 0.85) + ['2'] * int(n_records * 0.15),  # 85% vacinados
            'DOSE_2_COV': ['1'] * int(n_records * 0.75) + ['2'] * int(n_records * 0.25),  # 75% com 2ª dose
        })
        
        return data
    
    @pytest.mark.asyncio
    async def test_calculate_mortality_rate(self, metrics_tool, metrics_test_data):
        """Testa cálculo da taxa de mortalidade."""
        result = await metrics_tool.calculate_mortality_rate(
            metrics_test_data, 
            "2024-06-15"
        )
        
        assert isinstance(result, dict)
        assert 'rate' in result
        assert 'interpretation' in result
        assert 'total_cases' in result
        assert 'deaths' in result
        
        # A taxa deve ser aproximadamente 20% (conforme dados de teste)
        assert 15 <= result['rate'] <= 25
    
    @pytest.mark.asyncio
    async def test_calculate_icu_occupancy_rate(self, metrics_tool, metrics_test_data):
        """Testa cálculo da taxa de ocupação de UTI."""
        result = await metrics_tool.calculate_icu_occupancy_rate(
            metrics_test_data,
            "2024-06-15"
        )
        
        assert isinstance(result, dict)
        assert 'rate' in result
        assert 'total_hospitalized' in result
        assert 'icu_cases' in result
        
        # A taxa deve ser aproximadamente 30% (conforme dados de teste)
        assert 25 <= result['rate'] <= 35
    
    @pytest.mark.asyncio
    async def test_calculate_vaccination_rate(self, metrics_tool, metrics_test_data):
        """Testa cálculo da taxa de vacinação."""
        result = await metrics_tool.calculate_vaccination_rate(
            metrics_test_data,
            "2024-06-15"
        )
        
        assert isinstance(result, dict)
        assert 'rate' in result
        assert 'vaccinated_cases' in result
        assert 'vaccination_breakdown' in result
        
        # A taxa deve ser alta (conforme dados de teste)
        assert result['rate'] >= 70
    
    @pytest.mark.asyncio
    async def test_calculate_case_increase_rate(self, metrics_tool, metrics_test_data):
        """Testa cálculo da taxa de aumento de casos."""
        result = await metrics_tool.calculate_case_increase_rate(
            metrics_test_data,
            "2024-06-15"
        )
        
        assert isinstance(result, dict)
        assert 'rate' in result
        assert 'current_cases' in result
        assert 'previous_cases' in result
        assert 'period_analysis' in result
    
    @pytest.mark.asyncio
    async def test_invalid_data_handling(self, metrics_tool):
        """Testa tratamento de dados inválidos."""
        # DataFrame vazio
        empty_df = pd.DataFrame()
        
        result = await metrics_tool.calculate_mortality_rate(empty_df, "2024-01-01")
        assert result['rate'] == 0.0
        
        # DataFrame sem colunas necessárias
        invalid_df = pd.DataFrame({'col1': [1, 2, 3]})
        
        with pytest.raises(ValueError):
            await metrics_tool.calculate_mortality_rate(invalid_df, "2024-01-01")
    
    def test_health_check(self, metrics_tool):
        """Testa verificação de saúde."""
        health = metrics_tool.health_check()
        
        assert isinstance(health, dict)
        assert 'status' in health
        assert health['status'] in ['healthy', 'error']