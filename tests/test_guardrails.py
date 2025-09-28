import pytest
import pandas as pd
from src.utils.guardrails import SRAGGuardrails

class TestSRAGGuardrails:
    """Testes para o sistema de guardrails."""
    
    @pytest.fixture
    def guardrails(self):
        """Fixture para instância do SRAGGuardrails."""
        return SRAGGuardrails()
    
    def test_initialization(self, guardrails):
        """Testa inicialização dos guardrails."""
        assert hasattr(guardrails, 'sensitive_columns')
        assert hasattr(guardrails, 'metric_limits')
        assert isinstance(guardrails.sensitive_columns, list)
        assert isinstance(guardrails.metric_limits, dict)
    
    def test_validate_request(self, guardrails):
        """Testa validação de solicitações."""
        # Solicitação válida
        valid_request = {
            'report_date': '2024-03-15',
            'include_charts': True,
            'include_news': True
        }
        
        result = guardrails.validate_request(valid_request)
        assert isinstance(result, dict)
        assert 'validation_timestamp' in result
        assert 'validated_by' in result
        
        # Solicitação inválida
        invalid_request = {
            'report_date': 'data-inválida',
            'include_charts': 'não-booleano'
        }
        
        with pytest.raises(ValueError):
            guardrails.validate_request(invalid_request)
    
    def test_validate_health_data(self, guardrails, sample_srag_data):
        """Testa validação de dados de saúde."""
        # Adicionar dados sensíveis aos dados de teste
        test_data = sample_srag_data.copy()
        test_data['CPF'] = ['123.456.789-00'] * len(test_data)
        test_data['NM_PACIENT'] = ['João Silva'] * len(test_data)
        
        validated_data = guardrails.validate_health_data(test_data)
        
        assert isinstance(validated_data, pd.DataFrame)
        # Dados sensíveis devem ter sido removidos
        assert 'CPF' not in validated_data.columns
        assert 'NM_PACIENT' not in validated_data.columns
    
    def test_validate_metrics(self, guardrails):
        """Testa validação de métricas."""
        # Métricas válidas
        valid_metrics = {
            'mortality_rate': {'rate': 8.5, 'interpretation': 'Normal'},
            'icu_occupancy_rate': {'rate': 25.0, 'interpretation': 'Baixa'},
            'vaccination_rate': {'rate': 75.0, 'interpretation': 'Boa'}
        }
        
        result = guardrails.validate_metrics(valid_metrics)
        assert isinstance(result, dict)
        assert 'validation_warnings' in result
        assert len(result['validation_warnings']) == 0  # Sem avisos para valores normais
        
        # Métricas com valores suspeitos
        suspicious_metrics = {
            'mortality_rate': {'rate': 45.0, 'interpretation': 'Muito alta'},  # Acima do máximo
            'icu_occupancy_rate': {'rate': -5.0, 'interpretation': 'Inválida'},  # Negativa
        }
        
        result = guardrails.validate_metrics(suspicious_metrics)
        assert len(result['validation_warnings']) > 0
    
    def test_filter_news_content(self, guardrails, mock_news_articles):
        """Testa filtro de conteúdo de notícias."""
        # Adicionar artigo com conteúdo problemático
        problematic_articles = mock_news_articles + [{
            'title': 'Teoria da conspiração sobre fake news da vacina',
            'summary': 'Artigo promovendo desinformação',
            'source': 'site-suspeito.com'
        }]
        
        news_analysis = {'articles': problematic_articles}
        
        filtered = guardrails.filter_news_content(news_analysis)
        
        assert isinstance(filtered, dict)
        assert 'articles' in filtered
        # Artigo problemático deve ter sido filtrado
        assert len(filtered['articles']) < len(problematic_articles)
    
    def test_validate_final_report(self, guardrails):
        """Testa validação final do relatório."""
        # Relatório válido
        valid_report = {
            'metadata': {
                'report_date': '2024-03-15',
                'generation_timestamp': '2024-03-15T10:00:00Z'
            },
            'metrics': {
                'mortality_rate': {'rate': 8.5}
            }
        }
        
        result = guardrails.validate_final_report(valid_report)
        
        assert isinstance(result, dict)
        assert 'guardrails_signature' in result
        assert result['metadata']['validated_by_guardrails'] == True
        
        # Relatório inválido (sem seções obrigatórias)
        invalid_report = {'some_data': 'test'}
        
        with pytest.raises(ValueError):
            guardrails.validate_final_report(invalid_report)