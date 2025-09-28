import pytest
import pandas as pd
from src.data.validator import SRAGDataValidator

class TestSRAGDataValidator:
    """Testes para o validador de dados SRAG."""
    
    @pytest.fixture
    def validator(self):
        """Fixture para instância do validador."""
        return SRAGDataValidator()
    
    def test_initialization(self, validator):
        """Testa inicialização do validador."""
        assert hasattr(validator, 'validation_rules')
        assert hasattr(validator, 'validation_stats')
        assert isinstance(validator.validation_rules, dict)
    
    def test_validate_data_quality(self, validator, sample_srag_data):
        """Testa validação de qualidade completa."""
        result = validator.validate_data_quality(sample_srag_data)
        
        assert isinstance(result, dict)
        assert 'passed' in result
        assert 'quality_score' in result
        assert 'errors' in result
        assert 'warnings' in result
        assert 'statistics' in result
        
        # Score deve estar entre 0 e 100
        assert 0 <= result['quality_score'] <= 100
    
    def test_validation_with_problematic_data(self, validator):
        """Testa validação com dados problemáticos."""
        # Criar dados com problemas conhecidos
        problematic_data = pd.DataFrame({
            'DT_NOTIFIC': ['01/01/2024', '32/13/2024', ''],  # Data inválida
            'NU_IDADE_N': [25, -5, 200],  # Idade inválida
            'CS_SEXO': ['M', 'F', 'X'],  # Sexo inválido
            'EVOLUCAO': ['1', '2', '8'],  # Evolução inválida
            'UTI': ['1', '2', '5']  # UTI inválida
        })
        
        result = validator.validate_data_quality(problematic_data)
        
        # Deve detectar problemas
        assert len(result['warnings']) > 0 or len(result['errors']) > 0
        assert result['quality_score'] < 90  # Score deve ser baixo
    
    def test_export_validation_report(self, validator, sample_srag_data, tmp_path):
        """Testa exportação de relatório de validação."""
        validation_result = validator.validate_data_quality(sample_srag_data)
        
        report_file = tmp_path / "validation_report.json"
        saved_path = validator.export_validation_report(
            validation_result,
            str(report_file)
        )
        
        assert saved_path == str(report_file)
        assert report_file.exists()
        
        # Verificar conteúdo do relatório
        import json
        with open(report_file, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        assert 'metadata' in report_data
        assert 'summary' in report_data
        assert 'detailed_results' in report_data