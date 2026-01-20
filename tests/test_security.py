import pytest
import pandas as pd
import re

class TestSecurity:
    """Testes de segurança e proteção de dados."""
    
    def test_data_anonymization(self):
        """Testa anonimização de dados pessoais."""
        from src.utils.guardrails import SRAGGuardrails
        
        # Criar dados com informações pessoais
        sensitive_data = pd.DataFrame({
            'CPF': ['123.456.789-00', '987.654.321-11'],
            'NM_PACIENT': ['João Silva', 'Maria Santos'],
            'NU_TEL': ['(11) 99999-9999', '(21) 88888-8888'],
            'DT_NOTIFIC': ['01/01/2024', '02/01/2024'],
            'NU_IDADE_N': [45, 32]
        })
        
        guardrails = SRAGGuardrails()
        anonymized = guardrails._anonymize_personal_data(sensitive_data)
        
        # Dados sensíveis devem ter sido removidos
        sensitive_cols = ['CPF', 'NM_PACIENT', 'NU_TEL']
        for col in sensitive_cols:
            assert col not in anonymized.columns
        
        # Dados não sensíveis devem ser preservados
        assert 'DT_NOTIFIC' in anonymized.columns
        assert 'NU_IDADE_N' in anonymized.columns
    
    def test_string_pattern_anonymization(self):
        """Testa anonimização de padrões em strings."""
        from src.utils.guardrails import SRAGGuardrails
        
        guardrails = SRAGGuardrails()
        
        # Textos com dados sensíveis
        text_with_cpf = "Paciente CPF 123.456.789-00 internado"
        text_with_phone = "Contato: (11) 99999-9999"
        text_with_email = "Email: paciente@example.com"
        
        # Anonimizar
        clean_cpf = guardrails._anonymize_string_patterns(text_with_cpf)
        clean_phone = guardrails._anonymize_string_patterns(text_with_phone)
        clean_email = guardrails._anonymize_string_patterns(text_with_email)
        
        # Verificar se padrões foram removidos
        assert "123.456.789-00" not in clean_cpf
        assert "[CPF removido]" in clean_cpf
        
        assert "(11) 99999-9999" not in clean_phone
        assert "[telefone removido]" in clean_phone
        
        assert "paciente@example.com" not in clean_email
        assert "[email removido]" in clean_email
    
    def test_news_content_filtering(self):
        """Testa filtro de conteúdo inadequado em notícias."""
        from src.utils.guardrails import SRAGGuardrails
        
        guardrails = SRAGGuardrails()
        
        # Artigo com conteúdo inadequado
        bad_article = {
            'title': 'Fake news sobre teoria da conspiração da vacina',
            'content': 'Este artigo promove desinformação...',
            'source': 'site-duvidoso.com'
        }
        
        # Artigo legítimo
        good_article = {
            'title': 'Aumento de casos de SRAG em SP',
            'content': 'Dados oficiais mostram crescimento...',
            'source': 'g1.com'
        }
        
        assert not guardrails._is_article_appropriate(bad_article)
        assert guardrails._is_article_appropriate(good_article)
    
    def test_report_signature_generation(self):
        """Testa geração de assinatura digital de relatórios."""
        from src.utils.guardrails import SRAGGuardrails
        
        guardrails = SRAGGuardrails()
        
        # Relatórios idênticos devem ter assinaturas idênticas
        report1 = {
            'metadata': {'report_date': '2024-01-01'},
            'metrics': {'mortality_rate': {'rate': 8.5}}
        }
        
        report2 = {
            'metadata': {'report_date': '2024-01-01'}, 
            'metrics': {'mortality_rate': {'rate': 8.5}}
        }
        
        sig1 = guardrails._generate_validation_signature(report1)
        sig2 = guardrails._generate_validation_signature(report2)
        
        assert sig1 == sig2
        assert sig1.startswith('SRAG-')
        assert len(sig1) > 10
    
    def test_input_validation_security(self):
        """Testa validação de entrada contra ataques."""
        from src.utils.guardrails import SRAGGuardrails
        
        guardrails = SRAGGuardrails()
        
        # Tentativas de injeção
        malicious_inputs = [
            {'report_date': "'; DROP TABLE users; --"},
            {'report_date': "<script>alert('xss')</script>"},
            {'report_date': "../../../etc/passwd"},
            {'report_date': "2024-01-01' OR '1'='1"}
        ]
        
        for malicious_input in malicious_inputs:
            with pytest.raises(ValueError):
                guardrails.validate_request(malicious_input)
    
    def test_sensitive_data_detection(self):
        """Testa detecção automática de dados sensíveis."""
        from src.config.settings import is_column_sensitive
        
        # Colunas que devem ser detectadas como sensíveis
        sensitive_columns = [
            'CPF', 'NM_PACIENT', 'IDENTIDADE', 
            'NU_TEL', 'CO_CARTAO_CNS'
        ]
        
        for col in sensitive_columns:
            assert is_column_sensitive(col)
        
        # Colunas que não devem ser detectadas como sensíveis
        non_sensitive_columns = [
            'DT_NOTIFIC', 'SG_UF_NOT', 'NU_IDADE_N', 
            'CS_SEXO', 'EVOLUCAO'
        ]
        
        for col in non_sensitive_columns:
            assert not is_column_sensitive(col)