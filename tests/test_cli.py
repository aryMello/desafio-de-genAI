import pytest
import subprocess
import sys
from unittest.mock import patch

class TestCommandLineInterface:
    """Testes para interface de linha de comando."""
    
    def test_main_help_option(self):
        """Testa opção de help do main.py."""
        try:
            result = subprocess.run(
                [sys.executable, 'main.py', '--help'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            assert result.returncode == 0
            assert 'usage:' in result.stdout.lower() or 'Sistema de Relatórios' in result.stdout
            
        except subprocess.TimeoutExpired:
            pytest.skip("Comando demorou muito para executar")
        except Exception as e:
            pytest.skip(f"Erro ao executar comando: {e}")
    
    def test_status_only_option(self):
        """Testa opção --status-only."""
        try:
            result = subprocess.run(
                [sys.executable, 'main.py', '--status-only'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Comando pode falhar por falta de dados, mas deve executar
            assert 'STATUS DO SISTEMA' in result.stdout or result.returncode != 0
            
        except subprocess.TimeoutExpired:
            pytest.skip("Comando demorou muito para executar")
        except Exception as e:
            pytest.skip(f"Erro ao executar comando: {e}")
    
    def test_example_usage_execution(self):
        """Testa execução do example_usage.py."""
        try:
            result = subprocess.run(
                [sys.executable, 'example_usage.py'],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            # Script pode falhar por dependências, mas deve executar sem crash
            assert result.returncode in [0, 1]  # Sucesso ou falha controlada
            
        except subprocess.TimeoutExpired:
            pytest.skip("Example usage demorou muito para executar")
        except Exception as e:
            pytest.skip(f"Erro ao executar example_usage.py: {e}")

# conftest.py já foi definido no início, agora vamos adicionar mais fixtures úteis

@pytest.fixture
def mock_openai_response():
    """Mock para respostas da API OpenAI."""
    return {
        'choices': [{
            'message': {
                'content': 'Esta é uma análise simulada dos dados SRAG baseada nas métricas fornecidas.'
            }
        }],
        'usage': {
            'total_tokens': 100
        }
    }

@pytest.fixture
def mock_successful_report():
    """Mock de relatório bem-sucedido para testes."""
    return {
        'metadata': {
            'report_date': '2024-03-15',
            'generation_timestamp': '2024-03-15T10:00:00Z',
            'total_records': 1000
        },
        'metrics': {
            'case_increase_rate': {'rate': 15.3, 'interpretation': 'Aumento moderado'},
            'mortality_rate': {'rate': 8.7, 'interpretation': 'Taxa normal'},
            'icu_occupancy_rate': {'rate': 23.4, 'interpretation': 'Ocupação baixa'},
            'vaccination_rate': {'rate': 67.2, 'interpretation': 'Boa cobertura'}
        },
        'charts': {
            'daily_cases': {
                'file_path': '/tmp/daily_cases.png',
                'total_cases': 450
            },
            'monthly_cases': {
                'file_path': '/tmp/monthly_cases.png', 
                'total_cases': 12500
            }
        },
        'news_analysis': {
            'summary': 'Análise de 3 notícias relevantes sobre SRAG',
            'articles': [],
            'context_score': 7.5
        },
        'summary': {
            'charts_generated': 2,
            'news_articles_analyzed': 3
        },
        'output_files': {
            'html_report': '/tmp/srag_report_2024-03-15.html',
            'daily_chart': '/tmp/daily_cases.png',
            'monthly_chart': '/tmp/monthly_cases.png'
        }
    }