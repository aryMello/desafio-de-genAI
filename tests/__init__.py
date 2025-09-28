# tests/__init__.py
"""
Testes para o Sistema de Relatórios SRAG - ABC HealthCare Inc.

Este módulo contém todos os testes automatizados para validar o funcionamento
do sistema de relatórios automatizados baseado em IA Generativa.

"""

import sys
import os
from pathlib import Path

# Configurações globais para testes
TEST_DATA_DIR = Path(__file__).parent / "test_data"
TEST_OUTPUT_DIR = Path(__file__).parent / "test_output"

# Garantir que src está no path
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Configurações de teste para evitar conflitos com produção
os.environ.setdefault("TESTING", "true")
os.environ.setdefault("LOG_LEVEL", "WARNING")  # Menos verbose nos testes
os.environ.setdefault("DISABLE_EXTERNAL_APIS", "true")  # Desabilitar APIs reais

# Versão dos testes (para tracking de compatibilidade)
__test_version__ = "1.0.0"
__compatible_system_version__ = "1.0.0"

# Utilitários para testes
def get_test_data_path(filename: str) -> Path:
    """
    Retorna caminho para arquivo de dados de teste.
    
    Args:
        filename: Nome do arquivo de teste
        
    Returns:
        Path para o arquivo
    """
    return TEST_DATA_DIR / filename

def get_test_output_path(filename: str) -> Path:
    """
    Retorna caminho para arquivo de saída de teste.
    
    Args:
        filename: Nome do arquivo de saída
        
    Returns:
        Path para o arquivo
    """
    TEST_OUTPUT_DIR.mkdir(exist_ok=True)
    return TEST_OUTPUT_DIR / filename

def cleanup_test_files():
    """Remove arquivos temporários de teste."""
    import shutil
    
    if TEST_OUTPUT_DIR.exists():
        shutil.rmtree(TEST_OUTPUT_DIR)
    
    # Limpar outros diretórios temporários se existirem
    temp_dirs = [
        PROJECT_ROOT / "test_temp",
        PROJECT_ROOT / "data" / "test",
        PROJECT_ROOT / "logs" / "test"
    ]
    
    for temp_dir in temp_dirs:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

# Configuração de pytest programática (backup do pytest.ini)
def pytest_configure(config):
    """Configuração programática do pytest."""
    config.addinivalue_line(
        "markers", 
        "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", 
        "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers",
        "performance: marks tests as performance tests"
    )

def pytest_sessionfinish(session, exitstatus):
    """Cleanup após todas as sessões de teste."""
    cleanup_test_files()

# Validações de ambiente para testes
def validate_test_environment():
    """Valida se ambiente está configurado corretamente para testes."""
    issues = []
    
    # Verificar Python version
    if sys.version_info < (3, 9):
        issues.append("Python 3.9+ requerido para testes")
    
    # Verificar se src está acessível
    if not SRC_DIR.exists():
        issues.append(f"Diretório src não encontrado: {SRC_DIR}")
    
    # Verificar dependências críticas
    try:
        import pytest
        import pandas
        import numpy
    except ImportError as e:
        issues.append(f"Dependência faltando: {e}")
    
    if issues:
        raise EnvironmentError(
            "Ambiente de teste inválido:\n" + "\n".join(f"- {issue}" for issue in issues)
        )

# Executar validação ao importar
try:
    validate_test_environment()
except EnvironmentError as e:
    print(f"AVISO: {e}")

# Constantes úteis para testes
class TestConstants:
    """Constantes utilizadas nos testes."""
    
    # Dados de exemplo
    SAMPLE_REPORT_DATE = "2024-03-15"
    SAMPLE_START_DATE = "2023-01-01"
    SAMPLE_END_DATE = "2024-12-31"
    
    # Métricas esperadas (ranges aceitáveis)
    MORTALITY_RATE_RANGE = (0.0, 50.0)
    ICU_RATE_RANGE = (0.0, 100.0)
    VACCINATION_RATE_RANGE = (0.0, 100.0)
    CASE_INCREASE_RATE_RANGE = (-100.0, 1000.0)
    
    # Timeouts para testes
    FAST_TIMEOUT = 5.0  # segundos
    NORMAL_TIMEOUT = 30.0
    SLOW_TIMEOUT = 120.0
    
    # Tamanhos de dados para testes
    SMALL_DATASET_SIZE = 100
    MEDIUM_DATASET_SIZE = 1000  
    LARGE_DATASET_SIZE = 10000
    
    # Caminhos relativos
    DATA_DIR = "data"
    LOGS_DIR = "logs"
    REPORTS_DIR = "data/reports"
    
    # Estados brasileiros para testes
    VALID_STATES = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    # Valores categóricos válidos para SRAG
    VALID_SEXO = ['M', 'F', 'I']
    VALID_EVOLUCAO = ['1', '2', '3', '9']
    VALID_UTI = ['1', '2', '9']
    VALID_BINARY = ['1', '2', '9']

# Helpers para asserções customizadas
class TestHelpers:
    """Métodos auxiliares para testes."""
    
    @staticmethod
    def assert_metric_result(result: dict, expected_keys: list = None):
        """
        Valida estrutura padrão de resultado de métrica.
        
        Args:
            result: Dict com resultado da métrica
            expected_keys: Chaves esperadas adicionais
        """
        if expected_keys is None:
            expected_keys = []
            
        base_keys = ['rate', 'interpretation', 'calculation_metadata']
        all_expected_keys = base_keys + expected_keys
        
        assert isinstance(result, dict), "Resultado deve ser dict"
        
        for key in all_expected_keys:
            assert key in result, f"Chave '{key}' faltando no resultado"
        
        assert isinstance(result['rate'], (int, float)), "Rate deve ser numérico"
        assert isinstance(result['interpretation'], str), "Interpretation deve ser string"
        assert len(result['interpretation']) > 0, "Interpretation não pode estar vazia"
    
    @staticmethod
    def assert_dataframe_valid(df: 'pandas.DataFrame', min_rows: int = 1):
        """
        Valida DataFrame básico.
        
        Args:
            df: DataFrame para validar
            min_rows: Número mínimo de linhas
        """
        import pandas as pd
        
        assert isinstance(df, pd.DataFrame), "Deve ser DataFrame"
        assert len(df) >= min_rows, f"DataFrame deve ter pelo menos {min_rows} linhas"
        assert len(df.columns) > 0, "DataFrame deve ter colunas"
    
    @staticmethod
    def assert_file_exists(file_path: str, should_exist: bool = True):
        """
        Verifica existência de arquivo.
        
        Args:
            file_path: Caminho do arquivo
            should_exist: Se arquivo deve existir
        """
        path_obj = Path(file_path)
        
        if should_exist:
            assert path_obj.exists(), f"Arquivo deve existir: {file_path}"
        else:
            assert not path_obj.exists(), f"Arquivo não deveria existir: {file_path}"
    
    @staticmethod
    def assert_performance_acceptable(execution_time: float, max_time: float):
        """
        Verifica se performance está aceitável.
        
        Args:
            execution_time: Tempo de execução em segundos
            max_time: Tempo máximo aceitável
        """
        assert execution_time <= max_time, (
            f"Performance inaceitável: {execution_time:.2f}s > {max_time}s"
        )
    
    @staticmethod
    def assert_no_sensitive_data(data, sensitive_patterns: list = None):
        """
        Verifica se não há dados sensíveis.
        
        Args:
            data: Dados para verificar (dict, DataFrame, string)
            sensitive_patterns: Padrões sensíveis adicionais
        """
        import re
        
        if sensitive_patterns is None:
            sensitive_patterns = [
                r'\d{3}\.\d{3}\.\d{3}-\d{2}',  # CPF
                r'\(\d{2}\)\s?\d{4,5}-?\d{4}',  # Telefone
                r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'  # Email
            ]
        
        data_str = str(data)
        
        for pattern in sensitive_patterns:
            matches = re.findall(pattern, data_str)
            assert not matches, f"Dados sensíveis encontrados: {matches}"

# Disponibilizar utilitários no namespace
__all__ = [
    'TEST_DATA_DIR',
    'TEST_OUTPUT_DIR', 
    'get_test_data_path',
    'get_test_output_path',
    'cleanup_test_files',
    'validate_test_environment',
    'TestConstants',
    'TestHelpers'
]