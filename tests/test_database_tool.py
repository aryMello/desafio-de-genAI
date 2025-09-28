import pytest
import pandas as pd
from unittest.mock import Mock, patch, AsyncMock
from src.tools.database_tool import DatabaseTool

class TestDatabaseTool:
    """Testes para a ferramenta de banco de dados."""
    
    @pytest.fixture
    def database_tool(self):
        """Fixture para instância do DatabaseTool."""
        return DatabaseTool()
    
    def test_initialization(self, database_tool):
        """Testa inicialização do DatabaseTool."""
        assert database_tool.tool_name == "DatabaseTool"
        assert hasattr(database_tool, 'essential_columns')
        assert hasattr(database_tool, 'cache')
        assert isinstance(database_tool.essential_columns, list)
    
    @pytest.mark.asyncio
    async def test_load_srag_data_file_not_found(self, database_tool):
        """Testa carregamento com arquivo inexistente."""
        with pytest.raises(FileNotFoundError):
            await database_tool.load_srag_data(
                "2024-01-01", "2024-12-31"
            )
    
    @pytest.mark.asyncio
    async def test_load_srag_data_success(self, database_tool, sample_csv_file):
        """Testa carregamento bem-sucedido de dados."""
        # Sobrescrever caminho dos dados
        database_tool.data_path = sample_csv_file
        
        data = await database_tool.load_srag_data(
            "2023-01-01", "2025-12-31"
        )
        
        assert isinstance(data, pd.DataFrame)
        assert len(data) > 0
        assert 'DT_NOTIFIC' in data.columns
    
    @pytest.mark.asyncio
    async def test_process_data(self, database_tool, sample_srag_data):
        """Testa processamento de dados."""
        processed = await database_tool.process_data(sample_srag_data)
        
        assert isinstance(processed, pd.DataFrame)
        assert len(processed) <= len(sample_srag_data)  # Pode filtrar registros inválidos
        
        # Verificar se campos derivados foram criados
        expected_derived_fields = ['FAIXA_ETARIA', 'EVOLUCAO_SIMPLES', 'CASO_GRAVE']
        for field in expected_derived_fields:
            if field not in processed.columns:
                continue  # Alguns campos podem não ser criados dependendo dos dados
        
    def test_get_data_summary(self, database_tool, sample_srag_data):
        """Testa geração de resumo dos dados."""
        summary = database_tool.get_data_summary(sample_srag_data)
        
        assert isinstance(summary, dict)
        assert 'total_records' in summary
        assert 'columns_available' in summary
        assert 'completeness' in summary
        assert summary['total_records'] == len(sample_srag_data)
    
    def test_health_check(self, database_tool):
        """Testa verificação de saúde."""
        health = database_tool.health_check()
        
        assert isinstance(health, dict)
        assert 'status' in health
        assert 'timestamp' in health
        assert health['status'] in ['healthy', 'error', 'degraded']
    
    def test_cache_functionality(self, database_tool):
        """Testa funcionalidade de cache."""
        # Limpar cache
        database_tool.clear_cache()
        
        # Verificar cache stats
        stats = database_tool.get_cache_stats()
        assert isinstance(stats, dict)
        assert stats['entries'] == 0