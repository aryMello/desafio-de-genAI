import pytest
import pandas as pd
from src.data.processor import SRAGDataProcessor

class TestSRAGDataProcessor:
    """Testes para o processador de dados SRAG."""
    
    @pytest.fixture
    def processor(self):
        """Fixture para instância do processador."""
        return SRAGDataProcessor()
    
    @pytest.mark.asyncio
    async def test_load_and_process(self, processor, sample_csv_file):
        """Testa carregamento e processamento completo."""
        result = await processor.load_and_process(
            file_path=sample_csv_file,
            start_date="2023-01-01",
            end_date="2025-12-31"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        
        # Verificar se campos derivados foram criados
        derived_fields = ['FAIXA_ETARIA', 'CASO_GRAVE', 'STATUS_VACINAL']
        for field in derived_fields:
            # Nem todos os campos podem estar presentes dependendo dos dados
            if field in result.columns:
                assert result[field].notna().any()
    
    def test_processing_stats(self, processor):
        """Testa coleta de estatísticas de processamento."""
        stats = processor.get_processing_summary()
        
        assert isinstance(stats, dict)
        assert 'processing_stats' in stats
        assert 'settings_used' in stats
        assert 'timestamp' in stats
    
    @pytest.mark.asyncio
    async def test_save_processed_data(self, processor, sample_srag_data, tmp_path):
        """Testa salvamento de dados processados."""
        output_file = tmp_path / "test_output.parquet"
        
        saved_path = processor.save_processed_data(
            sample_srag_data, 
            str(output_file),
            format='parquet'
        )
        
        assert saved_path == str(output_file)
        assert output_file.exists()
        
        # Verificar se dados foram salvos corretamente
        loaded_data = pd.read_parquet(saved_path)
        assert len(loaded_data) == len(sample_srag_data)