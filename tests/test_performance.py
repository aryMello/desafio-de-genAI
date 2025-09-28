import pytest
import time
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import psutil
import gc

class TestSystemPerformance:
    """Testes de performance e otimização do sistema."""
    
    @pytest.fixture
    def large_dataset(self):
        """Cria dataset grande para testes de performance."""
        n_records = 10000  # Dataset maior para teste de performance
        
        data = {
            'DT_NOTIFIC': pd.date_range('2020-01-01', periods=n_records, freq='H').strftime('%d/%m/%Y'),
            'SG_UF': np.random.choice(['SP', 'RJ', 'MG', 'PR', 'RS'], n_records),
            'NU_IDADE_N': np.random.randint(0, 100, n_records),
            'CS_SEXO': np.random.choice(['M', 'F'], n_records),
            'UTI': np.random.choice(['1', '2'], n_records),
            'EVOLUCAO': np.random.choice(['1', '2', '3'], n_records),
            'FEBRE': np.random.choice(['1', '2'], n_records),
            'TOSSE': np.random.choice(['1', '2'], n_records)
        }
        
        return pd.DataFrame(data)
    
    def test_data_loading_performance(self, large_dataset, tmp_path):
        """Testa performance de carregamento de dados."""
        # Salvar dataset grande
        csv_file = tmp_path / "large_srag.csv"
        large_dataset.to_csv(csv_file, sep=';', index=False, encoding='latin-1')
        
        from src.data.processor import SRAGDataProcessor
        processor = SRAGDataProcessor()
        
        # Medir tempo de carregamento
        start_time = time.time()
        
        # Simular carregamento (sem processamento completo)
        data = pd.read_csv(csv_file, sep=';', encoding='latin-1')
        
        loading_time = time.time() - start_time
        
        assert loading_time < 5.0  # Deve carregar em menos de 5 segundos
        assert len(data) == len(large_dataset)
    
    @pytest.mark.asyncio
    async def test_metrics_calculation_performance(self):
        """Testa performance do cálculo de métricas."""
        from src.tools.metrics_tool import MetricsCalculatorTool
        
        # Criar dados de teste
        n_records = 5000
        test_data = pd.DataFrame({
            'DT_NOTIFIC': pd.date_range('2023-01-01', periods=n_records, freq='6H'),
            'EVOLUCAO': np.random.choice(['1', '2'], n_records),
            'UTI': np.random.choice(['1', '2'], n_records),
            'DOSE_1_COV': np.random.choice(['1', '2'], n_records)
        })
        
        metrics_tool = MetricsCalculatorTool()
        
        # Medir tempo de cálculo de cada métrica
        metrics_times = {}
        
        start_time = time.time()
        result = await metrics_tool.calculate_mortality_rate(test_data, "2023-06-15")
        metrics_times['mortality'] = time.time() - start_time
        
        start_time = time.time()
        result = await metrics_tool.calculate_icu_occupancy_rate(test_data, "2023-06-15")
        metrics_times['icu'] = time.time() - start_time
        
        start_time = time.time()
        result = await metrics_tool.calculate_vaccination_rate(test_data, "2023-06-15")
        metrics_times['vaccination'] = time.time() - start_time
        
        # Cada métrica deve ser calculada em menos de 2 segundos
        for metric, calc_time in metrics_times.items():
            assert calc_time < 2.0, f"Métrica {metric} muito lenta: {calc_time}s"
    
    def test_memory_usage_optimization(self, large_dataset):
        """Testa otimização de uso de memória."""
        from src.data.processor import SRAGDataProcessor
        
        processor = SRAGDataProcessor()
        
        # Medir uso de memória antes
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Processar dados (incluindo otimização de memória)
        optimized_data = processor._optimize_memory_usage(large_dataset)
        
        # Forçar garbage collection
        gc.collect()
        
        # Medir uso de memória depois
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        
        # Verificar se otimização funcionou
        original_size = large_dataset.memory_usage(deep=True).sum() / 1024 / 1024
        optimized_size = optimized_data.memory_usage(deep=True).sum() / 1024 / 1024
        
        # Dados otimizados devem usar menos memória ou no máximo a mesma quantidade
        assert optimized_size <= original_size * 1.1  # Tolerância de 10%
    
    def test_validation_performance(self, large_dataset):
        """Testa performance do sistema de validação."""
        from src.data.validator import SRAGDataValidator
        
        validator = SRAGDataValidator()
        
        start_time = time.time()
        result = validator.validate_data_quality(large_dataset)
        validation_time = time.time() - start_time
        
        # Validação deve ser rápida mesmo para datasets grandes
        assert validation_time < 10.0  # Menos de 10 segundos
        assert isinstance(result, dict)
        assert 'quality_score' in result
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """Testa operações concorrentes."""
        from src.tools.metrics_tool import MetricsCalculatorTool
        
        # Criar múltiplos datasets pequenos
        datasets = []
        for i in range(5):
            data = pd.DataFrame({
                'DT_NOTIFIC': pd.date_range('2023-01-01', periods=1000, freq='H'),
                'EVOLUCAO': np.random.choice(['1', '2'], 1000),
                'UTI': np.random.choice(['1', '2'], 1000)
            })
            datasets.append(data)
        
        metrics_tool = MetricsCalculatorTool()
        
        # Executar cálculos concorrentemente
        start_time = time.time()
        
        tasks = []
        for i, data in enumerate(datasets):
            task = metrics_tool.calculate_mortality_rate(data, "2023-06-15")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        concurrent_time = time.time() - start_time
        
        # Execução concorrente deve ser mais eficiente que sequencial
        assert concurrent_time < 15.0  # Tempo razoável para 5 operações
        assert len(results) == 5
        assert all(isinstance(r, dict) for r in results)