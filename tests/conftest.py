import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os
import sys

# Adicionar src ao path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

@pytest.fixture
def sample_srag_data():
    """
    Fixture que cria dados SRAG de exemplo para testes.
    
    Returns:
        DataFrame com dados de teste
    """
    # Criar dados realistas de SRAG
    n_records = 1000
    
    # Gerar datas aleatórias nos últimos 2 anos
    start_date = datetime.now() - timedelta(days=730)
    dates = [start_date + timedelta(days=np.random.randint(0, 730)) for _ in range(n_records)]
    
    data = {
        'NU_NOTIFIC': [str(i) for i in range(n_records)],
        'DT_NOTIFIC': [d.strftime('%d/%m/%Y') for d in dates],
        'SG_UF_NOT': np.random.choice(['SP', 'RJ', 'MG', 'PR', 'RS'], n_records),
        'CO_MUN_NOT': np.random.randint(100000, 999999, n_records),
        'CS_SEXO': np.random.choice(['M', 'F', 'I'], n_records, p=[0.48, 0.50, 0.02]),
        'NU_IDADE_N': np.random.gamma(2, 30, n_records).astype(int),  # Distribuição realista de idades
        'DT_NASC': [d.strftime('%d/%m/%Y') for d in dates],
        'UTI': np.random.choice(['1', '2', '9'], n_records, p=[0.25, 0.70, 0.05]),
        'SUPORT_VEN': np.random.choice(['1', '2', '9'], n_records, p=[0.15, 0.80, 0.05]),
        'HOSPITAL': np.random.choice(['1', '2', '9'], n_records, p=[0.20, 0.75, 0.05]),
        'DT_INTERNA': [d.strftime('%d/%m/%Y') for d in dates],
        'EVOLUCAO': np.random.choice(['1', '2', '3', '9'], n_records, p=[0.75, 0.15, 0.05, 0.05]),
        'DT_EVOLUCA': [d.strftime('%d/%m/%Y') for d in dates],
        'VACINA_COV': np.random.choice(['1', '2', '9'], n_records, p=[0.70, 0.25, 0.05]),
        'DOSE_1_COV': np.random.choice(['1', '2', '9'], n_records, p=[0.85, 0.10, 0.05]),
        'DOSE_2_COV': np.random.choice(['1', '2', '9'], n_records, p=[0.75, 0.20, 0.05]),
        'DOSE_REF': np.random.choice(['1', '2', '9'], n_records, p=[0.60, 0.35, 0.05]),
        'FEBRE': np.random.choice(['1', '2', '9'], n_records, p=[0.80, 0.15, 0.05]),
        'TOSSE': np.random.choice(['1', '2', '9'], n_records, p=[0.75, 0.20, 0.05]),
        'GARGANTA': np.random.choice(['1', '2', '9'], n_records, p=[0.40, 0.55, 0.05]),
        'DISPNEIA': np.random.choice(['1', '2', '9'], n_records, p=[0.60, 0.35, 0.05]),
        'DESC_RESP': np.random.choice(['1', '2', '9'], n_records, p=[0.55, 0.40, 0.05]),
        'SATURACAO': np.random.choice(['1', '2', '9'], n_records, p=[0.45, 0.50, 0.05]),
        'DIARREIA': np.random.choice(['1', '2', '9'], n_records, p=[0.30, 0.65, 0.05]),
        'VOMITO': np.random.choice(['1', '2', '9'], n_records, p=[0.25, 0.70, 0.05]),
        'DT_COLETA': [d.strftime('%d/%m/%Y') for d in dates],
        'PCR_RESUL': np.random.choice(['1', '2', '9'], n_records, p=[0.20, 0.70, 0.10]),
        'DT_PCR': [d.strftime('%d/%m/%Y') for d in dates],
        'CLASSI_FIN': np.random.choice(['1', '2', '3', '4', '5'], n_records, p=[0.60, 0.10, 0.05, 0.15, 0.10]),
        'CRITERIO': np.random.choice(['1', '2', '3', '9'], n_records, p=[0.50, 0.30, 0.15, 0.05]),
        'DT_DIGITA': [d.strftime('%d/%m/%Y') for d in dates]
    }
    
    df = pd.DataFrame(data)
    
    # Adicionar alguns dados problemáticos para testar validações
    # Idades inválidas
    df.loc[np.random.choice(df.index, 10), 'NU_IDADE_N'] = np.random.choice([-5, 150, 999])
    
    # Valores categóricos inválidos
    df.loc[np.random.choice(df.index, 5), 'CS_SEXO'] = 'X'
    df.loc[np.random.choice(df.index, 5), 'EVOLUCAO'] = '5'
    
    # Alguns valores nulos
    null_indices = np.random.choice(df.index, 50)
    df.loc[null_indices, 'NU_IDADE_N'] = np.nan
    
    return df

@pytest.fixture
def sample_csv_file(sample_srag_data, tmp_path):
    """
    Fixture que cria arquivo CSV temporário para testes.
    
    Args:
        sample_srag_data: Dados de exemplo
        tmp_path: Diretório temporário do pytest
        
    Returns:
        Path para arquivo CSV criado
    """
    csv_file = tmp_path / "test_srag_data.csv"
    sample_srag_data.to_csv(csv_file, index=False, sep=';', encoding='latin-1')
    return str(csv_file)

@pytest.fixture
def mock_news_articles():
    """
    Fixture que cria artigos de notícias de exemplo.
    
    Returns:
        Lista de artigos simulados
    """
    return [
        {
            'title': 'Aumento de casos de SRAG preocupa autoridades',
            'summary': 'Dados mostram crescimento de 15% nos casos de síndrome respiratória',
            'link': 'https://example.com/news1',
            'published': datetime.now().isoformat(),
            'source': 'g1.com',
            'relevance_score': 8
        },
        {
            'title': 'Nova variante pode estar relacionada a surto respiratório',
            'summary': 'Especialistas investigam relação entre nova variante e internações em UTI',
            'link': 'https://example.com/news2', 
            'published': (datetime.now() - timedelta(days=2)).isoformat(),
            'source': 'folha.uol.com.br',
            'relevance_score': 9
        },
        {
            'title': 'Campanha de vacinação busca reduzir casos graves',
            'summary': 'Ministério da Saúde intensifica vacinação para conter SRAG',
            'link': 'https://example.com/news3',
            'published': (datetime.now() - timedelta(days=5)).isoformat(),
            'source': 'saude.gov.br',
            'relevance_score': 7
        }
    ]

@pytest.fixture
def temp_directories(tmp_path):
    """
    Fixture que cria estrutura de diretórios temporária.
    
    Args:
        tmp_path: Diretório temporário do pytest
        
    Returns:
        Dict com caminhos dos diretórios
    """
    dirs = {
        'data': tmp_path / 'data',
        'raw': tmp_path / 'data' / 'raw',
        'processed': tmp_path / 'data' / 'processed',
        'reports': tmp_path / 'data' / 'reports',
        'logs': tmp_path / 'logs',
        'cache': tmp_path / 'data' / 'cache'
    }
    
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
    
    return {k: str(v) for k, v in dirs.items()}