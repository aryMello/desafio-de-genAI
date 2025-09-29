import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
from pathlib import Path
import warnings

from ..utils.logger import get_logger
from ..utils.config import Config
from .base_tool import BaseTool

# Suprimir warnings do pandas
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

logger = get_logger(__name__)

class DatabaseTool(BaseTool):
    """
    Ferramenta para acesso aos dados SRAG do DataSUS.
    
    Responsabilidades:
    - Carregar dados do arquivo CSV
    - Processar e limpar dados
    - Aplicar filtros e validações
    - Gerenciar cache de dados
    """
    
    def __init__(self):
        """Inicializa a ferramenta de banco de dados."""
        super().__init__("DatabaseTool")
        
        self.config = Config()
        self.data_path = self.config.get('DATA_PATH', 'data/raw/srag_data.csv')
        self.cache = {}
        self.last_load_time = None
        
        # Definir colunas essenciais para análise SRAG
        self.essential_columns = [
            'DT_NOTIFIC',      # Data de notificação
            'SG_UF',           # Estado
            'ID_MUNICIP',      # Município
            'CS_SEXO',         # Sexo
            'NU_IDADE_N',      # Idade
            'UTI',             # Internação em UTI
            'SUPORT_VEN',      # Suporte ventilatório
            'EVOLUCAO',        # Evolução do caso
            'DT_EVOLUCA',      # Data da evolução
            'VACINA_COV',      # Vacinação COVID
            'DOSE_1_COV',      # 1ª dose
            'DOSE_2_COV',      # 2ª dose
            'DOSE_REF',        # Dose de reforço
            'FEBRE',           # Sintoma: febre
            'TOSSE',           # Sintoma: tosse
            'DISPNEIA',        # Sintoma: dispneia
            'DESC_RESP',       # Desconforto respiratório
            'SATURACAO',       # Saturação O2
            'DIARREIA',        # Sintoma: diarreia
            'VOMITO'           # Sintoma: vômito
        ]
        
        logger.info("DatabaseTool inicializada")
    
    async def load_srag_data(
        self, 
        start_date: str, 
        end_date: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Carrega dados SRAG do arquivo CSV para o período especificado.
        
        Args:
            start_date: Data de início (YYYY-MM-DD)
            end_date: Data de fim (YYYY-MM-DD)
            use_cache: Se deve usar cache quando disponível
            
        Returns:
            DataFrame com dados SRAG filtrados
            
        Raises:
            FileNotFoundError: Se arquivo não encontrado
            ValueError: Se formato de data inválido
        """
        try:
            logger.info(f"Carregando dados SRAG: {start_date} a {end_date}")
            
            # Verificar cache
            cache_key = f"{start_date}_{end_date}"
            if use_cache and cache_key in self.cache:
                cache_data = self.cache[cache_key]
                cache_age = datetime.now() - cache_data['timestamp']
                
                if cache_age < timedelta(hours=1):  # Cache válido por 1 hora
                    logger.info("Usando dados do cache")
                    return cache_data['data'].copy()
            
            # Verificar se arquivo existe
            if not os.path.exists(self.data_path):
                raise FileNotFoundError(f"Arquivo de dados não encontrado: {self.data_path}")
            
            # Carregar dados do CSV
            logger.info(f"Lendo arquivo CSV: {self.data_path}")
            
            # Carregar em chunks para arquivos grandes
            chunk_size = 10000
            chunks = []
            
            for chunk in pd.read_csv(
                self.data_path,
                encoding='utf-8',
                sep=';',
                chunksize=chunk_size,
                low_memory=False,
                usecols=self._get_available_columns()
            ):
                # Processar chunk
                processed_chunk = self._process_chunk(chunk, start_date, end_date)
                if not processed_chunk.empty:
                    chunks.append(processed_chunk)
            
            # Concatenar todos os chunks
            if chunks:
                data = pd.concat(chunks, ignore_index=True)
                logger.info(f"Dados carregados: {len(data)} registros")
            else:
                data = pd.DataFrame()
                logger.warning("Nenhum dado encontrado para o período especificado")
            
            # Armazenar no cache
            self.cache[cache_key] = {
                'data': data.copy(),
                'timestamp': datetime.now()
            }
            
            self.last_load_time = datetime.now()
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados SRAG: {e}")
            raise
    
    def _get_available_columns(self) -> List[str]:
        """
        Verifica quais colunas essenciais estão disponíveis no arquivo.
        
        Returns:
            Lista de colunas disponíveis
        """
        try:
            # Ler apenas o header para verificar colunas
            sample = pd.read_csv(
                self.data_path,
                encoding='utf-8',  # <-- MUDOU AQUI
                sep=';',
                nrows=0
            )
            
            available_cols = []
            for col in self.essential_columns:
                if col in sample.columns:
                    available_cols.append(col)
                else:
                    logger.warning(f"Coluna não encontrada: {col}")
            
            logger.info(f"Colunas disponíveis: {len(available_cols)} de {len(self.essential_columns)}")
            return available_cols if available_cols else None
            
        except Exception as e:
            logger.error(f"Erro ao verificar colunas: {e}")
            return None
        
    def _process_chunk(
        self, 
        chunk: pd.DataFrame, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        Processa um chunk de dados aplicando filtros e limpezas básicas.
        
        Args:
            chunk: DataFrame chunk
            start_date: Data de início do filtro
            end_date: Data de fim do filtro
            
        Returns:
            DataFrame processado
        """
        try:
            # Converter datas - CORRIGIDO PARA FORMATO ISO
            if 'DT_NOTIFIC' in chunk.columns:
                chunk['DT_NOTIFIC'] = pd.to_datetime(
                    chunk['DT_NOTIFIC'], 
                    format='%Y-%m-%d',  # <-- MUDOU AQUI
                    errors='coerce'
                )
                
                # Filtrar por período
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                
                mask = (chunk['DT_NOTIFIC'] >= start_dt) & (chunk['DT_NOTIFIC'] <= end_dt)
                chunk = chunk[mask]
            
            # Limpeza básica
            chunk = self._basic_cleaning(chunk)
            
            return chunk
            
        except Exception as e:
            logger.warning(f"Erro no processamento de chunk: {e}")
            return pd.DataFrame()
    
    def _basic_cleaning(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica limpeza básica nos dados.
        """
        try:
            # Remover registros com data de notificação inválida
            if 'DT_NOTIFIC' in data.columns:
                data = data.dropna(subset=['DT_NOTIFIC'])
            
            # Padronizar valores categóricos SEM substituir valores
            categorical_columns = ['CS_SEXO', 'SG_UF', 'UTI', 'EVOLUCAO', 'VACINA_COV']
            
            for col in categorical_columns:
                if col in data.columns:
                    # Apenas converter para string e limpar espaços
                    data[col] = data[col].astype(str).str.strip()
                    # Substituir apenas valores vazios
                    data[col] = data[col].replace(['nan', 'NaN', 'NAN', ''], np.nan)
            
            # Limpar idade
            if 'NU_IDADE_N' in data.columns:
                data['NU_IDADE_N'] = pd.to_numeric(data['NU_IDADE_N'], errors='coerce')
                data.loc[
                    (data['NU_IDADE_N'] < 0) | (data['NU_IDADE_N'] > 120), 
                    'NU_IDADE_N'
                ] = np.nan
            
            # Converter campos numéricos sem filtrar
            numeric_fields = [
                'UTI', 'SUPORT_VEN', 'FEBRE', 'TOSSE', 'DISPNEIA', 
                'DESC_RESP', 'SATURACAO', 'DIARREIA', 'VOMITO'
            ]
            
            for col in numeric_fields:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # DIAGNÓSTICO: Log dos valores encontrados
            if 'EVOLUCAO' in data.columns:
                evolucao_counts = data['EVOLUCAO'].value_counts(dropna=False).head(10)
                logger.info(f"Valores EVOLUCAO encontrados: {evolucao_counts.to_dict()}")
            
            if 'UTI' in data.columns:
                uti_counts = data['UTI'].value_counts(dropna=False).head(10)
                logger.info(f"Valores UTI encontrados: {uti_counts.to_dict()}")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro na limpeza básica: {e}", exc_info=True)
            return data
    
    async def process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Processa dados para análise, aplicando transformações avançadas.
        
        Args:
            raw_data: DataFrame com dados brutos
            
        Returns:
            DataFrame processado para análise
        """
        try:
            logger.info("Iniciando processamento avançado dos dados")
            
            data = raw_data.copy()
            
            # Criar campos derivados
            data = self._create_derived_fields(data)
            
            # Aplicar classificações
            data = self._apply_classifications(data)
            
            # Validar integridade
            data = self._validate_data_integrity(data)
            
            logger.info(f"Processamento concluído: {len(data)} registros válidos")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro no processamento de dados: {e}")
            raise
    
    def _create_derived_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cria campos derivados para análise.
        
        Args:
            data: DataFrame original
            
        Returns:
            DataFrame com campos derivados
        """
        try:
            # Campo de faixa etária
            if 'NU_IDADE_N' in data.columns:
                data['FAIXA_ETARIA'] = pd.cut(
                    data['NU_IDADE_N'], 
                    bins=[0, 12, 18, 60, 120], 
                    labels=['Criança', 'Adolescente', 'Adulto', 'Idoso'],
                    include_lowest=True
                )
            
            # Campo de evolução simplificada
            # CORRIGIDO: Aceitar string ou numérico
            if 'EVOLUCAO' in data.columns:
                # Converter para string para comparação
                evolucao_str = data['EVOLUCAO'].astype(str).str.strip()
                
                data['EVOLUCAO_SIMPLES'] = 'Ignorado'
                data.loc[evolucao_str == '1', 'EVOLUCAO_SIMPLES'] = 'Cura'
                data.loc[evolucao_str == '2', 'EVOLUCAO_SIMPLES'] = 'Óbito'
                data.loc[evolucao_str == '3', 'EVOLUCAO_SIMPLES'] = 'Óbito por outras causas'
                
                logger.info(f"Distribuição EVOLUCAO_SIMPLES: {data['EVOLUCAO_SIMPLES'].value_counts().to_dict()}")
            
            # Campo de gravidade baseado em UTI e suporte ventilatório
            # CORRIGIDO: Aceitar tanto 1 quanto '1'
            if 'UTI' in data.columns:
                uti_values = pd.to_numeric(data['UTI'], errors='coerce')
                data['CASO_GRAVE'] = (uti_values == 1).astype(int)
                logger.info(f"Casos graves (UTI=1): {data['CASO_GRAVE'].sum()}")
            
            # Campo de status vacinal consolidado
            # CORRIGIDO: Verificar múltiplos valores possíveis
            if 'VACINA_COV' in data.columns or any(col in data.columns for col in ['DOSE_1_COV', 'DOSE_2_COV']):
                data['STATUS_VACINAL'] = 'Não informado'
                
                # Se tem campo VACINA_COV
                if 'VACINA_COV' in data.columns:
                    vacina_str = data['VACINA_COV'].astype(str).str.strip()
                    data.loc[vacina_str == '2', 'STATUS_VACINAL'] = 'Não vacinado'
                    data.loc[vacina_str == '1', 'STATUS_VACINAL'] = 'Vacinado'
                
                # Se tem campos de dose
                if 'DOSE_1_COV' in data.columns:
                    dose1 = pd.to_numeric(data['DOSE_1_COV'], errors='coerce')
                    data.loc[dose1 == 1, 'STATUS_VACINAL'] = '1ª dose'
                
                if 'DOSE_2_COV' in data.columns:
                    dose2 = pd.to_numeric(data['DOSE_2_COV'], errors='coerce')
                    data.loc[dose2 == 1, 'STATUS_VACINAL'] = '2ª dose'
                
                if 'DOSE_REF' in data.columns:
                    dose_ref = pd.to_numeric(data['DOSE_REF'], errors='coerce')
                    data.loc[dose_ref == 1, 'STATUS_VACINAL'] = 'Dose reforço'
                
                logger.info(f"Distribuição STATUS_VACINAL: {data['STATUS_VACINAL'].value_counts().to_dict()}")
            
            # Campo de número de sintomas
            symptom_cols = ['FEBRE', 'TOSSE', 'DISPNEIA', 'DESC_RESP', 'DIARREIA', 'VOMITO']
            available_symptoms = [col for col in symptom_cols if col in data.columns]
            
            if available_symptoms:
                # Converter para numérico
                for col in available_symptoms:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                data['NUM_SINTOMAS'] = data[available_symptoms].eq(1).sum(axis=1)
            
            # Campo de semana epidemiológica
            if 'DT_NOTIFIC' in data.columns:
                data['ANO'] = data['DT_NOTIFIC'].dt.year
                data['MES'] = data['DT_NOTIFIC'].dt.month
                data['SEMANA_EPI'] = data['DT_NOTIFIC'].dt.isocalendar().week
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao criar campos derivados: {e}", exc_info=True)
            return data
    
    def _apply_classifications(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica classificações padronizadas aos dados.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame com classificações aplicadas
        """
        try:
            # Classificação de risco por idade
            if 'NU_IDADE_N' in data.columns:
                data['GRUPO_RISCO_IDADE'] = 'Baixo'
                data.loc[data['NU_IDADE_N'] >= 60, 'GRUPO_RISCO_IDADE'] = 'Alto'
                data.loc[data['NU_IDADE_N'] < 2, 'GRUPO_RISCO_IDADE'] = 'Alto'
            
            # Classificação de evolução
            # CORRIGIDO: Aceitar string ou numérico
            if 'EVOLUCAO' in data.columns:
                evolucao_str = data['EVOLUCAO'].astype(str).str.strip()
                data['TEVE_OBITO'] = evolucao_str.isin(['2', '3']).astype(int)
                logger.info(f"Total de óbitos identificados: {data['TEVE_OBITO'].sum()}")
            
            # Classificação de internação
            # CORRIGIDO: Aceitar tanto 1 quanto '1'
            if 'UTI' in data.columns:
                uti_numeric = pd.to_numeric(data['UTI'], errors='coerce')
                data['TEVE_UTI'] = (uti_numeric == 1).astype(int)
                logger.info(f"Total de internações em UTI: {data['TEVE_UTI'].sum()}")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao aplicar classificações: {e}", exc_info=True)
            return data
    
    def _validate_data_integrity(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Valida integridade dos dados e remove registros inconsistentes.
        
        Args:
            data: DataFrame a ser validado
            
        Returns:
            DataFrame validado
        """
        try:
            initial_count = len(data)
            
            # Remover registros sem data de notificação
            if 'DT_NOTIFIC' in data.columns:
                data = data.dropna(subset=['DT_NOTIFIC'])
            
            # Validar consistência de datas
            if 'DT_EVOLUCA' in data.columns and 'DT_NOTIFIC' in data.columns:
                # Data de evolução não pode ser anterior à notificação
                mask = data['DT_EVOLUCA'] >= data['DT_NOTIFIC']
                data = data[mask | data['DT_EVOLUCA'].isna()]
            
            # Remover duplicatas baseadas em critérios específicos
            if all(col in data.columns for col in ['DT_NOTIFIC', 'ID_MUNICIP', 'NU_IDADE_N']):
                data = data.drop_duplicates(
                    subset=['DT_NOTIFIC', 'ID_MUNICIP', 'NU_IDADE_N', 'CS_SEXO'],
                    keep='first'
                )
            
            final_count = len(data)
            removed_count = initial_count - final_count
            
            if removed_count > 0:
                logger.info(f"Validação removeu {removed_count} registros inconsistentes")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro na validação de integridade: {e}")
            return data
    
    def get_data_summary(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera resumo estatístico dos dados carregados.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            Dict com estatísticas resumidas
        """
        try:
            summary = {
                'total_records': len(data),
                'columns_available': list(data.columns),
                'date_range': {},
                'completeness': {},
                'statistics': {}
            }
            
            # Período dos dados
            if 'DT_NOTIFIC' in data.columns:
                dates = data['DT_NOTIFIC'].dropna()
                if len(dates) > 0:
                    summary['date_range'] = {
                        'start': dates.min().strftime('%Y-%m-%d'),
                        'end': dates.max().strftime('%Y-%m-%d'),
                        'total_days': (dates.max() - dates.min()).days
                    }
            
            # Completude das colunas principais
            for col in self.essential_columns:
                if col in data.columns:
                    non_null_count = data[col].count()
                    completeness_pct = (non_null_count / len(data)) * 100
                    summary['completeness'][col] = {
                        'non_null_count': int(non_null_count),
                        'completeness_percent': round(completeness_pct, 2)
                    }
            
            # Estatísticas específicas
            if 'NU_IDADE_N' in data.columns:
                age_data = data['NU_IDADE_N'].dropna()
                if len(age_data) > 0:
                    summary['statistics']['age'] = {
                        'mean': round(age_data.mean(), 1),
                        'median': round(age_data.median(), 1),
                        'min': int(age_data.min()),
                        'max': int(age_data.max())
                    }
            
            if 'SG_UF' in data.columns:
                uf_counts = data['SG_UF'].value_counts().head(5)
                summary['statistics']['top_states'] = uf_counts.to_dict()
            
            if 'EVOLUCAO' in data.columns:
                evolucao_counts = data['EVOLUCAO'].value_counts()
                summary['statistics']['evolution'] = evolucao_counts.to_dict()
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo: {e}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de banco de dados.
        
        Returns:
            Dict com status de saúde
        """
        try:
            status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'data_file': {
                    'path': self.data_path,
                    'exists': os.path.exists(self.data_path)
                },
                'cache': {
                    'entries': len(self.cache),
                    'last_load': self.last_load_time.isoformat() if self.last_load_time else None
                }
            }
            
            # Verificar arquivo de dados
            if not os.path.exists(self.data_path):
                status['status'] = 'error'
                status['error'] = f'Arquivo de dados não encontrado: {self.data_path}'
            else:
                # Informações do arquivo
                file_stats = os.stat(self.data_path)
                status['data_file'].update({
                    'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                    'modified': datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                })
                
                # Teste básico de leitura
                try:
                    test_data = pd.read_csv(
                        self.data_path,
                        encoding='utf-8',
                        sep=';',
                        nrows=5
                    )
                    status['data_file']['readable'] = True
                    status['data_file']['columns_count'] = len(test_data.columns)
                    
                except Exception as e:
                    status['status'] = 'error'
                    status['error'] = f'Erro ao ler arquivo: {str(e)}'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def clear_cache(self) -> None:
        """Limpa o cache de dados."""
        self.cache.clear()
        logger.info("Cache de dados limpo")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do cache.
        
        Returns:
            Dict com estatísticas do cache
        """
        stats = {
            'entries': len(self.cache),
            'keys': list(self.cache.keys()),
            'total_size_mb': 0
        }
        
        try:
            for key, value in self.cache.items():
                if 'data' in value:
                    # Estimar tamanho em MB
                    size_mb = value['data'].memory_usage(deep=True).sum() / (1024 * 1024)
                    stats['total_size_mb'] += size_mb
            
            stats['total_size_mb'] = round(stats['total_size_mb'], 2)
            
        except Exception as e:
            logger.error(f"Erro ao calcular estatísticas do cache: {e}")
        
        return stats
        