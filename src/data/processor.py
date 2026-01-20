import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import warnings
from pathlib import Path
import re

from ..config.settings import settings, SRAG_COLUMNS_MAPPING
from ..utils.logger import get_logger
from .validator import SRAGDataValidator

warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

logger = get_logger(__name__)

class SRAGDataProcessor:
    """
    Processador principal de dados SRAG do DataSUS.
    
    Responsabilidades:
    - Carregamento otimizado de dados CSV
    - Limpeza e padronização de dados
    - Criação de campos derivados
    - Aplicação de transformações
    - Validação de qualidade
    """
    
    def __init__(self):
        """Inicializa o processador de dados."""
        self.validator = SRAGDataValidator()
        self.settings = settings
        self.processing_stats = {
            'records_loaded': 0,
            'records_processed': 0,
            'records_filtered': 0,
            'columns_processed': 0,
            'processing_time': 0.0,
            'errors_found': 0,
            'warnings_found': 0
        }
        
        logger.info("SRAGDataProcessor inicializado")
    
    def load_and_process(
        self, 
        file_path: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Carrega e processa dados SRAG completos.
        
        Args:
            file_path: Caminho para arquivo CSV (usar padrão se None)
            start_date: Data de início do filtro (YYYY-MM-DD)
            end_date: Data de fim do filtro (YYYY-MM-DD)
            columns: Lista específica de colunas para carregar
            
        Returns:
            DataFrame com dados processados
            
        Raises:
            FileNotFoundError: Se arquivo não encontrado
            ValueError: Se parâmetros inválidos
        """
        start_time = datetime.now()
        
        try:
            # Usar caminho padrão se não especificado
            if file_path is None:
                file_path = self.settings.database.data_path
            
            logger.info(f"Iniciando processamento de dados: {file_path}")
            
            # Carregar dados brutos
            raw_data = self._load_raw_data(file_path, columns)
            self.processing_stats['records_loaded'] = len(raw_data)
            
            # Aplicar filtros de data se especificados
            if start_date or end_date:
                raw_data = self._filter_by_date_range(raw_data, start_date, end_date)
            
            # Processar dados
            processed_data = self._process_data_pipeline(raw_data)
            
            # Estatísticas finais
            processing_time = (datetime.now() - start_time).total_seconds()
            self.processing_stats['processing_time'] = processing_time
            self.processing_stats['records_processed'] = len(processed_data)
            
            logger.info(
                f"Processamento concluído: {len(processed_data)} registros em {processing_time:.2f}s"
            )
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Erro no processamento de dados: {e}")
            raise
    
    def _load_raw_data(self, file_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Carrega dados brutos do CSV usando processamento em chunks.
        
        Args:
            file_path: Caminho para o arquivo
            columns: Colunas específicas para carregar
            
        Returns:
            DataFrame com dados brutos
        """
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            # Determinar colunas a carregar
            if columns is None:
                columns = self._get_available_columns(file_path)
            
            logger.info(f"Carregando {len(columns)} colunas em chunks de {self.settings.database.chunk_size}")
            
            # Carregar dados em chunks para arquivos grandes
            chunks = []
            chunk_count = 0
            
            for chunk in pd.read_csv(
                file_path,
                encoding=self.settings.database.encoding,
                sep=self.settings.database.separator,
                chunksize=self.settings.database.chunk_size,
                usecols=columns,
                low_memory=False,
                dtype=str  # Carregar tudo como string inicialmente
            ):
                # Processamento básico do chunk
                processed_chunk = self._preprocess_chunk(chunk)
                chunks.append(processed_chunk)
                chunk_count += 1
                
                if chunk_count % 10 == 0:
                    logger.info(f"Processados {chunk_count} chunks...")
            
            # Concatenar todos os chunks
            if chunks:
                data = pd.concat(chunks, ignore_index=True)
                logger.info(f"Dados carregados: {len(data)} registros de {chunk_count} chunks")
            else:
                data = pd.DataFrame()
                logger.warning("Nenhum dado carregado")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro no carregamento de dados: {e}")
            raise
    
    def _get_available_columns(self, file_path: str) -> List[str]:
        """
        Identifica colunas disponíveis no arquivo CSV.
        
        Args:
            file_path: Caminho para o arquivo
            
        Returns:
            Lista de colunas disponíveis
        """
        try:
            # Ler apenas o cabeçalho
            sample = pd.read_csv(
                file_path,
                encoding=self.settings.database.encoding,
                sep=self.settings.database.separator,
                nrows=0
            )
            
            all_columns = list(sample.columns)
            essential_columns = self.settings.database.essential_columns
            
            # Verificar quais colunas essenciais estão disponíveis
            available_essential = [col for col in essential_columns if col in all_columns]
            
            # Adicionar outras colunas úteis que estejam disponíveis
            useful_additional = [
                'ID_AGRAVO', 'DT_SIN_PRI', 'SEM_PRI', 'SEM_NOT', 'DT_INTERNA',
                'ID_UNIDADE', 'COD_AGE_NOT', 'DT_DIGITA', 'HOSPITAL',
                'COD_FINALIZA', 'CLASSI_FIN', 'CRITERIO', 'PNEUMONIA',
                'OUTRO_SIN', 'OUTRO_DES', 'PUERPERA', 'CARDIOPATI',
                'HEMATOLOGI', 'SIND_DOWN', 'HEPATICA', 'ASMA',
                'DIABETES', 'NEUROLOGIC', 'PNEUMOPATI', 'IMUNODEPRE',
                'RENAL', 'OBESIDADE', 'OUT_MORBI', 'MORB_DESC',
                'ANTIVIRAL', 'TP_ANTIVIR', 'OUT_ANTIV', 'DT_ANTIVIR',
                'RAIO_X', 'AMOSTRA', 'DT_COLETA', 'TP_AMOSTRA'
            ]
            
            available_additional = [col for col in useful_additional if col in all_columns]
            
            # Combinar colunas
            selected_columns = available_essential + available_additional
            
            logger.info(f"Selecionadas {len(selected_columns)} colunas de {len(all_columns)} disponíveis")
            logger.info(f"Colunas essenciais disponíveis: {len(available_essential)}/{len(essential_columns)}")
            
            return selected_columns
            
        except Exception as e:
            logger.error(f"Erro ao verificar colunas: {e}")
            # Fallback para colunas essenciais apenas
            return self.settings.database.essential_columns
    
    def _preprocess_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Pré-processamento básico de um chunk de dados.
        
        Args:
            chunk: DataFrame chunk
            
        Returns:
            Chunk pré-processado
        """

        # Renomear colunas conforme mapeamento
        chunk.rename(columns=SRAG_COLUMNS_MAPPING, inplace=True)
        
        # Converter datas para datetime
        date_columns = [
            'data_notificacao', 'data_inicio_sintomas', 'data_internacao',
            'data_obito', 'data_coleta', 'data_digita', 'data_antiviral'
        ]
        
        for col in date_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce', format='%Y-%m-%d')
        
        return chunk

    def _filter_by_date_range(
        self, 
        data: pd.DataFrame, 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """
        Filtra dados por intervalo de datas.
        
        Args:
            data: DataFrame com dados
            start_date: Data de início (YYYY-MM-DD)
            end_date: Data de fim (YYYY-MM-DD)
            
        Returns:
            DataFrame filtrado
        """
        if start_date:
            start_dt = pd.to_datetime(start_date)
            data = data[data['data_notificacao'] >= start_dt]
        
        if end_date:
            end_dt = pd.to_datetime(end_date)
            data = data[data['data_notificacao'] <= end_dt]
        
        logger.info(f"Dados filtrados por intervalo de datas: {len(data)} registros restantes")
        return data

    def _process_data_pipeline(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica o pipeline completo de processamento de dados.
        
        Args:
            data: DataFrame com dados brutos
            
        Returns:
            DataFrame com dados processados
        """
        # Exemplo de etapas de processamento adicionais
        data = self._create_derived_fields(data)
        data = self.validator.validate(data)
        
        return data

    def _create_derived_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cria campos derivados necessários.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame com campos derivados
        """
        # Exemplo: calcular idade em anos a partir da data de nascimento
        if 'data_nascimento' in data.columns:
            data['idade'] = (pd.to_datetime('today') - data['data_nascimento']).dt.days // 365
        
        logger.info("Campos derivados criados")
        return data

    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do processamento.
        
        Returns:
            Dicionário com estatísticas
        """
        return self.processing_stats


