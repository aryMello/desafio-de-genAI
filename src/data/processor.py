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
        try:
            # 1. Converter data de notificação
            if 'DT_NOTIFIC' in chunk.columns:
                chunk['DT_NOTIFIC'] = pd.to_datetime(
                    chunk['DT_NOTIFIC'],
                    format='%Y-%m-%d',
                    errors='coerce'
                )
            
            # 2. Limpar strings - remover espaços e normalizar
            string_columns = chunk.select_dtypes(include=['object']).columns
            for col in string_columns:
                if col != 'DT_NOTIFIC':  # Não processar datas
                    chunk[col] = chunk[col].str.strip()
                    chunk[col] = chunk[col].replace(['', 'nan', 'NaN', 'NAN'], np.nan)
            
            return chunk
            
        except Exception as e:
            logger.warning(f"Erro no pré-processamento de chunk: {e}")
            return chunk
    
    def _filter_by_date_range(
        self, 
        data: pd.DataFrame, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Filtra dados por período de datas.
        
        Args:
            data: DataFrame com dados
            start_date: Data de início (YYYY-MM-DD)
            end_date: Data de fim (YYYY-MM-DD)
            
        Returns:
            DataFrame filtrado
        """
        try:
            if 'DT_NOTIFIC' not in data.columns:
                logger.warning("Coluna DT_NOTIFIC não encontrada para filtro de data")
                return data
            
            initial_count = len(data)
            mask = pd.Series([True] * len(data))
            
            if start_date:
                start_dt = pd.to_datetime(start_date)
                mask &= data['DT_NOTIFIC'] >= start_dt
            
            if end_date:
                end_dt = pd.to_datetime(end_date)
                mask &= data['DT_NOTIFIC'] <= end_dt
            
            filtered_data = data[mask]
            filtered_count = len(filtered_data)
            
            logger.info(
                f"Filtro de data aplicado: {filtered_count}/{initial_count} registros "
                f"({(filtered_count/initial_count)*100:.1f}%)"
            )
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"Erro no filtro de data: {e}")
            return data
    
    def _process_data_pipeline(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de processamento de dados.
        
        Args:
            data: DataFrame com dados brutos
            
        Returns:
            DataFrame processado
        """
        try:
            logger.info("Iniciando pipeline de processamento")
            
            # 1. Limpeza básica
            data = self._clean_basic_fields(data)
            
            # 2. Converter tipos de dados
            data = self._convert_data_types(data)
            
            # 3. Criar campos derivados
            data = self._create_derived_fields(data)
            
            # 4. Aplicar classificações
            data = self._apply_classifications(data)
            
            # 5. Validar dados
            data = self._validate_and_filter(data)
            
            # 6. Calcular campos adicionais
            data = self._calculate_additional_fields(data)
            
            logger.info("Pipeline de processamento concluído")
            return data
            
        except Exception as e:
            logger.error(f"Erro no pipeline de processamento: {e}")
            raise
    
    def _clean_basic_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Limpeza básica de campos comuns - VERSÃO CORRIGIDA.
        
        CORREÇÕES:
        1. Preserva valores originais de EVOLUCAO e UTI
        2. Não faz conversão para maiúsculas
        3. Log detalhado para diagnóstico
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame com campos limpos
        """
        try:
            # Remover registros sem data de notificação
            if 'DT_NOTIFIC' in data.columns:
                initial_count = len(data)
                data = data.dropna(subset=['DT_NOTIFIC'])
                removed = initial_count - len(data)
                if removed > 0:
                    logger.info(f"Removidos {removed} registros sem DT_NOTIFIC")
                    self.processing_stats['rows_removed'] += removed
            
            # Campos categóricos - APENAS limpar espaços, NÃO converter case
            categorical_fields = ['CS_SEXO', 'SG_UF', 'UTI', 'EVOLUCAO', 'VACINA_COV', 'HOSPITAL']
            
            for field in categorical_fields:
                if field in data.columns:
                    # Converter para string e limpar apenas espaços
                    data[field] = data[field].astype(str).str.strip()
                    
                    # Substituir valores claramente inválidos por NaN
                    invalid_values = ['nan', 'NaN', 'NAN', '', 'None', 'none']
                    data[field] = data[field].replace(invalid_values, np.nan)
                    
                    # Atualizar contador de erros
                    null_count = data[field].isna().sum()
                    if null_count > 0:
                        self.processing_stats['errors_found'] += null_count
            
            # Log detalhado para diagnóstico
            if 'EVOLUCAO' in data.columns:
                evolucao_values = data['EVOLUCAO'].value_counts(dropna=False).head(10)
                logger.info(f"Valores EVOLUCAO após limpeza: {evolucao_values.to_dict()}")
            
            if 'UTI' in data.columns:
                uti_values = data['UTI'].value_counts(dropna=False).head(10)
                logger.info(f"Valores UTI após limpeza: {uti_values.to_dict()}")
            
            if 'HOSPITAL' in data.columns:
                hospital_values = data['HOSPITAL'].value_counts(dropna=False).head(10)
                logger.info(f"Valores HOSPITAL após limpeza: {hospital_values.to_dict()}")
            
            # Incrementar contador de colunas processadas
            self.processing_stats['columns_processed'] += len(
                [f for f in categorical_fields if f in data.columns]
            )
            
            return data
            
        except Exception as e:
            logger.error(f"Erro na limpeza básica: {e}")
            return data
    
    def _convert_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Converte tipos de dados apropriadamente.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame com tipos convertidos
        """
        try:
            # Campos numéricos
            numeric_fields = [
                'NU_IDADE_N', 'UTI', 'SUPORT_VEN', 'FEBRE', 'TOSSE', 
                'DISPNEIA', 'DESC_RESP', 'SATURACAO', 'DIARREIA', 'VOMITO'
            ]
            
            for field in numeric_fields:
                if field in data.columns:
                    data[field] = pd.to_numeric(data[field], errors='coerce')
            
            # Validar idade
            if 'NU_IDADE_N' in data.columns:
                # Filtrar idades inválidas
                data.loc[
                    (data['NU_IDADE_N'] < 0) | (data['NU_IDADE_N'] > 120),
                    'NU_IDADE_N'
                ] = np.nan
            
            # Campos de data adicionais
            date_fields = ['DT_EVOLUCA', 'DT_SIN_PRI', 'DT_INTERNA', 'DT_ENTUTI', 'DT_SAIDUTI']
            
            for field in date_fields:
                if field in data.columns:
                    data[field] = pd.to_datetime(data[field], format='%Y-%m-%d', errors='coerce')
            
            return data
            
        except Exception as e:
            logger.error(f"Erro na conversão de tipos: {e}")
            return data
    
    def _create_derived_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Cria campos derivados para análise - VERSÃO CORRIGIDA.
        
        CORREÇÕES:
        1. Trata EVOLUCAO e UTI como strings para comparação segura
        2. Converte para numérico apenas quando necessário
        3. Validação mais robusta dos valores
        
        Args:
            data: DataFrame original
            
        Returns:
            DataFrame com campos derivados
        """
        try:
            logger.info("Criando campos derivados")
            
            # 1. Faixa etária
            if 'NU_IDADE_N' in data.columns:
                data['FAIXA_ETARIA'] = pd.cut(
                    data['NU_IDADE_N'],
                    bins=[0, 2, 12, 18, 60, 120],
                    labels=['Lactente', 'Criança', 'Adolescente', 'Adulto', 'Idoso'],
                    include_lowest=True
                )
            
            # 2. Evolução simplificada
            if 'EVOLUCAO' in data.columns:
                # Garantir que é string
                evolucao_str = data['EVOLUCAO'].astype(str).str.strip()
                
                # Inicializar com valor padrão
                data['EVOLUCAO_SIMPLES'] = 'Ignorado'
                
                # Mapear valores conhecidos
                data.loc[evolucao_str == '1', 'EVOLUCAO_SIMPLES'] = 'Cura'
                data.loc[evolucao_str == '2', 'EVOLUCAO_SIMPLES'] = 'Óbito SRAG'
                data.loc[evolucao_str == '3', 'EVOLUCAO_SIMPLES'] = 'Óbito outras causas'
                
                # Campo binário para óbito (2 ou 3)
                data['TEVE_OBITO'] = evolucao_str.isin(['2', '3']).astype(int)
                
                obitos = data['TEVE_OBITO'].sum()
                total = len(data)
                logger.info(f"Óbitos identificados: {obitos} ({(obitos/total*100):.2f}%)")
            
            # 3. Internação em UTI 
            if 'UTI' in data.columns:
                # Primeiro tenta como numérico
                uti_numeric = pd.to_numeric(data['UTI'], errors='coerce')
                
                # Se houver muitos NaN, tenta como string
                if uti_numeric.isna().sum() > len(data) * 0.5:
                    # Tratar como string
                    uti_str = data['UTI'].astype(str).str.strip()
                    data['TEVE_UTI'] = uti_str.isin(['1', '1.0']).astype(int)
                else:
                    # Tratar como numérico
                    data['TEVE_UTI'] = (uti_numeric == 1).astype(int)
                
                utis = data['TEVE_UTI'].sum()
                total = len(data)
                logger.info(f"Internações UTI identificadas: {utis} ({(utis/total*100):.2f}%)")
            
            # 4. Hospitalização
            if 'HOSPITAL' in data.columns and 'TEVE_HOSPITALIZACAO' not in data.columns:
                hospital_numeric = pd.to_numeric(data['HOSPITAL'], errors='coerce')
                
                if hospital_numeric.isna().sum() > len(data) * 0.5:
                    # Tratar como string
                    hospital_str = data['HOSPITAL'].astype(str).str.strip()
                    data['TEVE_HOSPITALIZACAO'] = hospital_str.isin(['1', '1.0']).astype(int)
                else:
                    # Tratar como numérico
                    data['TEVE_HOSPITALIZACAO'] = (hospital_numeric == 1).astype(int)
            
            # 5. Caso grave (UTI ou suporte ventilatório)
            if 'TEVE_UTI' in data.columns:
                data['CASO_GRAVE'] = data['TEVE_UTI']
                
                if 'SUPORT_VEN' in data.columns:
                    suport_ven = pd.to_numeric(data['SUPORT_VEN'], errors='coerce')
                    data['CASO_GRAVE'] = (
                        (data['TEVE_UTI'] == 1) | (suport_ven == 1)
                    ).astype(int)
            
            # 6. Status vacinal consolidado
            data['STATUS_VACINAL'] = 'Não informado'
            
            if 'VACINA_COV' in data.columns:
                vacina_str = data['VACINA_COV'].astype(str).str.strip()
                data.loc[vacina_str == '2', 'STATUS_VACINAL'] = 'Não vacinado'
                data.loc[vacina_str == '1', 'STATUS_VACINAL'] = 'Vacinado'
            
            # Refinar com informações de dose
            if 'DOSE_1_COV' in data.columns:
                dose1 = pd.to_numeric(data['DOSE_1_COV'], errors='coerce')
                data.loc[dose1 == 1, 'STATUS_VACINAL'] = '1ª dose'
            
            if 'DOSE_2_COV' in data.columns:
                dose2 = pd.to_numeric(data['DOSE_2_COV'], errors='coerce')
                data.loc[dose2 == 1, 'STATUS_VACINAL'] = '2ª dose'
            
            if 'DOSE_REF' in data.columns:
                dose_ref = pd.to_numeric(data['DOSE_REF'], errors='coerce')
                data.loc[dose_ref == 1, 'STATUS_VACINAL'] = 'Dose reforço'
            
            vacinal_dist = data['STATUS_VACINAL'].value_counts()
            logger.info(f"Distribuição STATUS_VACINAL: {vacinal_dist.to_dict()}")
            
            # 7. Contagem de sintomas
            symptom_cols = ['FEBRE', 'TOSSE', 'DISPNEIA', 'DESC_RESP', 'SATURACAO', 'DIARREIA', 'VOMITO']
            available_symptoms = [col for col in symptom_cols if col in data.columns]
            
            if available_symptoms:
                data['NUM_SINTOMAS'] = data[available_symptoms].eq(1).sum(axis=1)
            
            # 8. Campos temporais
            if 'DT_NOTIFIC' in data.columns:
                data['ANO'] = data['DT_NOTIFIC'].dt.year
                data['MES'] = data['DT_NOTIFIC'].dt.month
                data['DIA_SEMANA'] = data['DT_NOTIFIC'].dt.dayofweek
                data['SEMANA_EPI'] = data['DT_NOTIFIC'].dt.isocalendar().week
                data['TRIMESTRE'] = data['DT_NOTIFIC'].dt.quarter
            
            # 9. Tempo de internação (se disponível)
            if all(col in data.columns for col in ['DT_INTERNA', 'DT_EVOLUCA']):
                data['DIAS_INTERNACAO'] = (data['DT_EVOLUCA'] - data['DT_INTERNA']).dt.days
                # Filtrar valores negativos
                data.loc[data['DIAS_INTERNACAO'] < 0, 'DIAS_INTERNACAO'] = np.nan
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao criar campos derivados: {e}", exc_info=True)
            return data
    
    def _apply_classifications(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica classificações padronizadas.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame com classificações
        """
        try:
            # 1. Grupo de risco por idade
            if 'NU_IDADE_N' in data.columns:
                data['GRUPO_RISCO_IDADE'] = 'Baixo'
                data.loc[data['NU_IDADE_N'] >= 60, 'GRUPO_RISCO_IDADE'] = 'Alto'
                data.loc[data['NU_IDADE_N'] < 5, 'GRUPO_RISCO_IDADE'] = 'Alto'
            
            # 2. Classificação de gravidade
            if 'NUM_SINTOMAS' in data.columns:
                data['GRAVIDADE_SINTOMAS'] = 'Leve'
                data.loc[data['NUM_SINTOMAS'] >= 3, 'GRAVIDADE_SINTOMAS'] = 'Moderada'
                data.loc[data['NUM_SINTOMAS'] >= 5, 'GRAVIDADE_SINTOMAS'] = 'Grave'
            
            # 3. Desfecho
            if 'EVOLUCAO_SIMPLES' in data.columns:
                data['DESFECHO'] = data['EVOLUCAO_SIMPLES'].map({
                    'Cura': 'Recuperado',
                    'Óbito SRAG': 'Óbito',
                    'Óbito outras causas': 'Óbito',
                    'Ignorado': 'Em acompanhamento'
                }).fillna('Em acompanhamento')
            
            # 4. Faixa de tempo de evolução
            if 'DIAS_INTERNACAO' in data.columns:
                data['TEMPO_INTERNACAO'] = pd.cut(
                    data['DIAS_INTERNACAO'],
                    bins=[-np.inf, 7, 14, 30, np.inf],
                    labels=['Até 1 semana', '1-2 semanas', '2-4 semanas', 'Mais de 1 mês']
                )
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao aplicar classificações: {e}")
            return data
    
    def _validate_and_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Valida e filtra dados inconsistentes.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            DataFrame validado
        """
        try:
            initial_count = len(data)
            
            # 1. Validar consistência de datas
            if all(col in data.columns for col in ['DT_NOTIFIC', 'DT_EVOLUCA']):
                # Data de evolução não pode ser anterior à notificação
                invalid_dates = data['DT_EVOLUCA'] < data['DT_NOTIFIC']
                data.loc[invalid_dates, 'DT_EVOLUCA'] = np.nan
                
                invalid_count = invalid_dates.sum()
                if invalid_count > 0:
                    logger.warning(f"Corrigidas {invalid_count} datas de evolução inválidas")
            
            # 2. Remover duplicatas óbvias
            if all(col in data.columns for col in ['DT_NOTIFIC', 'ID_MUNICIP', 'NU_IDADE_N', 'CS_SEXO']):
                data = data.drop_duplicates(
                    subset=['DT_NOTIFIC', 'ID_MUNICIP', 'NU_IDADE_N', 'CS_SEXO'],
                    keep='first'
                )
            
            final_count = len(data)
            removed = initial_count - final_count
            
            if removed > 0:
                logger.info(f"Validação removeu {removed} registros ({(removed/initial_count)*100:.1f}%)")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro na validação: {e}")
            return data
    
    def _calculate_additional_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula campos adicionais úteis para análise.
        
        Args:
            data: DataFrame processado
            
        Returns:
            DataFrame com campos adicionais
        """
        try:
            # 1. Identificador único de caso (se não existir)
            if 'ID_CASO' not in data.columns:
                data['ID_CASO'] = range(1, len(data) + 1)
            
            # 2. Flag de comorbidades
            comorbidity_cols = [
                'CARDIOPATI', 'DIABETES', 'PNEUMOPATI', 'IMUNODEPRE',
                'RENAL', 'OBESIDADE', 'ASMA', 'HEPATICA'
            ]
            
            available_comorbidities = [col for col in comorbidity_cols if col in data.columns]
            
            if available_comorbidities:
                # Converter para numérico
                for col in available_comorbidities:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                
                # Contar comorbidades
                data['NUM_COMORBIDADES'] = data[available_comorbidities].eq(1).sum(axis=1)
                data['TEM_COMORBIDADE'] = (data['NUM_COMORBIDADES'] > 0).astype(int)
            
            # 3. Score de risco (simplificado)
            data['SCORE_RISCO'] = 0
            
            if 'GRUPO_RISCO_IDADE' in data.columns:
                data.loc[data['GRUPO_RISCO_IDADE'] == 'Alto', 'SCORE_RISCO'] += 1
            
            if 'TEM_COMORBIDADE' in data.columns:
                data.loc[data['TEM_COMORBIDADE'] == 1, 'SCORE_RISCO'] += 1
            
            if 'STATUS_VACINAL' in data.columns:
                data.loc[data['STATUS_VACINAL'] == 'Não vacinado', 'SCORE_RISCO'] += 1
            
            # 4. Classificação final de risco
            if 'SCORE_RISCO' in data.columns:
                data['CLASSIFICACAO_RISCO'] = 'Baixo'
                data.loc[data['SCORE_RISCO'] >= 2, 'CLASSIFICACAO_RISCO'] = 'Médio'
                data.loc[data['SCORE_RISCO'] >= 3, 'CLASSIFICACAO_RISCO'] = 'Alto'
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao calcular campos adicionais: {e}")
            return data
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do último processamento.
        
        CORREÇÃO: Adiciona 'settings_used' que os testes esperam
        
        Returns:
            Dict com estatísticas incluindo settings_used
        """
        return {
            'processing_stats': self.processing_stats,
            'settings_used': {
                'encoding': self.settings.database.encoding,
                'separator': self.settings.database.separator,
                'chunk_size': self.settings.database.chunk_size,
                'date_format': '%Y-%m-%d'
            },
            # Manter campos originais por compatibilidade
            **self.processing_stats
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Alias para get_processing_stats com formato esperado pelos testes.
        
        Returns:
            Dict com estatísticas no formato esperado
        """
        stats = self.processing_stats.copy()
        
        # Adicionar settings_used se não existir
        if 'settings_used' not in stats:
            stats['settings_used'] = {
                'encoding': self.settings.database.encoding,
                'separator': self.settings.database.separator,
                'chunk_size': self.settings.database.chunk_size,
                'date_format': '%Y-%m-%d'
            }
        
        return stats



    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Método simplificado para processar DataFrame já carregado.
        Este método é usado pelos testes unitários.
        
        Args:
            data: DataFrame com dados SRAG
            
        Returns:
            DataFrame processado
            
        Raises:
            ValueError: Se dados vazios ou None
        """
        if data is None or len(data) == 0:
            raise ValueError("No data to process")
        
        try:
            logger.info(f"Processando {len(data)} registros via método process()")
            
            # Resetar stats para este processamento
            self.processing_stats = {
                'initial_rows': len(data),
                'records_loaded': len(data),
                'records_processed': 0,
                'columns_processed': 0,
                'processing_time': 0.0,
                'errors_found': 0,
                'warnings_found': 0,
                'rows_removed': 0
            }
            
            start_time = datetime.now()
            
            # Executar pipeline de processamento
            processed_data = self._process_data_pipeline(data)
            
            # Atualizar estatísticas
            processing_time = (datetime.now() - start_time).total_seconds()
            self.processing_stats['processing_time'] = processing_time
            self.processing_stats['records_processed'] = len(processed_data)
            self.processing_stats['final_rows'] = len(processed_data)
            self.processing_stats['rows_removed'] = (
                self.processing_stats['initial_rows'] - len(processed_data)
            )
            
            logger.info(
                f"Processamento concluído: {len(processed_data)} registros em {processing_time:.2f}s"
            )
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Erro no método process(): {e}")
            raise


    def validate_required_columns(
    self, 
    data: pd.DataFrame, 
    required_columns: Optional[List[str]] = None
) -> None:
        """
        Valida presença de colunas requeridas.
        
        Args:
            data: DataFrame a validar
            required_columns: Lista de colunas obrigatórias (usa essenciais se None)
            
        Raises:
            ValueError: Se colunas obrigatórias não encontradas
        """
        if required_columns is None:
            required_columns = self.settings.database.essential_columns
        
        missing = [col for col in required_columns if col not in data.columns]
        
        if missing:
            available_cols = ', '.join(list(data.columns)[:10])
            raise ValueError(
                f"Coluna(s) {', '.join(missing)} não encontrada(s) nos dados. "
                f"Colunas disponíveis: {available_cols}..."
            )

    
    def generate_data_quality_report(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera relatório de qualidade dos dados.
        
        Args:
            data: DataFrame processado
            
        Returns:
            Dict com métricas de qualidade
        """
        try:
            report = {
                'total_records': len(data),
                'columns': len(data.columns),
                'completeness': {},
                'data_quality_scores': {},
                'warnings': []
            }
            
            # Completude por coluna
            for col in data.columns:
                non_null = data[col].count()
                completeness = (non_null / len(data)) * 100
                report['completeness'][col] = {
                    'non_null_count': int(non_null),
                    'completeness_percent': round(completeness, 2)
                }
                
                if completeness < 50:
                    report['warnings'].append(
                        f"Coluna '{col}' com baixa completude: {completeness:.1f}%"
                    )
            
            # Scores de qualidade
            essential_cols = ['DT_NOTIFIC', 'EVOLUCAO', 'UTI', 'NU_IDADE_N']
            available_essential = [col for col in essential_cols if col in data.columns]
            
            report['data_quality_scores']['essential_columns_present'] = (
                len(available_essential) / len(essential_cols)
            ) * 100
            
            # Completude média
            avg_completeness = np.mean([
                v['completeness_percent'] 
                for v in report['completeness'].values()
            ])
            report['data_quality_scores']['average_completeness'] = round(avg_completeness, 2)
            
            return report
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de qualidade: {e}")
            return {'error': str(e)}
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Alias para get_processing_stats."""
        return self.get_processing_stats()

    def save_processed_data(self, data: pd.DataFrame, output_path: str, format: str = 'csv') -> str:
        """Salva dados processados."""
        if format == 'parquet':
            try:
                data.to_parquet(output_path, index=False)
            except ImportError:
                logger.warning("pyarrow não disponível, salvando como CSV")
                output_path = output_path.replace('.parquet', '.csv')
                data.to_csv(output_path, index=False, sep=';')
        elif format == 'csv':
            data.to_csv(output_path, index=False, sep=';')
        else:
            raise ValueError(f"Formato não suportado: {format}")
        return output_path

    def _optimize_memory_usage(self, data: pd.DataFrame) -> pd.DataFrame:
        """Otimiza uso de memória."""
        for col in data.columns:
            if data[col].dtype == 'object':
                if data[col].nunique() / len(data[col]) < 0.5:
                    data[col] = data[col].astype('category')
        return data