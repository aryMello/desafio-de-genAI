import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
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
        
        # Auto-detect separator
        self.separator = self._detect_separator()
        
        # Definir colunas essenciais para análise SRAG
        self.essential_columns = [
            'DT_NOTIFIC',      # Data de notificação
            'SG_UF_NOT',       # Estado de notificação
            'CO_MUN_NOT',      # Código do município
            'CS_SEXO',         # Sexo
            'NU_IDADE_N',      # Idade
            'UTI',             # Internação em UTI
            'SUPORT_VEN',      # Suporte ventilatório
            'EVOLUCAO',        # Evolução do caso
            'DT_EVOLUCA',      # Data da evolução
            'HOSPITAL',        # Internação hospitalar
            'DT_INTERNA',      # Data de internação
            'VACINA_COV',      # Vacinação COVID (código)
            'DOSE_1_COV',      # 1ª dose COVID (DATA)
            'DOSE_2_COV',      # 2ª dose COVID (DATA)
            'DOSE_REF',        # Dose de reforço COVID (DATA)
            'FEBRE',           # Sintoma: febre
            'TOSSE',           # Sintoma: tosse
            'DISPNEIA',        # Sintoma: dispneia
            'DESC_RESP',       # Desconforto respiratório
            'SATURACAO',       # Saturação O2
            'DIARREIA',        # Sintoma: diarreia
            'VOMITO'           # Sintoma: vômito
        ]
        
        logger.info(f"DatabaseTool inicializada - Separador: '{self.separator}'")
    
    def _detect_separator(self) -> str:
        """
        Auto-detecta o separador do arquivo CSV (comma ou semicolon).
        
        Returns:
            String com o separador detectado (',' ou ';')
        """
        try:
            if not os.path.exists(self.data_path):
                logger.warning("Arquivo não encontrado, usando separador padrão ';'")
                return ';'
            
            # Ler primeira linha
            with open(self.data_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
            
            # Contar ocorrências de cada separador potencial
            comma_count = first_line.count(',')
            semicolon_count = first_line.count(';')
            
            # Usar o que aparecer mais vezes
            if semicolon_count > comma_count:
                logger.info("Separador detectado: semicolon (;)")
                return ';'
            else:
                logger.info("Separador detectado: comma (,)")
                return ','
                
        except Exception as e:
            logger.warning(f"Erro ao detectar separador: {e}. Usando padrão ';'")
            return ';'
    
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
            chunk_size = 50000
            chunks = []
            max_chunks = 20  # Limit to ~1M rows for performance
            chunk_count = 0
            
            try:
                for chunk in pd.read_csv(
                    self.data_path,
                    encoding='utf-8',
                    sep=self.separator,
                    chunksize=chunk_size,
                    on_bad_lines='skip',
                    engine='c',
                    usecols=self._get_available_columns()
                ):
                    chunk_count += 1
                    if chunk_count > max_chunks:
                        logger.warning(f"Limite de chunks ({max_chunks}) atingido. Carregando {len(chunks)} chunks.")
                        break
                    
                    # Processar chunk
                    processed_chunk = self._process_chunk(chunk, start_date, end_date)
                    if not processed_chunk.empty:
                        chunks.append(processed_chunk)
            except pd.errors.ParserError as e:
                logger.warning(f"Erro de parsing ao carregar CSV (linhas mal formatadas serão ignoradas): {e}")
                pass
            
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
                encoding='utf-8',
                sep=self.separator,
                nrows=0,
                on_bad_lines='skip',
                engine='python'
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
            # Parse all dates first
            chunk = self._parse_all_dates(chunk)
            
            # Filtrar por período usando DT_NOTIFIC
            if 'DT_NOTIFIC' in chunk.columns:
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
    
    def _parse_all_dates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Parse all date fields with correct formats.
        
        CRITICAL FIX: SRAG data has TWO different date formats:
        - ISO format (YYYY-MM-DD): Most dates like DT_NOTIFIC, DT_EVOLUCA
        - Brazilian format (DD/MM/YYYY): COVID vaccination dose dates
        """
        # ISO format dates (YYYY-MM-DD)
        iso_date_fields = [
            'DT_NOTIFIC', 'DT_SIN_PRI', 'DT_NASC', 'DT_EVOLUCA', 
            'DT_ENCERRA', 'DT_DIGITA', 'DT_INTERNA', 'DT_COLETA',
            'DT_PCR', 'DT_ENTUTI', 'DT_SAIDUTI', 'DT_RAIOX',
            'DT_ANTIVIR', 'DT_TOMO', 'DT_CO_SOR', 'DT_RES',
            'DT_TRT_COV', 'VG_DTRES', 'DT_UT_DOSE', 'DT_VAC_MAE'
        ]
        
        # Brazilian format dates (DD/MM/YYYY) - COVID vaccination doses
        brazilian_date_fields = [
            'DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF', 
            'DOSE_2REF', 'DOSE_ADIC', 'DOS_RE_BI',
            'DT_DOSEUNI', 'DT_1_DOSE', 'DT_2_DOSE'
        ]
        
        # Parse ISO dates
        for col in iso_date_fields:
            if col in data.columns:
                data[col] = pd.to_datetime(
                    data[col], 
                    format='%Y-%m-%d',
                    errors='coerce'
                )
        
        # Parse Brazilian format dates
        for col in brazilian_date_fields:
            if col in data.columns:
                data[col] = pd.to_datetime(
                    data[col],
                    format='%d/%m/%Y',  # CRITICAL FIX
                    errors='coerce'
                )
                
                # Log parsing results for debugging
                if col in ['DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF']:
                    valid_count = data[col].notna().sum()
                    if valid_count > 0:
                        logger.debug(f"{col}: {valid_count} valid dates parsed")
        
        return data
    
    def _basic_cleaning(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica limpeza básica nos dados com tipos padronizados.
        """
        try:
            # Remover registros com data de notificação inválida
            if 'DT_NOTIFIC' in data.columns:
                data = data.dropna(subset=['DT_NOTIFIC'])
            
            # Padronizar valores categóricos
            categorical_columns = {
                'CS_SEXO': ['M', 'F', 'I'],
                'EVOLUCAO': ['1', '2', '3', '9'],
                'UTI': ['1', '2', '9'],
                'SUPORT_VEN': ['1', '2', '9'],
                'VACINA_COV': ['1', '2', '9'],
                'HOSPITAL': ['1', '2', '9']
            }
            
            for col, valid_values in categorical_columns.items():
                if col in data.columns:
                    # Converter para string, remover .0, strip
                    data[col] = (data[col]
                                .astype(str)
                                .str.replace('.0', '', regex=False)
                                .str.strip())
                    
                    # Substituir valores inválidos por NaN
                    data[col] = data[col].apply(
                        lambda x: x if x in valid_values + ['nan', 'NaN', ''] else np.nan
                    )
            
            # Limpar idade
            if 'NU_IDADE_N' in data.columns:
                data['NU_IDADE_N'] = pd.to_numeric(data['NU_IDADE_N'], errors='coerce')
                data.loc[
                    (data['NU_IDADE_N'] < 0) | (data['NU_IDADE_N'] > 120), 
                    'NU_IDADE_N'
                ] = np.nan
            
            # Converter campos de sintomas para numérico
            symptom_fields = [
                'FEBRE', 'TOSSE', 'DISPNEIA', 'DESC_RESP', 
                'SATURACAO', 'DIARREIA', 'VOMITO'
            ]
            
            for col in symptom_fields:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # Log diagnóstico
            if 'EVOLUCAO' in data.columns:
                evolucao_counts = data['EVOLUCAO'].value_counts(dropna=False).head(10)
                logger.debug(f"Valores EVOLUCAO: {evolucao_counts.to_dict()}")
            
            if 'UTI' in data.columns:
                uti_counts = data['UTI'].value_counts(dropna=False).head(10)
                logger.debug(f"Valores UTI: {uti_counts.to_dict()}")
            
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
            
            # Ensure dates are parsed
            data = self._parse_all_dates(data)
            
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
            if 'EVOLUCAO' in data.columns:
                data['EVOLUCAO_SIMPLES'] = 'Ignorado'
                data.loc[data['EVOLUCAO'] == '1', 'EVOLUCAO_SIMPLES'] = 'Cura'
                data.loc[data['EVOLUCAO'] == '2', 'EVOLUCAO_SIMPLES'] = 'Óbito'
                data.loc[data['EVOLUCAO'] == '3', 'EVOLUCAO_SIMPLES'] = 'Óbito por outras causas'
                
                logger.debug(f"Distribuição EVOLUCAO_SIMPLES: {data['EVOLUCAO_SIMPLES'].value_counts().to_dict()}")
            
            # Campo de gravidade baseado em UTI
            if 'UTI' in data.columns:
                data['CASO_GRAVE'] = (data['UTI'] == '1').astype(int)
                logger.debug(f"Casos graves (UTI=1): {data['CASO_GRAVE'].sum()}")
            
            # Campo de status vacinal COVID baseado em DATAS de dose
            data = self._create_vaccination_status_field(data)
            
            # Campo de número de sintomas
            symptom_cols = ['FEBRE', 'TOSSE', 'DISPNEIA', 'DESC_RESP', 'DIARREIA', 'VOMITO']
            available_symptoms = [col for col in symptom_cols if col in data.columns]
            
            if available_symptoms:
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
    
    def _create_vaccination_status_field(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Create vaccination status field based on DOSE DATES, not VACINA_COV code.
        
        CRITICAL FIX: VACINA_COV is unreliable for COVID vaccination status.
        Use actual dose dates instead.
        """
        data['STATUS_VACINAL_COVID'] = 'Não informado'
        
        dose_cols = ['DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF', 'DOSE_2REF', 'DOSE_ADIC']
        available_doses = [col for col in dose_cols if col in data.columns]
        
        if not available_doses:
            logger.warning("No COVID dose date columns available")
            return data
        
        # Not vaccinated = no dose dates at all
        has_any_dose = data[available_doses].notna().any(axis=1)
        data.loc[~has_any_dose, 'STATUS_VACINAL_COVID'] = 'Não vacinado'
        
        # Has first dose
        if 'DOSE_1_COV' in data.columns:
            has_dose1 = data['DOSE_1_COV'].notna()
            data.loc[has_dose1 & ~data[['DOSE_2_COV']].notna().any(axis=1) if 'DOSE_2_COV' in data.columns else has_dose1, 
                    'STATUS_VACINAL_COVID'] = '1ª dose apenas'
        
        # Has second dose
        if 'DOSE_2_COV' in data.columns:
            has_dose2 = data['DOSE_2_COV'].notna()
            no_booster = ~data[['DOSE_REF', 'DOSE_2REF', 'DOSE_ADIC']].notna().any(axis=1) if any(c in data.columns for c in ['DOSE_REF', 'DOSE_2REF', 'DOSE_ADIC']) else True
            data.loc[has_dose2 & no_booster, 'STATUS_VACINAL_COVID'] = 'Esquema completo (2 doses)'
        
        # Has booster
        if 'DOSE_REF' in data.columns:
            has_booster = data['DOSE_REF'].notna()
            no_second_booster = ~data[['DOSE_2REF', 'DOSE_ADIC']].notna().any(axis=1) if any(c in data.columns for c in ['DOSE_2REF', 'DOSE_ADIC']) else True
            data.loc[has_booster & no_second_booster, 'STATUS_VACINAL_COVID'] = 'Com dose de reforço'
        
        # Has 2nd booster or additional
        if 'DOSE_2REF' in data.columns or 'DOSE_ADIC' in data.columns:
            has_extra = data[['DOSE_2REF', 'DOSE_ADIC']].notna().any(axis=1) if all(c in data.columns for c in ['DOSE_2REF', 'DOSE_ADIC']) else (
                data['DOSE_2REF'].notna() if 'DOSE_2REF' in data.columns else data['DOSE_ADIC'].notna()
            )
            data.loc[has_extra, 'STATUS_VACINAL_COVID'] = 'Com reforços adicionais'
        
        logger.info(f"Distribuição STATUS_VACINAL_COVID: {data['STATUS_VACINAL_COVID'].value_counts().to_dict()}")
        
        return data
    
    def _apply_classifications(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica classificações padronizadas aos dados.
        """
        try:
            # Classificação de risco por idade
            if 'NU_IDADE_N' in data.columns:
                data['GRUPO_RISCO_IDADE'] = 'Baixo'
                data.loc[data['NU_IDADE_N'] >= 60, 'GRUPO_RISCO_IDADE'] = 'Alto'
                data.loc[data['NU_IDADE_N'] < 2, 'GRUPO_RISCO_IDADE'] = 'Alto'
            
            # Classificação de evolução
            if 'EVOLUCAO' in data.columns:
                data['TEVE_OBITO'] = data['EVOLUCAO'].isin(['2', '3']).astype(int)
                logger.debug(f"Total de óbitos identificados: {data['TEVE_OBITO'].sum()}")
            
            # Classificação de internação
            if 'UTI' in data.columns:
                data['TEVE_UTI'] = (data['UTI'] == '1').astype(int)
                logger.debug(f"Total de internações em UTI: {data['TEVE_UTI'].sum()}")
            
            return data
            
        except Exception as e:
            logger.error(f"Erro ao aplicar classificações: {e}", exc_info=True)
            return data
    
    def _validate_data_integrity(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Valida integridade dos dados e remove registros inconsistentes.
        """
        try:
            initial_count = len(data)
            
            # Remover registros sem data de notificação
            if 'DT_NOTIFIC' in data.columns:
                data = data.dropna(subset=['DT_NOTIFIC'])
            
            # Validar consistência de datas
            if 'DT_EVOLUCA' in data.columns and 'DT_NOTIFIC' in data.columns:
                # Data de evolução não pode ser anterior à notificação
                mask = (data['DT_EVOLUCA'] >= data['DT_NOTIFIC']) | data['DT_EVOLUCA'].isna()
                data = data[mask]
            
            # Remover duplicatas baseadas em critérios específicos
            if all(col in data.columns for col in ['DT_NOTIFIC', 'CO_MUN_NOT', 'NU_IDADE_N']):
                data = data.drop_duplicates(
                    subset=['DT_NOTIFIC', 'CO_MUN_NOT', 'NU_IDADE_N', 'CS_SEXO'],
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
            
            if 'SG_UF_NOT' in data.columns:
                uf_counts = data['SG_UF_NOT'].value_counts().head(5)
                summary['statistics']['top_states'] = uf_counts.to_dict()
            
            if 'EVOLUCAO' in data.columns:
                evolucao_counts = data['EVOLUCAO'].value_counts()
                summary['statistics']['evolution'] = evolucao_counts.to_dict()
            
            # Vaccination statistics
            if 'STATUS_VACINAL_COVID' in data.columns:
                vac_counts = data['STATUS_VACINAL_COVID'].value_counts()
                summary['statistics']['vaccination_status'] = vac_counts.to_dict()
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo: {e}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de banco de dados.
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
                        sep=self.separator,
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