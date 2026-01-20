import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
import warnings
from collections import Counter

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

class SRAGDataValidator:
    """
    Validador de qualidade e consistência para dados SRAG.
    
    Funcionalidades:
    - Validação de integridade de dados
    - Detecção de inconsistências
    - Verificação de completude
    - Identificação de outliers
    - Validação de regras de negócio
    """
    
    def __init__(self):
        """Inicializa o validador de dados."""
        self.settings = settings
        self.validation_rules = self._initialize_validation_rules()
        self.validation_stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'warnings_generated': 0
        }
        
        logger.info("SRAGDataValidator inicializado")
    
    def _initialize_validation_rules(self) -> Dict[str, Any]:
        """
        Inicializa regras de validação específicas para dados SRAG.
        
        Returns:
            Dict com regras de validação
        """
        return {
            # Validações de campo obrigatório
            'required_fields': ['DT_NOTIFIC', 'NU_NOTIFIC'],
            
            # Validações de formato de data
            'date_fields': ['DT_NOTIFIC', 'DT_EVOLUCA', 'DT_INTERNA', 'DT_SIN_PRI', 'DT_NASC', 'DT_COLETA', 'DT_PCR', 'DT_DIGITA'],
            
            # Validações de range para campos numéricos
            'numeric_ranges': {
                'NU_IDADE_N': {'min': 0, 'max': 120}
            },
            
            # Validações de valores categóricos válidos
            'categorical_values': {
                'CS_SEXO': ['M', 'F', 'I', ''],
                'EVOLUCAO': ['1', '2', '3', '9', ''],
                'UTI': ['1', '2', '9', ''],
                'SUPORT_VEN': ['1', '2', '9', ''],
                'VACINA_COV': ['1', '2', '9', ''],
                'DOSE_1_COV': ['1', '2', '9', ''],
                'DOSE_2_COV': ['1', '2', '9', ''],
                'DOSE_REF': ['1', '2', '9', ''],
                'HOSPITAL': ['1', '2', '9', '']
            },
            
            # Validações de consistência temporal
            'temporal_consistency': [
                ('DT_NOTIFIC', 'DT_EVOLUCA'),  # Evolução não pode ser antes da notificação
                ('DT_SIN_PRI', 'DT_NOTIFIC'),  # Sintomas não podem ser depois da notificação
                ('DT_INTERNA', 'DT_NOTIFIC')   # Internação não pode ser antes da notificação
            ],
            
            # Validações de lógica de negócio
            'business_rules': [
                {
                    'name': 'uti_requires_internment',
                    'description': 'UTI=1 requer internação hospitalar',
                    'condition': lambda df: (df.get('UTI') == '1') & (df.get('HOSPITAL', '2') == '2')
                },
                {
                    'name': 'death_without_symptoms',
                    'description': 'Óbito sem sintomas registrados',
                    'condition': lambda df: self._check_death_without_symptoms(df)
                },
                {
                    'name': 'vaccination_consistency',
                    'description': 'Consistência nas doses de vacina',
                    'condition': lambda df: self._check_vaccination_consistency(df)
                }
            ],
            
            # Limites para detecção de outliers
            'outlier_detection': {
                'NU_IDADE_N': {'method': 'iqr', 'factor': 1.5}
            }
        }
    
    def validate_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Executa validação completa de qualidade dos dados.
        
        Args:
            data: DataFrame com dados SRAG
            
        Returns:
            Dict com resultado da validação
        """
        try:
            logger.info(f"Iniciando validação de qualidade: {len(data)} registros")
            
            validation_result = {
                'passed': True,
                'total_records': len(data),
                'errors': [],
                'warnings': [],
                'statistics': {},
                'quality_score': 0.0,
                'validation_timestamp': datetime.now().isoformat()
            }
            
            # Executar diferentes tipos de validação
            validation_result.update(self._validate_completeness(data))
            validation_result.update(self._validate_data_types(data))
            validation_result.update(self._validate_ranges(data))
            validation_result.update(self._validate_categorical_values(data))
            validation_result.update(self._validate_temporal_consistency(data))
            validation_result.update(self._validate_business_rules(data))
            validation_result.update(self._detect_outliers(data))
            validation_result.update(self._validate_duplicates(data))
            
            # Calcular score de qualidade
            validation_result['quality_score'] = self._calculate_quality_score(validation_result)
            
            # Atualizar estatísticas
            self.validation_stats['total_validations'] += 1
            if validation_result['passed']:
                self.validation_stats['passed_validations'] += 1
            else:
                self.validation_stats['failed_validations'] += 1
            
            self.validation_stats['warnings_generated'] += len(validation_result['warnings'])
            
            logger.info(f"Validação concluída: Score {validation_result['quality_score']:.2f}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Erro na validação de qualidade: {e}")
            return {
                'passed': False,
                'total_records': len(data) if data is not None else 0,
                'errors': [f"Erro crítico na validação: {str(e)}"],
                'warnings': [],
                'quality_score': 0.0
            }
    
    def _validate_completeness(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida completude dos dados (campos obrigatórios).
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação de completude
        """
        try:
            completeness_result = {'completeness': {}}
            
            required_fields = self.validation_rules['required_fields']
            
            for field in required_fields:
                if field in data.columns:
                    null_count = data[field].isnull().sum()
                    null_percentage = (null_count / len(data)) * 100
                    
                    completeness_result['completeness'][field] = {
                        'null_count': int(null_count),
                        'null_percentage': round(null_percentage, 2),
                        'complete': null_count == 0
                    }
                    
                    # Adicionar erro se campo obrigatório tem muitos nulos
                    if null_percentage > 0:
                        if 'errors' not in completeness_result:
                            completeness_result['errors'] = []
                        completeness_result['errors'].append(
                            f"Campo obrigatório '{field}' tem {null_percentage:.1f}% valores nulos"
                        )
                else:
                    if 'errors' not in completeness_result:
                        completeness_result['errors'] = []
                    completeness_result['errors'].append(
                        f"Campo obrigatório '{field}' não encontrado nos dados"
                    )
            
            # Calcular completude geral para todos os campos
            overall_completeness = {}
            for col in data.columns:
                null_count = data[col].isnull().sum()
                null_percentage = (null_count / len(data)) * 100
                overall_completeness[col] = {
                    'null_count': int(null_count),
                    'null_percentage': round(null_percentage, 2)
                }
                
                # Avisos para campos com muitos nulos
                if null_percentage > 50:
                    if 'warnings' not in completeness_result:
                        completeness_result['warnings'] = []
                    completeness_result['warnings'].append(
                        f"Campo '{col}' tem alta taxa de nulos: {null_percentage:.1f}%"
                    )
            
            completeness_result['statistics']['field_completeness'] = overall_completeness
            
            return completeness_result
            
        except Exception as e:
            logger.error(f"Erro na validação de completude: {e}")
            return {'errors': [f"Erro na validação de completude: {str(e)}"]}
    
    def _validate_data_types(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida tipos de dados apropriados.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação de tipos
        """
        try:
            type_result = {'data_types': {}}
            
            # Verificar campos de data
            date_fields = self.validation_rules['date_fields']
            for field in date_fields:
                if field in data.columns:
                    # Verificar se é datetime ou pode ser convertido
                    if not pd.api.types.is_datetime64_any_dtype(data[field]):
                        # Tentar converter uma amostra
                        sample = data[field].dropna().head(100)
                        try:
                            pd.to_datetime(sample, format='%d/%m/%Y', errors='raise')
                            type_result['data_types'][field] = 'convertible_to_date'
                        except:
                            if 'warnings' not in type_result:
                                type_result['warnings'] = []
                            type_result['warnings'].append(
                                f"Campo de data '{field}' não está em formato válido"
                            )
                            type_result['data_types'][field] = 'invalid_date_format'
                    else:
                        type_result['data_types'][field] = 'valid_datetime'
            
            # Verificar campos numéricos
            numeric_fields = list(self.validation_rules['numeric_ranges'].keys())
            for field in numeric_fields:
                if field in data.columns:
                    if not pd.api.types.is_numeric_dtype(data[field]):
                        # Tentar converter
                        try:
                            pd.to_numeric(data[field], errors='raise')
                            type_result['data_types'][field] = 'convertible_to_numeric'
                        except:
                            if 'warnings' not in type_result:
                                type_result['warnings'] = []
                            type_result['warnings'].append(
                                f"Campo numérico '{field}' contém valores não numéricos"
                            )
                            type_result['data_types'][field] = 'invalid_numeric'
                    else:
                        type_result['data_types'][field] = 'valid_numeric'
            
            return type_result
            
        except Exception as e:
            logger.error(f"Erro na validação de tipos: {e}")
            return {'errors': [f"Erro na validação de tipos: {str(e)}"]}
    
    def _validate_ranges(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida se valores numéricos estão dentro de ranges válidos.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação de ranges
        """
        try:
            range_result = {'range_validation': {}}
            
            numeric_ranges = self.validation_rules['numeric_ranges']
            
            for field, range_config in numeric_ranges.items():
                if field in data.columns:
                    # Converter para numérico se necessário
                    numeric_data = pd.to_numeric(data[field], errors='coerce')
                    
                    min_val = range_config['min']
                    max_val = range_config['max']
                    
                    # Contar valores fora do range
                    below_min = (numeric_data < min_val).sum()
                    above_max = (numeric_data > max_val).sum()
                    
                    range_result['range_validation'][field] = {
                        'min_violations': int(below_min),
                        'max_violations': int(above_max),
                        'total_violations': int(below_min + above_max),
                        'expected_range': f"{min_val}-{max_val}"
                    }
                    
                    # Adicionar avisos se há violações
                    if below_min > 0:
                        if 'warnings' not in range_result:
                            range_result['warnings'] = []
                        range_result['warnings'].append(
                            f"Campo '{field}': {below_min} valores abaixo do mínimo ({min_val})"
                        )
                    
                    if above_max > 0:
                        if 'warnings' not in range_result:
                            range_result['warnings'] = []
                        range_result['warnings'].append(
                            f"Campo '{field}': {above_max} valores acima do máximo ({max_val})"
                        )
            
            return range_result
            
        except Exception as e:
            logger.error(f"Erro na validação de ranges: {e}")
            return {'errors': [f"Erro na validação de ranges: {str(e)}"]}
    
    def _validate_categorical_values(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida se valores categóricos estão dentro dos valores esperados.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação categórica
        """
        try:
            categorical_result = {'categorical_validation': {}}
            
            categorical_values = self.validation_rules['categorical_values']
            
            for field, valid_values in categorical_values.items():
                if field in data.columns:
                    # Converter para string e limpar
                    field_data = data[field].astype(str).str.strip().str.upper()
                    valid_values_upper = [str(v).upper() for v in valid_values]
                    
                    # Contar valores inválidos
                    valid_mask = field_data.isin(valid_values_upper + ['NAN'])  # NaN é aceitável
                    invalid_count = (~valid_mask).sum()
                    
                    # Valores únicos inválidos
                    invalid_values = field_data[~valid_mask].unique()
                    invalid_values = [v for v in invalid_values if v != 'NAN']
                    
                    categorical_result['categorical_validation'][field] = {
                        'invalid_count': int(invalid_count),
                        'invalid_values': invalid_values[:10],  # Máximo 10 exemplos
                        'valid_values': valid_values
                    }
                    
                    if invalid_count > 0:
                        if 'warnings' not in categorical_result:
                            categorical_result['warnings'] = []
                        categorical_result['warnings'].append(
                            f"Campo '{field}': {invalid_count} valores inválidos encontrados"
                        )
            
            return categorical_result
            
        except Exception as e:
            logger.error(f"Erro na validação categórica: {e}")
            return {'errors': [f"Erro na validação categórica: {str(e)}"]}
    
    def _validate_temporal_consistency(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida consistência temporal entre campos de data.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação temporal
        """
        try:
            temporal_result = {'temporal_validation': {}}
            
            temporal_rules = self.validation_rules['temporal_consistency']
            
            for earlier_field, later_field in temporal_rules:
                if all(field in data.columns for field in [earlier_field, later_field]):
                    # Converter para datetime se necessário
                    earlier_dates = pd.to_datetime(data[earlier_field], errors='coerce')
                    later_dates = pd.to_datetime(data[later_field], errors='coerce')
                    
                    # Verificar inconsistências (data posterior antes da anterior)
                    valid_records = earlier_dates.notna() & later_dates.notna()
                    inconsistent = valid_records & (later_dates < earlier_dates)
                    
                    inconsistent_count = inconsistent.sum()
                    
                    temporal_result['temporal_validation'][f"{earlier_field}_vs_{later_field}"] = {
                        'inconsistent_count': int(inconsistent_count),
                        'total_comparable': int(valid_records.sum()),
                        'consistency_rate': round(
                            (1 - inconsistent_count / valid_records.sum()) * 100 
                            if valid_records.sum() > 0 else 100, 2
                        )
                    }
                    
                    if inconsistent_count > 0:
                        if 'errors' not in temporal_result:
                            temporal_result['errors'] = []
                        temporal_result['errors'].append(
                            f"Inconsistência temporal: {inconsistent_count} registros onde "
                            f"'{later_field}' é anterior a '{earlier_field}'"
                        )
            
            return temporal_result
            
        except Exception as e:
            logger.error(f"Erro na validação temporal: {e}")
            return {'errors': [f"Erro na validação temporal: {str(e)}"]}
    
    def _validate_business_rules(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida regras específicas de negócio para dados SRAG.
        
        Args:
            data: DataFrame a validar
            
        Returns:
            Dict com resultado da validação de regras de negócio
        """
        try:
            business_result = {'business_rules_validation': {}}
            
            business_rules = self.validation_rules['business_rules']
            
            for rule in business_rules:
                try:
                    rule_name = rule['name']
                    rule_description = rule['description']
                    rule_condition = rule['condition']
                    
                    # Aplicar condição
                    violations = rule_condition(data)
                    
                    if hasattr(violations, 'sum'):  # É uma Series booleana
                        violation_count = violations.sum()
                    else:
                        violation_count = len(violations) if violations is not None else 0
                    
                    business_result['business_rules_validation'][rule_name] = {
                        'description': rule_description,
                        'violations': int(violation_count),
                        'passed': violation_count == 0
                    }
                    
                    if violation_count > 0:
                        if 'warnings' not in business_result:
                            business_result['warnings'] = []
                        business_result['warnings'].append(
                            f"Regra de negócio violada: {rule_description} "
                            f"({violation_count} casos)"
                        )
                
                except Exception as rule_error:
                    logger.warning(f"Erro ao aplicar regra {rule.get('name', 'unknown')}: {rule_error}")
            
            return business_result
            
        except Exception as e:
            logger.error(f"Erro na validação de regras de negócio: {e}")
            return {'errors': [f"Erro na validação de regras de negócio: {str(e)}"]}
    
    def _detect_outliers(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detecta outliers em campos numéricos.
        
        Args:
            data: DataFrame a analisar
            
        Returns:
            Dict com resultado da detecção de outliers
        """
        try:
            outlier_result = {'outlier_detection': {}}
            
            outlier_config = self.validation_rules['outlier_detection']
            
            for field, config in outlier_config.items():
                if field in data.columns:
                    numeric_data = pd.to_numeric(data[field], errors='coerce').dropna()
                    
                    if len(numeric_data) == 0:
                        continue
                    
                    method = config['method']
                    outliers = []
                    
                    if method == 'iqr':
                        # Método do Intervalo Interquartil
                        Q1 = numeric_data.quantile(0.25)
                        Q3 = numeric_data.quantile(0.75)
                        IQR = Q3 - Q1
                        factor = config.get('factor', 1.5)
                        
                        lower_bound = Q1 - factor * IQR
                        upper_bound = Q3 + factor * IQR
                        
                        outliers = numeric_data[
                            (numeric_data < lower_bound) | (numeric_data > upper_bound)
                        ]
                    
                    elif method == 'percentile':
                        # Método dos Percentis
                        lower_perc = config.get('lower', 0.01)
                        upper_perc = config.get('upper', 0.99)
                        
                        lower_bound = numeric_data.quantile(lower_perc)
                        upper_bound = numeric_data.quantile(upper_perc)
                        
                        outliers = numeric_data[
                            (numeric_data < lower_bound) | (numeric_data > upper_bound)
                        ]
                    
                    outlier_count = len(outliers)
                    outlier_percentage = (outlier_count / len(numeric_data)) * 100
                    
                    outlier_result['outlier_detection'][field] = {
                        'outlier_count': outlier_count,
                        'outlier_percentage': round(outlier_percentage, 2),
                        'method_used': method,
                        'sample_outliers': outliers.head(5).tolist() if len(outliers) > 0 else []
                    }
                    
                    # Avisar se muitos outliers
                    if outlier_percentage > 5:  # Mais de 5% são outliers
                        if 'warnings' not in outlier_result:
                            outlier_result['warnings'] = []
                        outlier_result['warnings'].append(
                            f"Campo '{field}' tem alta taxa de outliers: {outlier_percentage:.1f}%"
                        )
            
            return outlier_result
            
        except Exception as e:
            logger.error(f"Erro na detecção de outliers: {e}")
            return {'errors': [f"Erro na detecção de outliers: {str(e)}"]}
    
    def _validate_duplicates(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detecta registros duplicados.
        
        Args:
            data: DataFrame a analisar
            
        Returns:
            Dict com resultado da análise de duplicatas
        """
        try:
            duplicate_result = {'duplicate_analysis': {}}
            
            # Duplicatas completas
            complete_duplicates = data.duplicated().sum()
            
            # Duplicatas baseadas em campos-chave
            key_fields = ['DT_NOTIFIC', 'SG_UF_NOT', 'NU_IDADE_N', 'CS_SEXO']
            available_keys = [field for field in key_fields if field in data.columns]
            
            if available_keys:
                key_duplicates = data.duplicated(subset=available_keys).sum()
            else:
                key_duplicates = 0
            
            duplicate_result['duplicate_analysis'] = {
                'complete_duplicates': int(complete_duplicates),
                'key_field_duplicates': int(key_duplicates),
                'key_fields_used': available_keys,
                'unique_records': len(data) - int(key_duplicates)
            }
            
            # Avisos se há muitas duplicatas
            if complete_duplicates > 0:
                if 'warnings' not in duplicate_result:
                    duplicate_result['warnings'] = []
                duplicate_result['warnings'].append(
                    f"Encontradas {complete_duplicates} duplicatas completas"
                )
            
            if key_duplicates > complete_duplicates:
                if 'warnings' not in duplicate_result:
                    duplicate_result['warnings'] = []
                duplicate_result['warnings'].append(
                    f"Encontradas {key_duplicates} duplicatas baseadas em campos-chave"
                )
            
            return duplicate_result
            
        except Exception as e:
            logger.error(f"Erro na análise de duplicatas: {e}")
            return {'errors': [f"Erro na análise de duplicatas: {str(e)}"]}
    
    def _calculate_quality_score(self, validation_result: Dict[str, Any]) -> float:
        """
        Calcula score de qualidade geral dos dados.
        
        Args:
            validation_result: Resultado das validações
            
        Returns:
            Score de qualidade de 0.0 a 100.0
        """
        try:
            score = 100.0
            
            # Penalizar por erros críticos
            error_count = len(validation_result.get('errors', []))
            score -= error_count * 10  # 10 pontos por erro
            
            # Penalizar levemente por avisos
            warning_count = len(validation_result.get('warnings', []))
            score -= warning_count * 2  # 2 pontos por aviso
            
            # Bonificar por completude
            completeness = validation_result.get('completeness', {})
            if completeness:
                avg_completeness = np.mean([
                    100 - field_info.get('null_percentage', 100)
                    for field_info in completeness.values()
                    if isinstance(field_info, dict)
                ])
                score = score * (avg_completeness / 100)
            
            # Penalizar por duplicatas
            duplicates = validation_result.get('duplicate_analysis', {})
            if duplicates:
                duplicate_rate = duplicates.get('key_field_duplicates', 0) / validation_result.get('total_records', 1)
                score -= duplicate_rate * 20  # Até 20 pontos por duplicatas
            
            # Garantir que score está entre 0 e 100
            score = max(0.0, min(100.0, score))
            
            return round(score, 2)
            
        except Exception as e:
            logger.error(f"Erro no cálculo do score de qualidade: {e}")
            return 0.0
    
    def _check_death_without_symptoms(self, data: pd.DataFrame) -> pd.Series:
        """
        Verifica casos de óbito sem sintomas registrados.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            Series booleana indicando violações
        """
        try:
            # Identificar óbitos
            deaths = data.get('EVOLUCAO', pd.Series()) == '2'
            
            # Campos de sintomas
            symptom_fields = ['FEBRE', 'TOSSE', 'DISPNEIA', 'DESC_RESP', 'DIARREIA', 'VOMITO']
            available_symptoms = [f for f in symptom_fields if f in data.columns]
            
            if not available_symptoms:
                return pd.Series([False] * len(data), index=data.index)
            
            # Verificar se tem pelo menos um sintoma
            has_symptoms = data[available_symptoms].eq('1').any(axis=1)
            
            # Retornar casos de óbito sem sintomas
            return deaths & ~has_symptoms
            
        except Exception as e:
            logger.warning(f"Erro na verificação de óbitos sem sintomas: {e}")
            return pd.Series([False] * len(data), index=data.index)
    
    def _check_vaccination_consistency(self, data: pd.DataFrame) -> pd.Series:
        """
        Verifica consistência nas informações de vacinação.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            Series booleana indicando violações
        """
        try:
            violations = pd.Series([False] * len(data), index=data.index)
            
            # Verificar se tem 2ª dose sem 1ª dose
            if all(col in data.columns for col in ['DOSE_1_COV', 'DOSE_2_COV']):
                has_dose2_not_dose1 = (
                    (data['DOSE_1_COV'] != '1') & (data['DOSE_2_COV'] == '1')
                )
                violations = violations | has_dose2_not_dose1
            
            # Verificar se tem dose de reforço sem esquema completo
            if all(col in data.columns for col in ['DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF']):
                has_booster_incomplete = (
                    (data['DOSE_REF'] == '1') & 
                    ((data['DOSE_1_COV'] != '1') | (data['DOSE_2_COV'] != '1'))
                )
                violations = violations | has_booster_incomplete
            
            return violations
            
        except Exception as e:
            logger.warning(f"Erro na verificação de consistência vacinal: {e}")
            return pd.Series([False] * len(data), index=data.index)
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Retorna resumo das validações executadas.
        
        Returns:
            Dict com estatísticas de validação
        """
        return {
            'validation_stats': self.validation_stats.copy(),
            'rules_configured': len(self.validation_rules),
            'validator_version': '1.0.0',
            'timestamp': datetime.now().isoformat()
        }
    
    def export_validation_report(
        self, 
        validation_result: Dict[str, Any], 
        file_path: Optional[str] = None
    ) -> str:
        """
        Exporta relatório de validação detalhado.
        
        Args:
            validation_result: Resultado da validação
            file_path: Caminho para salvar (auto-gerado se None)
            
        Returns:
            Caminho do arquivo salvo
        """
        try:
            if file_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_path = f"{self.settings.system.reports_dir}/validation_report_{timestamp}.json"
            
            # Preparar relatório detalhado
            detailed_report = {
                'metadata': {
                    'generation_timestamp': datetime.now().isoformat(),
                    'validator_version': '1.0.0',
                    'total_records_validated': validation_result.get('total_records', 0)
                },
                'summary': {
                    'passed': validation_result.get('passed', False),
                    'quality_score': validation_result.get('quality_score', 0.0),
                    'error_count': len(validation_result.get('errors', [])),
                    'warning_count': len(validation_result.get('warnings', []))
                },
                'detailed_results': validation_result,
                'validation_rules_used': self.validation_rules,
                'statistics': self.get_validation_summary()
            }
            
            # Salvar como JSON
            import json
            from pathlib import Path
            
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(detailed_report, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Relatório de validação salvo: {file_path}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Erro ao exportar relatório de validação: {e}")
            raise