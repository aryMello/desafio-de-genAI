import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import warnings

from ..utils.logger import get_logger
from .base_tool import BaseTool

warnings.filterwarnings('ignore', category=RuntimeWarning)

logger = get_logger(__name__)

class MetricsCalculatorTool(BaseTool):
    """
    Ferramenta para cálculo de métricas epidemiológicas de SRAG.
    
    Métricas calculadas:
    1. Taxa de aumento de casos
    2. Taxa de mortalidade
    3. Taxa de ocupação de UTI
    4. Taxa de vacinação COVID (FIXED: usa datas de dose)
    """
    
    def __init__(self):
        """Inicializa a ferramenta de cálculo de métricas."""
        super().__init__("MetricsCalculatorTool")
        
        # Configurações para cálculo
        self.comparison_periods = {
            'weekly': 7,
            'monthly': 30,
            'quarterly': 90
        }
        
        logger.info("MetricsCalculatorTool inicializada")
    
    def _detect_and_adjust_reference_date(self, data: pd.DataFrame, reference_date: str) -> str:
        """
        Para dados recentes (últimos 3 anos), usa a data de referência fornecida.
        Apenas para dados muito antigos (>3 anos), ajusta para a última data disponível.
        """
        try:
            if data is None or len(data) == 0 or 'DT_NOTIFIC' not in data.columns:
                return reference_date
            
            # Converter para datetime
            dates = pd.to_datetime(data['DT_NOTIFIC'], errors='coerce')
            dates = dates.dropna()
            
            if len(dates) == 0:
                return reference_date
            
            max_date = dates.max()
            now = datetime.now()
            
            # Apenas ajustar se data máxima for mais de 3 anos atrás
            if max_date < now - timedelta(days=1095):
                adjusted_date = max_date.strftime("%Y-%m-%d")
                logger.info(f"Dados muito históricos. Ajustando referência para {adjusted_date}")
                return adjusted_date
            
            # Para dados recentes, manter a referência original
            return reference_date
            
        except Exception as e:
            logger.warning(f"Erro ao detectar data: {e}")
            return reference_date
    
    async def calculate_case_increase_rate(
        self, 
        data: pd.DataFrame, 
        reference_date: str,
        period_days: int = 30
    ) -> Dict[str, Any]:
        """
        Calcula a taxa de aumento de casos de SRAG.
        """
        execution_id = self.log_execution_start("calculate_case_increase_rate", {
            'reference_date': reference_date,
            'period_days': period_days,
            'data_records': len(data)
        })
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Calculando taxa de aumento de casos - período: {period_days} dias")
            
            if len(data) == 0 or 'DT_NOTIFIC' not in data.columns:
                return {
                    'rate': 0.0,
                    'interpretation': "Sem dados disponíveis para o período analisado",
                    'current_cases': 0,
                    'previous_cases': 0,
                    'absolute_change': 0,
                    'calculation_metadata': {
                        'period_days': period_days,
                        'reference_date': reference_date,
                        'calculation_timestamp': datetime.now().isoformat(),
                        'error': 'DataFrame vazio ou coluna DT_NOTIFIC não encontrada'
                    }
                }
            
            # Ajustar data de referência para dados históricos
            reference_date = self._detect_and_adjust_reference_date(data, reference_date)
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            
            # Definir períodos
            current_end = ref_date
            current_start = current_end - timedelta(days=period_days)
            previous_end = current_start
            previous_start = previous_end - timedelta(days=period_days)
            
            # Filtrar dados para cada período
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            
            # Casos do período atual
            current_mask = (
                (data['DT_NOTIFIC'] >= current_start) & 
                (data['DT_NOTIFIC'] <= current_end)
            )
            current_cases = len(data[current_mask])
            
            # Casos do período anterior
            previous_mask = (
                (data['DT_NOTIFIC'] >= previous_start) & 
                (data['DT_NOTIFIC'] <= previous_end)
            )
            previous_cases = len(data[previous_mask])
            
            # Calcular taxa de aumento
            if previous_cases == 0:
                if current_cases == 0:
                    increase_rate = 0.0
                    interpretation = "Sem casos em ambos os períodos"
                else:
                    increase_rate = 100.0
                    interpretation = "Novos casos identificados"
            else:
                increase_rate = ((current_cases - previous_cases) / previous_cases) * 100
                
                if increase_rate > 0:
                    interpretation = f"Aumento de {abs(increase_rate):.1f}% em relação ao período anterior"
                elif increase_rate < 0:
                    interpretation = f"Diminuição de {abs(increase_rate):.1f}% em relação ao período anterior"
                else:
                    interpretation = "Número de casos estável"
            
            result = {
                'rate': round(increase_rate, 2),
                'interpretation': interpretation,
                'current_cases': current_cases,
                'previous_cases': previous_cases,
                'absolute_change': current_cases - previous_cases,
                'period_analysis': {
                    'current_period': {
                        'start': current_start.strftime("%Y-%m-%d"),
                        'end': current_end.strftime("%Y-%m-%d"),
                        'cases': current_cases
                    },
                    'previous_period': {
                        'start': previous_start.strftime("%Y-%m-%d"),
                        'end': previous_end.strftime("%Y-%m-%d"),
                        'cases': previous_cases
                    }
                },
                'calculation_metadata': {
                    'period_days': period_days,
                    'reference_date': reference_date,
                    'calculation_timestamp': datetime.now().isoformat()
                }
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, True, execution_time, f"Taxa: {increase_rate:.2f}%")
            
            logger.info(f"Taxa de aumento calculada: {increase_rate:.2f}%")
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, False, execution_time, error=str(e))
            logger.error(f"Erro no cálculo da taxa de aumento: {e}")
            raise
    
    async def calculate_mortality_rate(
        self, 
        data: pd.DataFrame, 
        reference_date: str,
        period_days: int = 90
    ) -> Dict[str, Any]:
        """Calcula a taxa de mortalidade por SRAG."""
        execution_id = self.log_execution_start("calculate_mortality_rate", {
            'reference_date': reference_date,
            'period_days': period_days,
            'data_records': len(data)
        })
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Calculando taxa de mortalidade - período: {period_days} dias")
            
            # Ajustar data de referência
            reference_date = self._detect_and_adjust_reference_date(data, reference_date)
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_mask = (
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= ref_date)
            )
            period_data = data[period_mask].copy()
            
            logger.info(f"Registros no período: {len(period_data)}")
            
            if len(period_data) == 0:
                return {
                    'rate': 0.0,
                    'interpretation': "Sem dados para o período analisado",
                    'total_cases': 0,
                    'deaths': 0,
                    'calculation_metadata': {
                        'period_days': period_days,
                        'reference_date': reference_date,
                        'calculation_timestamp': datetime.now().isoformat()
                    }
                }
            
            total_cases = len(period_data)
            
            # Use TEVE_OBITO if available (more reliable)
            if 'TEVE_OBITO' in period_data.columns:
                deaths = int(period_data['TEVE_OBITO'].sum())
                logger.info(f"Óbitos via TEVE_OBITO: {deaths}")
            elif 'EVOLUCAO' in period_data.columns:
                # EVOLUCAO: '2' = death by SRAG, '3' = death by other causes
                death_mask = period_data['EVOLUCAO'].isin(['2', '3'])
                deaths = int(death_mask.sum())
                logger.info(f"Óbitos via EVOLUCAO: {deaths}")
            else:
                logger.warning("Nenhuma coluna de evolução/óbito encontrada")
                deaths = 0
            
            # Calculate mortality rate
            mortality_rate = (deaths / total_cases) * 100 if total_cases > 0 else 0.0
            
            # Interpretation
            if mortality_rate == 0:
                interpretation = "Nenhum óbito registrado no período"
            elif mortality_rate < 5:
                interpretation = f"Taxa de mortalidade baixa: {mortality_rate:.1f}%"
            elif mortality_rate < 15:
                interpretation = f"Taxa de mortalidade moderada: {mortality_rate:.1f}%"
            else:
                interpretation = f"Taxa de mortalidade alta: {mortality_rate:.1f}%"
            
            result = {
                'rate': round(mortality_rate, 2),
                'interpretation': interpretation,
                'total_cases': int(total_cases),
                'deaths': int(deaths),
                'survival_rate': round(100 - mortality_rate, 2),
                'period_analysis': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': reference_date,
                    'days_analyzed': period_days
                },
                'calculation_metadata': {
                    'period_days': period_days,
                    'reference_date': reference_date,
                    'calculation_timestamp': datetime.now().isoformat()
                }
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, True, execution_time, 
                                 f"Taxa: {mortality_rate:.2f}% ({deaths} óbitos)")
            
            logger.info(f"Taxa de mortalidade calculada: {mortality_rate:.2f}% ({deaths}/{total_cases})")
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, False, execution_time, error=str(e))
            logger.error(f"Erro no cálculo da taxa de mortalidade: {e}", exc_info=True)
            raise
    
    async def calculate_icu_occupancy_rate(
        self, 
        data: pd.DataFrame, 
        reference_date: str,
        period_days: int = 30
    ) -> Dict[str, Any]:
        """Calcula a taxa de ocupação de UTI por casos de SRAG."""
        execution_id = self.log_execution_start("calculate_icu_occupancy_rate", {
            'reference_date': reference_date,
            'period_days': period_days,
            'data_records': len(data)
        })
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Calculando taxa de ocupação de UTI - período: {period_days} dias")
            
            # Ajustar data de referência
            reference_date = self._detect_and_adjust_reference_date(data, reference_date)
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_mask = (
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= ref_date)
            )
            period_data = data[period_mask].copy()
            
            logger.info(f"Registros no período: {len(period_data)}")
            
            if len(period_data) == 0:
                return {
                    'rate': 0.0,
                    'interpretation': "Sem dados para o período analisado",
                    'total_hospitalized': 0,
                    'icu_cases': 0,
                    'calculation_metadata': {
                        'period_days': period_days,
                        'reference_date': reference_date,
                        'calculation_timestamp': datetime.now().isoformat()
                    }
                }
            
            total_hospitalized = len(period_data)
            
            # Use TEVE_UTI if available
            if 'TEVE_UTI' in period_data.columns:
                icu_cases = int(period_data['TEVE_UTI'].sum())
                logger.info(f"UTIs via TEVE_UTI: {icu_cases}")
            elif 'UTI' in period_data.columns:
                # UTI: '1' = Yes
                icu_mask = period_data['UTI'] == '1'
                icu_cases = int(icu_mask.sum())
                logger.info(f"UTIs via UTI: {icu_cases}")
            else:
                logger.warning("Nenhuma coluna de UTI encontrada")
                icu_cases = 0
            
            # Calculate ICU rate
            icu_rate = (icu_cases / total_hospitalized) * 100 if total_hospitalized > 0 else 0.0
            
            # Interpretation
            if icu_rate == 0:
                interpretation = "Nenhuma internação em UTI registrada"
            elif icu_rate < 20:
                interpretation = f"Taxa de UTI baixa: {icu_rate:.1f}%"
            elif icu_rate < 40:
                interpretation = f"Taxa de UTI moderada: {icu_rate:.1f}%"
            else:
                interpretation = f"Taxa de UTI alta: {icu_rate:.1f}% - casos graves"
            
            result = {
                'rate': round(icu_rate, 2),
                'interpretation': interpretation,
                'total_hospitalized': int(total_hospitalized),
                'icu_cases': int(icu_cases),
                'non_icu_cases': int(total_hospitalized - icu_cases),
                'period_analysis': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': reference_date,
                    'days_analyzed': period_days
                },
                'calculation_metadata': {
                    'period_days': period_days,
                    'reference_date': reference_date,
                    'calculation_timestamp': datetime.now().isoformat()
                }
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, True, execution_time, 
                                 f"Taxa: {icu_rate:.2f}% ({icu_cases} UTIs)")
            
            logger.info(f"Taxa de ocupação de UTI calculada: {icu_rate:.2f}% ({icu_cases}/{total_hospitalized})")
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, False, execution_time, error=str(e))
            logger.error(f"Erro no cálculo da taxa de ocupação de UTI: {e}", exc_info=True)
            raise
    
    async def calculate_vaccination_rate(
        self, 
        data: pd.DataFrame, 
        reference_date: str,
        period_days: int = 90
    ) -> Dict[str, Any]:
        """
        Calculate COVID-19 vaccination rate based on DOSE DATES (CRITICAL FIX).
        
        IMPORTANT CHANGES:
        - Uses DOSE_*_COV date fields (DD/MM/YYYY format)
        - Ignores VACINA_COV code field (unreliable for COVID vaccination)
        - Non-null dose date = person received that dose
        
        Args:
            data: DataFrame with SRAG data (dates already parsed)
            reference_date: Reference date (YYYY-MM-DD)
            period_days: Number of days for analysis period
            
        Returns:
            Dict with vaccination rate and metadata
        """
        execution_id = self.log_execution_start("calculate_vaccination_rate", {
            'reference_date': reference_date,
            'period_days': period_days,
            'data_records': len(data)
        })
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Calculando taxa de vacinação COVID - período: {period_days} dias")
            logger.info("MÉTODO: Baseado em datas de dose, NÃO em código VACINA_COV")
            
            # Ajustar data de referência
            reference_date = self._detect_and_adjust_reference_date(data, reference_date)
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_mask = (
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= ref_date)
            )
            period_data = data[period_mask].copy()
            
            logger.info(f"Registros no período: {len(period_data)}")
            
            if len(period_data) == 0:
                return self._empty_vaccination_result(reference_date, period_days)
            
            total_cases = len(period_data)
            
            # COVID vaccination dose columns (these are DATE fields)
            dose_cols = ['DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF', 
                        'DOSE_2REF', 'DOSE_ADIC', 'DOS_RE_BI']
            
            available_dose_cols = [col for col in dose_cols if col in period_data.columns]
            
            if not available_dose_cols:
                logger.warning("Nenhuma coluna de dose COVID encontrada")
                return self._empty_vaccination_result(reference_date, period_days)
            
            logger.info(f"Colunas de dose disponíveis: {available_dose_cols}")
            
            # Ensure date columns are datetime (should be from database_tool parsing)
            for col in available_dose_cols:
                if not pd.api.types.is_datetime64_any_dtype(period_data[col]):
                    logger.warning(f"{col} não é datetime, tentando parsear DD/MM/YYYY")
                    period_data[col] = pd.to_datetime(
                        period_data[col],
                        format='%d/%m/%Y',
                        errors='coerce'
                    )
            
            # Count each dose type (any non-null date = dose administered)
            vaccination_breakdown = {}
            
            if 'DOSE_1_COV' in period_data.columns:
                vaccination_breakdown['dose_1'] = int(period_data['DOSE_1_COV'].notna().sum())
                logger.info(f"1ª dose: {vaccination_breakdown['dose_1']} casos")
            else:
                vaccination_breakdown['dose_1'] = 0
            
            if 'DOSE_2_COV' in period_data.columns:
                vaccination_breakdown['dose_2'] = int(period_data['DOSE_2_COV'].notna().sum())
                logger.info(f"2ª dose: {vaccination_breakdown['dose_2']} casos")
            else:
                vaccination_breakdown['dose_2'] = 0
            
            if 'DOSE_REF' in period_data.columns:
                vaccination_breakdown['dose_booster'] = int(period_data['DOSE_REF'].notna().sum())
                logger.info(f"Dose reforço: {vaccination_breakdown['dose_booster']} casos")
            else:
                vaccination_breakdown['dose_booster'] = 0
            
            if 'DOSE_2REF' in period_data.columns:
                vaccination_breakdown['dose_2nd_booster'] = int(period_data['DOSE_2REF'].notna().sum())
            else:
                vaccination_breakdown['dose_2nd_booster'] = 0
            
            if 'DOSE_ADIC' in period_data.columns:
                vaccination_breakdown['dose_additional'] = int(period_data['DOSE_ADIC'].notna().sum())
            else:
                vaccination_breakdown['dose_additional'] = 0
            
            # Person is vaccinated if they have ANY dose date
            vaccinated_mask = period_data[available_dose_cols].notna().any(axis=1)
            vaccinated_cases = int(vaccinated_mask.sum())
            
            logger.info(f"Total vacinados (qualquer dose): {vaccinated_cases}/{total_cases}")
            
            # Calculate rate
            vaccination_rate = (vaccinated_cases / total_cases * 100) if total_cases > 0 else 0.0
            
            # Interpretation
            if vaccination_rate == 0:
                interpretation = "Nenhum caso vacinado identificado no período"
            elif vaccination_rate < 30:
                interpretation = f"Taxa de vacinação baixa: {vaccination_rate:.1f}%"
            elif vaccination_rate < 70:
                interpretation = f"Taxa de vacinação moderada: {vaccination_rate:.1f}%"
            else:
                interpretation = f"Taxa de vacinação alta: {vaccination_rate:.1f}%"
            
            result = {
                'rate': round(vaccination_rate, 2),
                'interpretation': interpretation,
                'total_cases': int(total_cases),
                'vaccinated_cases': int(vaccinated_cases),
                'unvaccinated_cases': int(total_cases - vaccinated_cases),
                'vaccination_breakdown': vaccination_breakdown,
                'period_analysis': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': reference_date,
                    'days_analyzed': period_days
                },
                'calculation_metadata': {
                    'period_days': period_days,
                    'reference_date': reference_date,
                    'calculation_timestamp': datetime.now().isoformat(),
                    'method': 'dose_dates',
                    'note': 'Calculated from DOSE_*_COV date fields, not VACINA_COV code'
                }
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, True, execution_time, 
                                 f"Taxa: {vaccination_rate:.2f}% ({vaccinated_cases}/{total_cases})")
            
            logger.info(f"Taxa de vacinação COVID calculada: {vaccination_rate:.2f}%")
            logger.info(f"Breakdown: {vaccination_breakdown}")
            
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, False, execution_time, error=str(e))
            logger.error(f"Erro no cálculo da taxa de vacinação: {e}", exc_info=True)
            raise
    
    def _empty_vaccination_result(self, reference_date: str, period_days: int) -> Dict[str, Any]:
        """Return empty result for vaccination calculation."""
        return {
            'rate': 0.0,
            'interpretation': "Dados de vacinação não disponíveis para o período",
            'total_cases': 0,
            'vaccinated_cases': 0,
            'unvaccinated_cases': 0,
            'vaccination_breakdown': {
                'dose_1': 0,
                'dose_2': 0,
                'dose_booster': 0
            },
            'calculation_metadata': {
                'period_days': period_days,
                'reference_date': reference_date,
                'calculation_timestamp': datetime.now().isoformat(),
                'method': 'dose_dates',
                'note': 'No vaccination data available'
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica saúde da ferramenta de métricas."""
        try:
            # Teste básico de funcionalidade
            test_data = pd.DataFrame({
                'DT_NOTIFIC': pd.date_range('2024-01-01', periods=100, freq='D'),
                'EVOLUCAO': np.random.choice(['1', '2'], 100),
                'UTI': np.random.choice(['1', '2'], 100),
                'DOSE_1_COV': pd.to_datetime(['2021-05-18'] * 50 + [None] * 50, format='%Y-%m-%d')
            })
            
            # Test metric calculation
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            test_result = loop.run_until_complete(
                self.calculate_mortality_rate(test_data, '2024-03-01', 30)
            )
            
            return {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'test_calculation': 'successful',
                'test_result_sample': {
                    'rate': test_result.get('rate'),
                    'interpretation': test_result.get('interpretation')
                },
                'available_metrics': [
                    'case_increase_rate',
                    'mortality_rate',
                    'icu_occupancy_rate',
                    'vaccination_rate'
                ],
                'vaccination_method': 'dose_dates (FIXED)',
                'tool_stats': self.get_tool_stats()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'available_metrics': [
                    'case_increase_rate',
                    'mortality_rate',
                    'icu_occupancy_rate',
                    'vaccination_rate'
                ]
            }