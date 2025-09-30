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
    4. Taxa de vacinação da população
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
            # ADICIONAR ESTA VERIFICAÇÃO
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
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            # Definir períodos
            current_end = ref_date
            current_start = current_end - timedelta(days=period_days)
            previous_end = current_start
            previous_start = previous_end - timedelta(days=period_days)
            
            # Filtrar dados para cada período
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
            # Converter para datetime se necessário
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
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados para o período
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
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
            deaths = 0
            
            # ESTRATÉGIA 1: Usar campo derivado TEVE_OBITO (preferencial)
            if 'TEVE_OBITO' in period_data.columns:
                deaths = int(period_data['TEVE_OBITO'].sum())
                logger.info(f"Óbitos via TEVE_OBITO: {deaths}")
            
            # ESTRATÉGIA 2: Calcular diretamente do EVOLUCAO
            elif 'EVOLUCAO' in period_data.columns:
                # Tentar múltiplos formatos
                evolucao_str = period_data['EVOLUCAO'].astype(str).str.strip()
                
                # Log dos valores encontrados
                evolucao_values = evolucao_str.value_counts()
                logger.info(f"Valores EVOLUCAO no período: {evolucao_values.to_dict()}")
                
                # Códigos de óbito: '2' = Óbito por SRAG, '3' = Óbito por outras causas
                death_mask = evolucao_str.isin(['2', '3', '2.0', '3.0'])
                deaths = death_mask.sum()
                logger.info(f"Óbitos via EVOLUCAO (códigos 2/3): {deaths}")
                
                # Se ainda zero, tentar formato numérico
                if deaths == 0:
                    evolucao_num = pd.to_numeric(period_data['EVOLUCAO'], errors='coerce')
                    death_mask_num = evolucao_num.isin([2, 3])
                    deaths = death_mask_num.sum()
                    logger.info(f"Óbitos via EVOLUCAO numérico: {deaths}")
            
            # ESTRATÉGIA 3: Outros campos possíveis
            elif 'CLASSI_FIN' in period_data.columns:
                # Classificação final - códigos de óbito podem variar
                logger.info("Tentando calcular via CLASSI_FIN")
                # Adicionar lógica específica se souber os códigos
            
            else:
                logger.warning("Nenhuma coluna de evolução/óbito encontrada nos dados")
                logger.info(f"Colunas disponíveis: {list(period_data.columns)}")
            
            # Calcular taxa de mortalidade
            mortality_rate = (deaths / total_cases) * 100 if total_cases > 0 else 0.0
            
            # Interpretação
            if mortality_rate == 0:
                if deaths == 0:
                    interpretation = "Nenhum óbito registrado no período"
                else:
                    interpretation = f"Taxa de mortalidade: {mortality_rate:.1f}%"
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
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados para o período
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
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
            icu_cases = 0
            
            # ESTRATÉGIA 1: Usar campo derivado TEVE_UTI (preferencial)
            if 'TEVE_UTI' in period_data.columns:
                icu_cases = int(period_data['TEVE_UTI'].sum())
                logger.info(f"UTIs via TEVE_UTI: {icu_cases}")
            
            # ESTRATÉGIA 2: Calcular diretamente do UTI
            elif 'UTI' in period_data.columns:
                # Tentar múltiplos formatos
                uti_str = period_data['UTI'].astype(str).str.strip()
                
                # Log dos valores encontrados
                uti_values = uti_str.value_counts()
                logger.info(f"Valores UTI no período: {uti_values.to_dict()}")
                
                # Código '1' = Sim (internado em UTI)
                icu_mask_str = uti_str == '1'
                icu_cases = icu_mask_str.sum()
                logger.info(f"UTIs via UTI string (código '1'): {icu_cases}")
                
                # Se ainda zero, tentar formato numérico
                if icu_cases == 0:
                    uti_num = pd.to_numeric(period_data['UTI'], errors='coerce')
                    icu_mask_num = uti_num == 1
                    icu_cases = icu_mask_num.sum()
                    logger.info(f"UTIs via UTI numérico (código 1): {icu_cases}")
            
            # ESTRATÉGIA 3: Campo alternativo
            elif 'CASO_GRAVE' in period_data.columns:
                icu_cases = int(period_data['CASO_GRAVE'].sum())
                logger.info(f"UTIs via CASO_GRAVE: {icu_cases}")
            
            else:
                logger.warning("Nenhuma coluna de UTI encontrada nos dados")
                logger.info(f"Colunas disponíveis: {list(period_data.columns)}")
            
            # Calcular taxa de ocupação de UTI
            icu_rate = (icu_cases / total_hospitalized) * 100 if total_hospitalized > 0 else 0.0
            
            # Interpretação
            if icu_rate == 0:
                if icu_cases == 0:
                    interpretation = "Nenhuma internação em UTI registrada"
                else:
                    interpretation = f"Taxa de UTI: {icu_rate:.1f}%"
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
        Calcula a taxa de vacinação da população afetada por SRAG.
        
        Taxa = (Casos vacinados / Total de casos) * 100
        
        Args:
            data: DataFrame com dados SRAG
            reference_date: Data de referência (YYYY-MM-DD)
            period_days: Número de dias do período de análise
            
        Returns:
            Dict com taxa de vacinação e metadados
        """
        execution_id = self.log_execution_start("calculate_vaccination_rate", {
            'reference_date': reference_date,
            'period_days': period_days,
            'data_records': len(data)
        })
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Calculando taxa de vacinação - período: {period_days} dias")
            
            ref_date = datetime.strptime(reference_date, "%Y-%m-%d")
            start_date = ref_date - timedelta(days=period_days)
            
            # Filtrar dados para o período
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_mask = (
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= ref_date)
            )
            period_data = data[period_mask]
            
            if len(period_data) == 0:
                return {
                    'rate': 0.0,
                    'interpretation': "Sem dados para o período analisado",
                    'total_cases': 0,
                    'vaccinated_cases': 0,
                    'calculation_metadata': {
                        'period_days': period_days,
                        'reference_date': reference_date,
                        'calculation_timestamp': datetime.now().isoformat()
                    }
                }
            
            total_cases = len(period_data)
            
            # Contar casos vacinados
            vaccinated_cases = 0
            vaccination_breakdown = {
                'dose_1': 0,
                'dose_2': 0,
                'dose_booster': 0,
                'unknown_status': 0
            }
            
            # Verificar diferentes colunas de vacinação
            if 'STATUS_VACINAL' in period_data.columns:
                vaccinated_mask = period_data['STATUS_VACINAL'] != 'Não vacinado'
                vaccinated_cases = vaccinated_mask.sum()
                
                # Breakdown por tipo de dose
                vaccination_breakdown['dose_1'] = len(
                    period_data[period_data['STATUS_VACINAL'] == '1ª dose']
                )
                vaccination_breakdown['dose_2'] = len(
                    period_data[period_data['STATUS_VACINAL'] == '2ª dose']
                )
                vaccination_breakdown['dose_booster'] = len(
                    period_data[period_data['STATUS_VACINAL'] == 'Dose reforço']
                )
                
            elif 'VACINA_COV' in period_data.columns:
                vaccinated_mask = period_data['VACINA_COV'].astype(str) == '1'
                vaccinated_cases = vaccinated_mask.sum()
                
            elif any(col in period_data.columns for col in ['DOSE_1_COV', 'DOSE_2_COV']):
                # Contar qualquer dose como vacinado
                dose_cols = [col for col in ['DOSE_1_COV', 'DOSE_2_COV', 'DOSE_REF'] 
                            if col in period_data.columns]
                
                vaccinated_mask = period_data[dose_cols].astype(str).eq('1').any(axis=1)
                vaccinated_cases = vaccinated_mask.sum()
                
                # Breakdown detalhado
                if 'DOSE_1_COV' in period_data.columns:
                    vaccination_breakdown['dose_1'] = (period_data['DOSE_1_COV'].astype(str) == '1').sum()
                if 'DOSE_2_COV' in period_data.columns:
                    vaccination_breakdown['dose_2'] = (period_data['DOSE_2_COV'].astype(str) == '1').sum()
                if 'DOSE_REF' in period_data.columns:
                    vaccination_breakdown['dose_booster'] = (period_data['DOSE_REF'].astype(str) == '1').sum()
            else:
                logger.warning("Colunas de vacinação não encontradas")
                vaccination_breakdown['unknown_status'] = total_cases
            
            # Calcular taxa de vacinação
            vaccination_rate = (vaccinated_cases / total_cases) * 100 if total_cases > 0 else 0.0
            
            # Interpretação
            if vaccination_rate == 0:
                interpretation = "Nenhum caso vacinado identificado"
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
                'vaccination_breakdown': {k: int(v) for k, v in vaccination_breakdown.items()},
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
            self.log_execution_end(execution_id, True, execution_time, f"Taxa: {vaccination_rate:.2f}%")
            
            logger.info(f"Taxa de vacinação calculada: {vaccination_rate:.2f}%")
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.log_execution_end(execution_id, False, execution_time, error=str(e))
            logger.error(f"Erro no cálculo da taxa de vacinação: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de métricas.
        
        Returns:
            Dict com status de saúde
        """
        try:
            # Teste básico de funcionalidade com dados sintéticos
            test_data = pd.DataFrame({
                'DT_NOTIFIC': pd.date_range('2024-01-01', periods=100, freq='D'),
                'EVOLUCAO': np.random.choice(['1', '2'], 100),
                'UTI': np.random.choice(['1', '2'], 100),
                'DOSE_1_COV': np.random.choice(['1', '2'], 100)
            })
            
            # Tentar calcular uma métrica simples (de forma síncrona para health check)
            import asyncio
            try:
                # Criar event loop se não existir
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