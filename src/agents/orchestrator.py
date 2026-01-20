import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd
import json

from ..tools.database_tool import DatabaseTool
from ..tools.news_tool import NewsSearchTool
from ..tools.metrics_tool import MetricsCalculatorTool
from ..tools.chart_tool import ChartGeneratorTool
from ..tools.report_tool import ReportGeneratorTool
from ..utils.logger import get_logger
from ..utils.guardrails import SRAGGuardrails
from .base_agent import BaseAgent

logger = get_logger(__name__)

class SRAGOrchestrator(BaseAgent):
    """
    Agente principal que coordena todo o processo de geração de relatórios SRAG.
    
    Responsabilidades:
    - Coordenar execução de todas as tools
    - Aplicar governança e transparência
    - Garantir qualidade dos dados
    - Gerar relatório final consolidado
    """
    
    def __init__(self):
        """Inicializa o orquestrador com todas as ferramentas necessárias."""
        super().__init__("SRAGOrchestrator")
        
        # Inicializar ferramentas
        self.database_tool = DatabaseTool()
        self.news_tool = NewsSearchTool()
        self.metrics_tool = MetricsCalculatorTool()
        self.chart_tool = ChartGeneratorTool()
        self.report_tool = ReportGeneratorTool()
        
        # Sistema de governança
        self.guardrails = SRAGGuardrails()
        
        # Controle de estado
        self.execution_state = {
            'current_step': None,
            'completed_steps': [],
            'errors': [],
            'start_time': None,
            'metrics': {}
        }
        
        logger.info("Orquestrador SRAG inicializado com sucesso")
    
    async def generate_report(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gera relatório completo de SRAG coordenando todas as ferramentas.
        
        Args:
            request_data: Dados da solicitação validados pelos guardrails
            
        Returns:
            Dict com relatório completo e metadados
            
        Raises:
            Exception: Para erros durante a execução
        """
        try:
            # Inicializar execução
            self._initialize_execution(request_data)
            
            # Etapa 1: Carregar e processar dados
            srag_data = await self._load_and_process_data(request_data['report_date'])
            
            # Etapa 2: Calcular métricas
            metrics = await self._calculate_metrics(srag_data, request_data['report_date'])
            
            # Etapa 3: Gerar gráficos (se solicitado)
            charts = {}
            if request_data.get('include_charts', True):
                charts = await self._generate_charts(srag_data)
            
            # Etapa 4: Buscar e analisar notícias (se solicitado)
            news_analysis = {}
            if request_data.get('include_news', True):
                news_analysis = await self._analyze_news(metrics)
            
            # Etapa 5: Gerar relatório final
            final_report = await self._generate_final_report(
                srag_data, metrics, charts, news_analysis, request_data
            )
            
            # Finalizar execução
            self._finalize_execution(final_report)
            
            return final_report
            
        except Exception as e:
            self._handle_execution_error(e)
            raise
    
    def _initialize_execution(self, request_data: Dict[str, Any]) -> None:
        """
        Inicializa o estado de execução e logging.
        
        Args:
            request_data: Dados da solicitação
        """
        self.execution_state = {
            'current_step': 'initialization',
            'completed_steps': [],
            'errors': [],
            'start_time': datetime.now(),
            'request_data': request_data,
            'metrics': {}
        }
        
        logger.info("Execução iniciada", extra={
            'report_date': request_data['report_date'],
            'include_charts': request_data.get('include_charts', True),
            'include_news': request_data.get('include_news', True)
        })
        
        # Log de auditoria
        self.log_decision(
            "execution_start",
            "Iniciando geração de relatório SRAG",
            f"Data: {request_data['report_date']}, "
            f"Gráficos: {request_data.get('include_charts')}, "
            f"Notícias: {request_data.get('include_news')}"
        )

    async def _load_and_process_data(self, report_date: str) -> pd.DataFrame:
        """
        Carrega e processa dados SRAG do banco de dados.
        
        CRITICAL FIX: Never use future dates (2099-12-31).
        Always use current date as maximum.
        """
        try:
            self._update_step("data_loading")
            logger.info("Iniciando carregamento de dados SRAG")
            
            # Define safe date range
            end_date = datetime.strptime(report_date, "%Y-%m-%d")
            today = datetime.now()
            
            # CRITICAL FIX: Never request future dates
            if end_date > today:
                logger.warning(f"Data do relatório ({report_date}) é futura, ajustando para hoje")
                end_date = today
            
            # Start from 12 months before end date
            start_date = end_date - timedelta(days=365)
            
            logger.info(f"Período de análise: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
            
            # Load data using database tool
            raw_data = await self.database_tool.load_srag_data(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )
            
            # If no recent data found, try loading ALL available historical data
            if raw_data is None or len(raw_data) == 0:
                logger.warning("Nenhum dado encontrado no período recente.")
                logger.info("Tentando carregar todos os dados históricos disponíveis...")
                
                # CRITICAL FIX: Use today's date, NOT 2099-12-31
                raw_data = await self.database_tool.load_srag_data(
                    start_date="2000-01-01",
                    end_date=today.strftime("%Y-%m-%d")  # ← FIXED: Use actual current date
                )
                
                if raw_data is not None and len(raw_data) > 0:
                    logger.info(f"Análise histórica: {len(raw_data)} registros totais disponíveis")
                    self.execution_state['analysis_mode'] = 'historical'
                    
                    # Update date range metadata to reflect actual data
                    actual_dates = raw_data['DT_NOTIFIC'].dropna()
                    if len(actual_dates) > 0:
                        actual_start = actual_dates.min()
                        actual_end = actual_dates.max()
                        logger.info(f"Dados históricos: {actual_start.strftime('%Y-%m-%d')} a {actual_end.strftime('%Y-%m-%d')}")
                else:
                    logger.warning("Nenhum dado disponível em todo o período")
                    return pd.DataFrame()
            else:
                self.execution_state['analysis_mode'] = 'recent'
            
            # Apply guardrails validation
            validated_data = self.guardrails.validate_health_data(raw_data)
            
            # Process data
            processed_data = await self.database_tool.process_data(validated_data)
            
            # Record statistics
            self.execution_state['metrics']['total_records'] = len(processed_data)
            self.execution_state['metrics']['date_range'] = {
                'start': start_date.strftime("%Y-%m-%d"),
                'end': end_date.strftime("%Y-%m-%d"),
                'actual_mode': self.execution_state.get('analysis_mode', 'recent')
            }
            
            self._complete_step("data_loading")
            logger.info(f"Dados carregados: {len(processed_data)} registros")
            logger.info(f"Modo de análise: {self.execution_state.get('analysis_mode', 'recent')}")
            
            return processed_data
            
        except Exception as e:
            self._log_step_error("data_loading", e)
            raise
    
    async def _calculate_metrics(self, data: pd.DataFrame, report_date: str) -> Dict[str, Any]:
        """
        Calcula todas as métricas necessárias para o relatório.
        
        Args:
            data: DataFrame com dados SRAG processados
            report_date: Data de referência
            
        Returns:
            Dict com métricas calculadas
        """
        try:
            self._update_step("metrics_calculation")
            logger.info("Iniciando cálculo de métricas")
            
            # Calcular cada métrica usando a ferramenta específica
            metrics = {}
            
            # Taxa de aumento de casos
            metrics['case_increase_rate'] = await self.metrics_tool.calculate_case_increase_rate(
                data, report_date
            )
            
            # Taxa de mortalidade
            metrics['mortality_rate'] = await self.metrics_tool.calculate_mortality_rate(
                data, report_date
            )
            
            # Taxa de ocupação de UTI
            metrics['icu_occupancy_rate'] = await self.metrics_tool.calculate_icu_occupancy_rate(
                data, report_date
            )
            
            # Taxa de vacinação
            metrics['vaccination_rate'] = await self.metrics_tool.calculate_vaccination_rate(
                data, report_date
            )
            
            # Adicionar metadados das métricas
            metrics['calculation_timestamp'] = datetime.now().isoformat()
            metrics['data_period'] = self.execution_state['metrics']['date_range']
            metrics['total_records_analyzed'] = len(data)
            
            # Validar métricas com guardrails
            validated_metrics = self.guardrails.validate_metrics(metrics)
            
            self._complete_step("metrics_calculation")
            logger.info("Métricas calculadas com sucesso", extra={
                'case_increase_rate': validated_metrics.get('case_increase_rate'),
                'mortality_rate': validated_metrics.get('mortality_rate'),
                'icu_occupancy_rate': validated_metrics.get('icu_occupancy_rate'),
                'vaccination_rate': validated_metrics.get('vaccination_rate')
            })
            
            return validated_metrics
            
        except Exception as e:
            self._log_step_error("metrics_calculation", e)
            raise
    
    async def _generate_charts(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera gráficos necessários para o relatório.
        
        Args:
            data: DataFrame com dados SRAG processados
            
        Returns:
            Dict com informações dos gráficos gerados
        """
        try:
            self._update_step("chart_generation")
            logger.info("Iniciando geração de gráficos")
            
            charts = {}
            
            # Gráfico de casos diários (últimos 30 dias)
            daily_chart = await self.chart_tool.generate_daily_cases_chart(data)
            charts['daily_cases'] = daily_chart
            
            # Gráfico de casos mensais (últimos 12 meses)
            monthly_chart = await self.chart_tool.generate_monthly_cases_chart(data)
            charts['monthly_cases'] = monthly_chart
            
            # Adicionar metadados
            charts['generation_timestamp'] = datetime.now().isoformat()
            charts['total_charts'] = len([c for c in charts.values() if isinstance(c, dict)])
            
            self._complete_step("chart_generation")
            logger.info(f"Gráficos gerados: {charts['total_charts']}")
            
            return charts
            
        except Exception as e:
            self._log_step_error("chart_generation", e)
            raise
    
    async def _analyze_news(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Busca e analisa notícias relacionadas a SRAG para contextualizar as métricas.
        Usa Google Gemini para análise aprimorada.
        
        Args:
            metrics: Métricas calculadas para contextualizar a busca
            
        Returns:
            Dict com análise de notícias
        """
        try:
            self._update_step("news_analysis")
            logger.info("Iniciando análise de notícias com Gemini")
            
            # Buscar notícias relevantes
            news_articles = await self.news_tool.search_srag_news(
                max_articles=15,
                date_range_days=90
            )
            
            # Analisar com Gemini (com fallback para análise tradicional)
            news_analysis = await self.news_tool.analyze_news_with_gemini(
                news_articles, metrics
            )
            
            # Aplicar guardrails nas notícias
            #filtered_analysis = self.guardrails.filter_news_content(news_analysis)
            
            self._complete_step("news_analysis")
            logger.info(f"Notícias analisadas: {len(news_articles)} com Gemini")
            
            return news_analysis
            
        except Exception as e:
            self._log_step_error("news_analysis", e)
            # Notícias são opcionais, continuar sem elas em caso de erro
            logger.warning("Continuando sem análise de notícias devido ao erro")
            return {
                'error': str(e),
                'articles': [],
                'analysis': "Análise de notícias indisponível devido a erro técnico"
            }
    
    async def _generate_final_report(
        self,
        data: pd.DataFrame,
        metrics: Dict[str, Any],
        charts: Dict[str, Any],
        news_analysis: Dict[str, Any],
        request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Gera o relatório final consolidando todos os componentes.
        
        Args:
            data: Dados SRAG processados
            metrics: Métricas calculadas
            charts: Gráficos gerados
            news_analysis: Análise de notícias
            request_data: Dados da solicitação original
            
        Returns:
            Dict com relatório final completo
        """
        try:
            self._update_step("report_generation")
            logger.info("Iniciando geração do relatório final")
            
            # Preparar dados para o relatório
            report_data = {
                'metadata': {
                    'report_date': request_data['report_date'],
                    'generation_timestamp': datetime.now().isoformat(),
                    'data_period': self.execution_state['metrics']['date_range'],
                    'total_records': len(data),
                    'execution_time_seconds': (
                        datetime.now() - self.execution_state['start_time']
                    ).total_seconds()
                },
                'metrics': metrics,
                'charts': charts,
                'news_analysis': news_analysis,
                'data_summary': self._create_data_summary(data)
            }
            
            # Gerar relatório usando a ferramenta específica
            final_report = await self.report_tool.generate_comprehensive_report(
                report_data
            )
            
            # Aplicar validação final
            validated_report = self.guardrails.validate_final_report(final_report)
            
            self._complete_step("report_generation")
            logger.info("Relatório final gerado com sucesso")
            
            return validated_report
            
        except Exception as e:
            self._log_step_error("report_generation", e)
            raise
    
    def _create_data_summary(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Cria resumo estatístico dos dados para o relatório.
        
        Args:
            data: DataFrame com dados processados
            
        Returns:
            Dict com resumo dos dados
        """
        try:
            summary = {
                'total_records': len(data),
                'date_range': {
                    'start': data['DT_NOTIFIC'].min() if 'DT_NOTIFIC' in data.columns else None,
                    'end': data['DT_NOTIFIC'].max() if 'DT_NOTIFIC' in data.columns else None
                },
                'geographic_distribution': {},
                'age_distribution': {},
                'severity_indicators': {}
            }
            
            # Distribuição geográfica (se disponível)
            if 'SG_UF' in data.columns:
                summary['geographic_distribution'] = (
                    data['SG_UF'].value_counts().head(10).to_dict()
                )
            
            # Distribuição por faixa etária (se disponível)
            if 'NU_IDADE_N' in data.columns:
                age_data = data['NU_IDADE_N'].dropna()
                if len(age_data) > 0:
                    summary['age_distribution'] = {
                        'mean_age': float(age_data.mean()),
                        'median_age': float(age_data.median()),
                        'age_ranges': {
                            '0-17': len(age_data[age_data < 18]),
                            '18-59': len(age_data[(age_data >= 18) & (age_data < 60)]),
                            '60+': len(age_data[age_data >= 60])
                        }
                    }
            
            # Indicadores de gravidade
            if 'UTI' in data.columns:
                uti_data = data['UTI'].dropna()
                summary['severity_indicators']['uti_cases'] = len(uti_data[uti_data == 1])
            
            if 'EVOLUCAO' in data.columns:
                evolucao_data = data['EVOLUCAO'].value_counts().to_dict()
                summary['severity_indicators']['evolution'] = evolucao_data
            
            return summary
            
        except Exception as e:
            logger.warning(f"Erro ao criar resumo de dados: {e}")
            return {'error': 'Resumo indisponível'}
    
    def _update_step(self, step_name: str) -> None:
        """
        Atualiza o passo atual da execução.
        
        Args:
            step_name: Nome do passo atual
        """
        self.execution_state['current_step'] = step_name
        logger.debug(f"Iniciando passo: {step_name}")
        
        # Log de auditoria
        self.log_decision(
            f"step_start_{step_name}",
            f"Iniciando execução do passo: {step_name}",
            f"Passos completados: {self.execution_state['completed_steps']}"
        )
    
    def _complete_step(self, step_name: str) -> None:
        """
        Marca um passo como completo.
        
        Args:
            step_name: Nome do passo completado
        """
        self.execution_state['completed_steps'].append(step_name)
        logger.debug(f"Passo completado: {step_name}")
        
        # Log de auditoria
        self.log_decision(
            f"step_complete_{step_name}",
            f"Passo completado com sucesso: {step_name}",
            f"Total de passos completados: {len(self.execution_state['completed_steps'])}"
        )
    
    def _log_step_error(self, step_name: str, error: Exception) -> None:
        """
        Registra erro em um passo da execução.
        
        Args:
            step_name: Nome do passo com erro
            error: Exceção ocorrida
        """
        error_info = {
            'step': step_name,
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
        
        self.execution_state['errors'].append(error_info)
        logger.error(f"Erro no passo {step_name}: {error}")
        
        # Log de auditoria
        self.log_decision(
            f"step_error_{step_name}",
            f"Erro durante execução do passo: {step_name}",
            f"Detalhes do erro: {str(error)}"
        )
    
    def _finalize_execution(self, final_report: Dict[str, Any]) -> None:
        """
        Finaliza a execução e registra estatísticas.
        
        Args:
            final_report: Relatório final gerado
        """
        execution_time = datetime.now() - self.execution_state['start_time']
        
        logger.info("Execução finalizada", extra={
            'execution_time_seconds': execution_time.total_seconds(),
            'completed_steps': len(self.execution_state['completed_steps']),
            'errors_count': len(self.execution_state['errors']),
            'report_size_kb': len(json.dumps(final_report)) / 1024
        })
        
        # Log de auditoria final
        self.log_decision(
            "execution_complete",
            "Execução completada com sucesso",
            f"Tempo total: {execution_time.total_seconds()}s, "
            f"Passos: {len(self.execution_state['completed_steps'])}, "
            f"Erros: {len(self.execution_state['errors'])}"
        )
    
    def _handle_execution_error(self, error: Exception) -> None:
        """
        Trata erros críticos durante a execução.
        
        Args:
            error: Exceção crítica
        """
        execution_time = datetime.now() - self.execution_state['start_time']
        
        logger.error("Execução falhou", extra={
            'execution_time_seconds': execution_time.total_seconds(),
            'current_step': self.execution_state['current_step'],
            'completed_steps': self.execution_state['completed_steps'],
            'error': str(error)
        })
        
        # Log de auditoria de erro
        self.log_decision(
            "execution_failed",
            "Execução falhou devido a erro crítico",
            f"Passo atual: {self.execution_state['current_step']}, "
            f"Erro: {str(error)}"
        )
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica a saúde do orquestrador e suas ferramentas.
        
        Returns:
            Dict com status de saúde
        """
        try:
            status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'tools': {}
            }
            
            # Verificar cada ferramenta
            tools = [
                ('database_tool', self.database_tool),
                ('news_tool', self.news_tool),
                ('metrics_tool', self.metrics_tool),
                ('chart_tool', self.chart_tool),
                ('report_tool', self.report_tool)
            ]
            
            all_healthy = True
            for tool_name, tool_instance in tools:
                try:
                    tool_status = tool_instance.health_check()
                    status['tools'][tool_name] = tool_status
                    
                    if tool_status.get('status') != 'healthy':
                        all_healthy = False
                        
                except Exception as e:
                    status['tools'][tool_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    all_healthy = False
            
            # Status geral
            if not all_healthy:
                status['status'] = 'degraded'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }