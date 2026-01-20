import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import json
import base64
import pytz

from .base_tool import BaseTool
from ..utils.logger import get_logger
from ..utils.llm_gemini import get_gemini_client

logger = get_logger(__name__)

# Timezone do Brasil
BRAZIL_TZ = pytz.timezone('America/Sao_Paulo')

class ReportGeneratorTool(BaseTool):
    """
    Ferramenta para geração do relatório final consolidado.
    
    Funcionalidades:
    - Compilação de todos os componentes
    - Geração de HTML responsivo
    - Exportação para PDF (opcional)
    - Template profissional
    """
    
    def __init__(self):
        """Inicializa ferramenta de geração de relatórios."""
        super().__init__("ReportGeneratorTool")
        
        # Diretório de saída
        self.output_dir = Path("data/reports")
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Template HTML base
        self.html_template = self._load_html_template()
        
        # Timezone do Brasil
        self.brazil_tz = BRAZIL_TZ
        
        logger.info("ReportGeneratorTool inicializada")

    def _format_datetime_br(self, dt_str: str, include_time: bool = True) -> str:
        """
        Formata data/hora para o formato brasileiro (GMT-3).
        
        Args:
            dt_str: String de data/hora
            include_time: Se True, inclui horário; se False, apenas data
            
        Returns:
            String formatada no padrão brasileiro
        """
        try:
            # Tentar parsear diferentes formatos
            if isinstance(dt_str, pd.Timestamp):
                dt = dt_str.to_pydatetime()
            elif isinstance(dt_str, datetime):
                dt = dt_str
            else:
                # Tentar parsear string
                try:
                    dt = pd.to_datetime(dt_str)
                except:
                    # Se falhar, tentar ISO format
                    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            
            # Converter para timezone do Brasil se necessário
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            
            dt_br = dt.astimezone(self.brazil_tz)
            
            # Formatar no padrão brasileiro
            if include_time:
                return dt_br.strftime("%d/%m/%Y às %H:%M:%S")
            else:
                return dt_br.strftime("%d/%m/%Y")
                
        except Exception as e:
            logger.warning(f"Erro ao formatar data {dt_str}: {e}")
            return str(dt_str)
    
    def _get_current_datetime_br(self) -> str:
        """
        Retorna data/hora atual no timezone do Brasil.
        
        Returns:
            String formatada com data/hora atual
        """
        now = datetime.now(self.brazil_tz)
        return now.strftime("%d/%m/%Y às %H:%M:%S")
    
    def _convert_timestamps_to_str(self, obj):
        """
        Converte recursivamente objetos Timestamp para strings no formato brasileiro.
        
        Args:
            obj: Objeto a ser convertido
            
        Returns:
            Objeto com timestamps convertidos
        """
        import pandas as pd
        
        if isinstance(obj, pd.Timestamp):
            return self._format_datetime_br(obj, include_time=False)
        elif isinstance(obj, datetime):
            return self._format_datetime_br(obj, include_time=True)
        elif isinstance(obj, dict):
            return {k: self._convert_timestamps_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_timestamps_to_str(item) for item in obj]
        else:
            return obj
    
    async def generate_comprehensive_report(
        self, 
        report_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Gera relatório completo consolidando todos os componentes.
        
        Args:
            report_data: Dados completos para o relatório
            
        Returns:
            Dict com informações do relatório gerado
        """
        execution_id = self.log_execution_start("generate_comprehensive_report", {
            'sections': list(report_data.keys())
        })
        
        start_time = datetime.now()
        
        try:
            # Extrair componentes
            metadata = report_data.get('metadata', {})
            metrics = report_data.get('metrics', {})
            charts = report_data.get('charts', {})
            news_analysis = report_data.get('news_analysis', {})
            data_summary = report_data.get('data_summary', {})
            
            # Gerar HTML
            html_content = self._generate_html_report(
                metadata, metrics, charts, news_analysis, data_summary
            )
            
            # Salvar arquivo HTML
            timestamp = datetime.now(self.brazil_tz).strftime("%Y%m%d_%H%M%S")
            report_date = metadata.get('report_date', 'unknown')
            html_file = self.output_dir / f"srag_report_{report_date}_{timestamp}.html"
            
            html_file.write_text(html_content, encoding='utf-8')
            
            # Gerar resumo executivo
            executive_summary = self._generate_executive_summary(metrics, news_analysis)
            
            # Preparar resposta
            report_info = {
                'html_file_path': str(html_file),
                'report_date': report_date,
                'generation_timestamp': self._get_current_datetime_br(),
                'executive_summary': executive_summary,
                'metrics_count': len([k for k, v in metrics.items() 
                                    if isinstance(v, dict) and 'rate' in v]),
                'charts_generated': len([k for k, v in charts.items() 
                                       if isinstance(v, dict) and 'file_path' in v]),
                'news_articles_analyzed': news_analysis.get('total_articles_analyzed', 0),
                'file_size_kb': round(html_file.stat().st_size / 1024, 2)
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                True,
                execution_time,
                f"Relatório gerado: {html_file.name}"
            )
            
            # Incluir seções obrigatórias para validação
            result = {
                'metadata': metadata,
                'metrics': metrics,
                'charts': charts,
                'news_analysis': news_analysis,
                'data_summary': data_summary,
                'report_info': report_info
            }

            # Converter timestamps para strings
            result = self._convert_timestamps_to_str(result)

            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                False,
                execution_time,
                error=str(e)
            )
            
            logger.error(f"Erro na geração do relatório: {e}")
            raise
    
    def _generate_html_report(
        self,
        metadata: Dict[str, Any],
        metrics: Dict[str, Any],
        charts: Dict[str, Any],
        news_analysis: Dict[str, Any],
        data_summary: Dict[str, Any]
    ) -> str:
        """
        Gera conteúdo HTML do relatório.
        
        Args:
            metadata: Metadados do relatório
            metrics: Métricas calculadas
            charts: Informações dos gráficos
            news_analysis: Análise de notícias
            data_summary: Resumo dos dados
            
        Returns:
            String com HTML completo
        """
        try:
            # Cabeçalho do relatório
            report_header = self._generate_report_header(metadata)
            
            # Seção de métricas
            metrics_section = self._generate_metrics_section(metrics)
            
            # Seção de gráficos
            charts_section = self._generate_charts_section(charts)
            
            # Seção de análise de notícias
            news_section = self._generate_news_section(news_analysis)
            
            # Seção de resumo de dados
            data_section = self._generate_data_section(data_summary)
            
            # Rodapé
            report_footer = self._generate_report_footer(metadata)
            
            # Combinar todas as seções
            html_content = self.html_template.format(
                title=f"Relatório SRAG - {metadata.get('report_date', 'N/A')}",
                header=report_header,
                metrics_section=metrics_section,
                charts_section=charts_section,
                news_section=news_section,
                data_section=data_section,
                footer=report_footer,
                generation_timestamp=self._get_current_datetime_br()
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"Erro na geração do HTML: {e}")
            raise
    
    def _generate_report_header(self, metadata: Dict[str, Any]) -> str:
        """Gera cabeçalho do relatório."""
        report_date = metadata.get('report_date', 'N/A')
        
        # Formatar data de geração
        generation_time_raw = metadata.get('generation_timestamp', '')
        if generation_time_raw:
            generation_time = self._format_datetime_br(generation_time_raw)
        else:
            generation_time = self._get_current_datetime_br()
            
        total_records = metadata.get('total_records', 0)
        
        return f"""
        <div class="report-header">
            <div class="container">
                <div class="row">
                    <div class="col-md-8">
                        <h1 class="display-4">Relatório SRAG</h1>
                        <h2 class="text">Síndrome Respiratória Aguda Grave</h2>
                        <p class="lead">Data de Referência: <strong>{report_date}</strong></p>
                    </div>
                    <div class="col-md-4 text-right">
                        <div class="report-info">
                            <p><strong>Registros Analisados:</strong> {total_records:,}</p>
                            <p><strong>Gerado em:</strong> {generation_time}</p>
                            <p><strong>Fuso Horário:</strong> GMT-3 (Brasília)</p>
                            <div class="badge badge-primary">ABC HealthCare Inc.</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def _generate_metrics_section(self, metrics: Dict[str, Any]) -> str:
        """Gera seção de métricas com breakdown detalhado."""
        metrics_html = """
        <div class="section">
            <div class="container">
                <h3 class="section-title">Análise de Métricas Epidemiológicas</h3>
        """
        
        # Taxa de Aumento de Casos
        if 'case_increase_rate' in metrics:
            rate_data = metrics['case_increase_rate']
            if isinstance(rate_data, dict):
                rate = rate_data.get('rate', 0)
                current = rate_data.get('current_cases', 0)
                previous = rate_data.get('previous_cases', 0)
                change = rate_data.get('absolute_change', 0)
                interpretation = rate_data.get('interpretation', '')
                
                metrics_html += f"""
                <div class="row mb-4">
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header bg-primary text-white">
                                <h5>Taxa de Aumento de Casos</h5>
                            </div>
                            <div class="card-body">
                                <div class="metric-value text-primary mb-3">{rate}%</div>
                                <p class="metric-interpretation mb-3"><strong>{interpretation}</strong></p>
                                <div class="metric-details">
                                    <p><strong>Período Atual:</strong> {current:,} casos</p>
                                    <p><strong>Período Anterior:</strong> {previous:,} casos</p>
                                    <p><strong>Mudança Absoluta:</strong> {change:+,} casos</p>
                                </div>
                            </div>
                        </div>
                    </div>
        """
        
        # Taxa de Mortalidade
        if 'mortality_rate' in metrics:
            rate_data = metrics['mortality_rate']
            if isinstance(rate_data, dict):
                rate = rate_data.get('rate', 0)
                total = rate_data.get('total_cases', 0)
                deaths = rate_data.get('deaths', 0)
                survival = rate_data.get('survival_rate', 0)
                interpretation = rate_data.get('interpretation', '')
                
                metrics_html += f"""
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header bg-danger text-white">
                                <h5>Taxa de Mortalidade</h5>
                            </div>
                            <div class="card-body">
                                <div class="metric-value text-danger mb-3">{rate}%</div>
                                <p class="metric-interpretation mb-3"><strong>{interpretation}</strong></p>
                                <div class="metric-details">
                                    <p><strong>Total de Casos:</strong> {total:,}</p>
                                    <p><strong>Óbitos:</strong> {deaths:,}</p>
                                    <p><strong>Taxa de Sobrevivência:</strong> {survival:.1f}%</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
        """
        
        # Taxa de Ocupação UTI
        if 'icu_occupancy_rate' in metrics:
            rate_data = metrics['icu_occupancy_rate']
            if isinstance(rate_data, dict):
                rate = rate_data.get('rate', 0)
                total_hosp = rate_data.get('total_hospitalized', 0)
                icu_cases = rate_data.get('icu_cases', 0)
                non_icu = rate_data.get('non_icu_cases', 0)
                interpretation = rate_data.get('interpretation', '')
                
                metrics_html += f"""
                <div class="row mb-4">
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header bg-warning text-dark">
                                <h5>Taxa de Ocupação de UTI</h5>
                            </div>
                            <div class="card-body">
                                <div class="metric-value text-warning mb-3">{rate}%</div>
                                <p class="metric-interpretation mb-3"><strong>{interpretation}</strong></p>
                                <div class="metric-details">
                                    <p><strong>Total Hospitalizado:</strong> {total_hosp:,}</p>
                                    <p><strong>Casos em UTI:</strong> {icu_cases:,}</p>
                                    <p><strong>Casos sem UTI:</strong> {non_icu:,}</p>
                                </div>
                            </div>
                        </div>
                    </div>
        """
        
        # Taxa de Vacinação com breakdown
        if 'vaccination_rate' in metrics:
            rate_data = metrics['vaccination_rate']
            if isinstance(rate_data, dict):
                rate = rate_data.get('rate', 0)
                total = rate_data.get('total_cases', 0)
                vaccinated = rate_data.get('vaccinated_cases', 0)
                unvaccinated = rate_data.get('unvaccinated_cases', 0)
                interpretation = rate_data.get('interpretation', '')
                breakdown = rate_data.get('vaccination_breakdown', {})
                
                vac_pct = (vaccinated / total * 100) if total > 0 else 0
                unvac_pct = (unvaccinated / total * 100) if total > 0 else 0
                
                metrics_html += f"""
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header bg-success text-white">
                                <h5>Taxa de Vacinação COVID-19</h5>
                            </div>
                            <div class="card-body">
                                <div class="metric-value text-success mb-3">{rate}%</div>
                                <p class="metric-interpretation mb-3"><strong>{interpretation}</strong></p>
                                <div class="metric-details">
                                    <p><strong>Total de Casos:</strong> {total:,}</p>
                                    <p><strong>Vacinados:</strong> {vaccinated:,} ({vac_pct:.1f}%)</p>
                                    <p><strong>Não Vacinados:</strong> {unvaccinated:,} ({unvac_pct:.1f}%)</p>
                                    <hr>
                                    <p><small><strong>Breakdown:</strong></p>
                                    <p>1ª Dose: {breakdown.get('dose_1', 0):,} | 2ª Dose: {breakdown.get('dose_2', 0):,} | Reforço: {breakdown.get('dose_booster', 0):,}</small></p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
        """
        
        metrics_html += """
            </div>
        </div>
        """
        
        return metrics_html
    
    def _generate_charts_section(self, charts: Dict[str, Any]) -> str:
        """Gera seção de gráficos com imagens incorporadas."""
        if not charts or 'total_charts' not in charts or charts['total_charts'] == 0:
            return """
            <div class="section">
                <div class="container">
                    <h3 class="section-title">Gráficos</h3>
                    <div class="alert alert-info">
                        Gráficos não disponíveis neste relatório.
                    </div>
                </div>
            </div>
            """
        
        charts_html = """
        <div class="section">
            <div class="container">
                <h3 class="section-title">Visualizações</h3>
                <div class="row">
        """
        
        # Gráfico diário
        if 'daily_cases' in charts and isinstance(charts['daily_cases'], dict):
            daily_info = charts['daily_cases']
            total_cases = daily_info.get('total_cases', 0)
            avg_cases = daily_info.get('avg_daily_cases', 0)
            peak_date = daily_info.get('peak_date', 'N/A')
            peak_cases = daily_info.get('peak_cases', 0)
            
            # Tentar embedar a imagem
            image_html = ""
            file_path = daily_info.get('file_path', '')
            if file_path and os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        image_html = f'<img src="data:image/png;base64,{img_base64}" style="width: 100%; max-width: 600px;" alt="Casos Diários">'
                except Exception as e:
                    logger.warning(f"Erro ao embedar imagem diária: {e}")
                    image_html = f'<p class="text-muted">Gráfico salvo em: {file_path}</p>'
            else:
                image_html = f'<p class="text-muted">Gráfico salvo em: {file_path}</p>'
            
            charts_html += f"""
            <div class="col-md-6 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5>Casos Diários</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-info">
                            <p><strong>Total de Casos:</strong> {total_cases:,}</p>
                            <p><strong>Média Diária:</strong> {avg_cases}</p>
                            <p><strong>Pico:</strong> {peak_cases} casos em {peak_date}</p>
                        </div>
                        <div class="chart-placeholder" style="background-color: white; padding: 1rem;">
                            {image_html}
                        </div>
                    </div>
                </div>
            </div>
            """
        
        # Gráfico mensal
        if 'monthly_cases' in charts and isinstance(charts['monthly_cases'], dict):
            monthly_info = charts['monthly_cases']
            total_cases = monthly_info.get('total_cases', 0)
            avg_cases = monthly_info.get('avg_monthly_cases', 0)
            peak_month = monthly_info.get('peak_month', 'N/A')
            peak_cases = monthly_info.get('peak_cases', 0)
            
            # Tentar embedar a imagem
            image_html = ""
            file_path = monthly_info.get('file_path', '')
            if file_path and os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as img_file:
                        img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        image_html = f'<img src="data:image/png;base64,{img_base64}" style="width: 100%; max-width: 600px;" alt="Casos Mensais">'
                except Exception as e:
                    logger.warning(f"Erro ao embedar imagem mensal: {e}")
                    image_html = f'<p class="text-muted">Gráfico salvo em: {file_path}</p>'
            else:
                image_html = f'<p class="text-muted">Gráfico salvo em: {file_path}</p>'
            
            charts_html += f"""
            <div class="col-md-6 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5>Casos Mensais (Tendência Histórica)</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-info">
                            <p><strong>Total de Casos:</strong> {total_cases:,}</p>
                            <p><strong>Média Mensal:</strong> {avg_cases}</p>
                            <p><strong>Pico:</strong> {peak_cases} casos em {peak_month}</p>
                        </div>
                        <div class="chart-placeholder" style="background-color: white; padding: 1rem;">
                            {image_html}
                        </div>
                    </div>
                </div>
            </div>
            """
        
        charts_html += """
                </div>
            </div>
        </div>
        """
        
        return charts_html    
    
    
    def _generate_news_section(self, news_analysis: Dict[str, Any]) -> str:
        """Gera seção de análise de notícias com lista de artigos."""
        if not news_analysis:
            news_analysis = {}
        
        # Extrair artigos
        articles = news_analysis.get('articles', [])
        summary = news_analysis.get('summary', 'Análise de contexto das notícias relacionadas a SRAG.')
        articles_count = news_analysis.get('total_articles_analyzed', len(articles))
        context_score = news_analysis.get('context_score', 0)
        
        # Se não há artigos e summary vazio, mostrar mensagem padrão
        if not articles and not summary:
            return """
            <div class="section">
                <div class="container">
                    <h3 class="section-title">Análise de Notícias</h3>
                    <div class="alert alert-info">
                        <p>Análise de notícias com dados de referência sobre SRAG e situação epidemiológica.</p>
                    </div>
                </div>
            </div>
            """
        
        # Gerar HTML para cada artigo
        articles_html = ""
        if articles:
            for article in articles[:5]:  # Mostrar até 5 artigos
                title = article.get('title', 'Sem título')
                link = article.get('link', '#')
                summary_text = article.get('summary', '')
                source = article.get('source', 'Fonte desconhecida')
                published = article.get('published', '')
                source_type = article.get('source_type', '')
                
                # Truncar summary se muito longo
                if len(summary_text) > 300:
                    summary_text = summary_text[:300] + "..."
                
                # Formatar data se disponível
                published_str = ""
                if published:
                    try:
                        published_str = self._format_datetime_br(published)
                    except:
                        published_str = published[:10] if len(published) > 10 else published
                
                badge_color = "badge-info" if source_type != 'fallback' else "badge-secondary"
                
                articles_html += f"""
                <div class="article-card mb-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-title"><a href="{link}" target="_blank">{title}</a></h6>
                            <p class="card-text small">{summary_text}</p>
                            <div class="article-meta">
                                <span class="badge {badge_color}">{source}</span>
                                <span class="text-muted small ml-2">{published_str}</span>
                            </div>
                        </div>
                    </div>
                </div>
                """
        
        # Se não houver artigos, mostrar mensagem de fallback
        if not articles_html:
            articles_html = """
            <div class="alert alert-info">
                <p>Notícias sobre SRAG não disponíveis no momento. Consulte as fontes oficiais:</p>
                <ul class="mb-0">
                    <li><strong>Ministério da Saúde:</strong> www.saude.gov.br</li>
                    <li><strong>FIOCRUZ:</strong> portal.fiocruz.br</li>
                    <li><strong>OpenDataSUS:</strong> dados sobre SRAG em tempo real</li>
                </ul>
            </div>
            """
        
        news_html = f"""
        <div class="section">
            <div class="container">
                <h3 class="section-title">Análise de Notícias</h3>
                <div class="row">
                    <div class="col-md-8">
                        <h5>Artigos Relevantes</h5>
                        {articles_html}
                    </div>
                    <div class="col-md-4">
                        <div class="card">
                            <div class="card-header">
                                <h5>Contexto</h5>
                            </div>
                            <div class="card-body">
                                <p>{summary}</p>
                                <hr>
                                <p><strong>Artigos Analisados:</strong> {articles_count}</p>
                                <p><strong>Score de Contexto:</strong> {context_score}/10</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
        
        return news_html
    
    def _generate_data_section(self, data_summary: Dict[str, Any]) -> str:
        """Gera seção de resumo dos dados."""
        total_records = data_summary.get('total_records', 0)
        date_range = data_summary.get('date_range', {})
        
        data_html = f"""
        <div class="section">
            <div class="container">
                <h3 class="section-title">Resumo dos Dados</h3>
                <div class="row">
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header">
                                <h5>Informações Gerais</h5>
                            </div>
                            <div class="card-body">
                                <p><strong>Total de Registros:</strong> {total_records:,}</p>
                                <p><strong>Período:</strong> {date_range.get('start', 'N/A')} a {date_range.get('end', 'N/A')}</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header">
                                <h5>Qualidade dos Dados</h5>
                            </div>
                            <div class="card-body">
                                <p><strong>Fonte:</strong> OpenDataSUS</p>
                                <p><strong>Status:</strong> Dados processados e validados</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
        
        return data_html
    
    def _generate_report_footer(self, metadata: Dict[str, Any]) -> str:
        """Gera rodapé do relatório."""
        generation_time_raw = metadata.get('generation_timestamp', '')
        if generation_time_raw:
            generation_time = self._format_datetime_br(generation_time_raw)
        else:
            generation_time = self._get_current_datetime_br()
        
        return f"""
        <div class="report-footer">
            <div class="container">
                <div class="row">
                    <div class="col-md-6">
                        <p><strong>ABC HealthCare Inc.</strong></p>
                        <p>Sistema de Relatórios Automatizados SRAG</p>
                    </div>
                    <div class="col-md-6 text-right">
                        <p>Gerado automaticamente em {generation_time}</p>
                        <p class="text-muted">Dados protegidos por sistemas de privacidade</p>
                        <p class="text-muted"><small>Horário de Brasília (GMT-3)</small></p>
                    </div>
                </div>
            </div>
        </div>
        """
    
    async def generate_executive_summary_with_gemini(
        self,
        metrics: Dict[str, Any],
        news_analysis: Dict[str, Any],
        data_summary: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Gera resumo executivo aprimorado usando Google Gemini.
        
        Args:
            metrics: Métricas calculadas
            news_analysis: Análise de notícias
            data_summary: Resumo dos dados analisados
            
        Returns:
            Dict com resumo executivo e insights
        """
        try:
            gemini = get_gemini_client()
            
            # Gerar insights consolidados
            insights = await gemini.generate_report_insights(
                data_summary, 
                metrics, 
                news_analysis
            )
            
            # Gerar explicações das métricas
            explanations = await gemini.generate_metrics_explanation(metrics)
            
            logger.info("Resumo executivo com Gemini gerado com sucesso")
            
            return {
                'insights': insights,
                'metrics_explanations': explanations,
                'gemini_enhanced': True
            }
            
        except Exception as e:
            logger.warning(f"Erro ao gerar resumo com Gemini, usando fallback: {e}")
            
            # Fallback para resumo tradicional
            summary = self._generate_executive_summary(metrics, news_analysis)
            return {
                'insights': summary,
                'metrics_explanations': {},
                'gemini_enhanced': False
            }
    
    def _generate_executive_summary(
        self, 
        metrics: Dict[str, Any], 
        news_analysis: Dict[str, Any]
    ) -> str:
        """Gera resumo executivo do relatório."""
        try:
            summary_points = []
            
            # Analisar métricas para resumo
            if 'case_increase_rate' in metrics:
                rate = metrics['case_increase_rate'].get('rate', 0)
                if rate > 10:
                    summary_points.append(f"Observado aumento significativo de {rate}% nos casos")
                elif rate < -10:
                    summary_points.append(f"Observada diminuição de {abs(rate)}% nos casos")
                else:
                    summary_points.append("Número de casos relativamente estável")
            
            if 'mortality_rate' in metrics:
                rate = metrics['mortality_rate'].get('rate', 0)
                if rate > 15:
                    summary_points.append(f"Taxa de mortalidade alta: {rate}%")
                elif rate < 5:
                    summary_points.append(f"Taxa de mortalidade baixa: {rate}%")
                else:
                    summary_points.append(f"Taxa de mortalidade moderada: {rate}%")
            
            if 'icu_occupancy_rate' in metrics:
                rate = metrics['icu_occupancy_rate'].get('rate', 0)
                if rate > 40:
                    summary_points.append(f"Alta demanda por UTI: {rate}% dos casos")
                else:
                    summary_points.append(f"Demanda por UTI: {rate}% dos casos")
            
            # Incluir contexto das notícias se disponível
            if news_analysis and 'context_score' in news_analysis:
                score = news_analysis['context_score']
                if score > 7:
                    summary_points.append("Notícias confirmam tendências observadas nas métricas")
                elif score > 4:
                    summary_points.append("Notícias parcialmente relacionadas às métricas")
                else:
                    summary_points.append("Limitada correlação entre notícias e métricas")
            
            return ". ".join(summary_points) + "." if summary_points else "Resumo não disponível."
            
        except Exception as e:
            logger.error(f"Erro na geração do resumo executivo: {e}")
            return "Erro na geração do resumo executivo."
    
    def _load_html_template(self) -> str:
        """Carrega template HTML base com design profissional moderno."""
        return """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        :root {{
            --primary-color: #2563eb;
            --primary-dark: #1e40af;
            --secondary-color: #64748b;
            --success-color: #10b981;
            --warning-color: #f59e0b;
            --danger-color: #ef4444;
            --light-bg: #f8fafc;
            --card-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.1);
            --card-shadow-hover: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -4px rgba(0, 0, 0, 0.1);
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: var(--light-bg);
            color: #1e293b;
            line-height: 1.6;
        }}
        
        /* Header Styles */
        .report-header {{
            background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #60a5fa 100%);
            color: white;
            padding: 3rem 0 2.5rem;
            margin-bottom: 2.5rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            position: relative;
            overflow: hidden;
        }}
        
        .report-header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: url('data:image/svg+xml,<svg width="100" height="100" xmlns="http://www.w3.org/2000/svg"><defs><pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M 40 0 L 0 0 0 40" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="1"/></pattern></defs><rect width="100" height="100" fill="url(%23grid)" /></svg>');
            opacity: 0.3;
        }}
        
        .report-header .container {{
            position: relative;
            z-index: 1;
        }}
        
        .report-header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
        }}
        
        .report-header h1::before {{
            content: '\\f7f2';
            font-family: 'Font Awesome 6 Free';
            font-weight: 900;
            font-size: 2rem;
            opacity: 0.9;
        }}
        
        .report-header h2 {{
            font-size: 1.25rem;
            font-weight: 400;
            opacity: 0.95;
            margin-bottom: 1rem;
        }}
        
        .report-header .lead {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}
        
        .report-info {{
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(10px);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        
        .report-info p {{
            margin-bottom: 0.75rem;
            font-size: 0.95rem;
        }}
        
        .report-info p:last-child {{
            margin-bottom: 0;
        }}
        
        .badge {{
            padding: 0.5rem 1rem;
            border-radius: 6px;
            font-weight: 600;
            font-size: 0.875rem;
            letter-spacing: 0.025em;
        }}
        
        .badge-primary {{
            background: white;
            color: var(--primary-color);
        }}
        
        /* Section Styles */
        .section {{
            margin-bottom: 3rem;
        }}
        
        .section-title {{
            color: #0f172a;
            font-size: 1.875rem;
            font-weight: 700;
            padding-bottom: 0.75rem;
            margin-bottom: 2rem;
            border-bottom: 3px solid var(--primary-color);
            position: relative;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}
        
        .section-title::before {{
            font-family: 'Font Awesome 6 Free';
            font-weight: 900;
            color: var(--primary-color);
            font-size: 1.5rem;
        }}
        
        .section-title.metrics::before {{
            content: '\\f200';
        }}
        
        .section-title.charts::before {{
            content: '\\f080';
        }}
        
        .section-title.news::before {{
            content: '\\f1ea';
        }}
        
        .section-title.data::before {{
            content: '\\f1c0';
        }}
        
        /* Card Styles */
        .card {{
            border: none;
            border-radius: 12px;
            box-shadow: var(--card-shadow);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            height: 100%;
            background: white;
            overflow: hidden;
        }}
        
        .card:hover {{
            box-shadow: var(--card-shadow-hover);
            transform: translateY(-4px);
        }}
        
        .card-header {{
            padding: 1.25rem 1.5rem;
            font-weight: 600;
            font-size: 1.125rem;
            border-bottom: none;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}
        
        .card-header::before {{
            font-family: 'Font Awesome 6 Free';
            font-weight: 900;
            font-size: 1.25rem;
        }}
        
        .card-header.bg-primary {{
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
            color: white;
        }}
        
        .card-header.bg-primary::before {{
            content: '\\f201';
        }}
        
        .card-header.bg-danger {{
            background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%);
            color: white;
        }}
        
        .card-header.bg-danger::before {{
            content: '\\f48e';
        }}
        
        .card-header.bg-warning {{
            background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
            color: white;
        }}
        
        .card-header.bg-warning::before {{
            content: '\\f0f8';
        }}
        
        .card-header.bg-success {{
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
        }}
        
        .card-header.bg-success::before {{
            content: '\\f48e';
        }}
        
        .card-body {{
            padding: 1.5rem;
        }}
        
        /* Metric Cards */
        .metric-value {{
            font-size: 3rem;
            font-weight: 700;
            line-height: 1;
            margin: 1rem 0;
            background: linear-gradient(135deg, var(--primary-color) 0%, #3b82f6 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .card-header.bg-danger + .card-body .metric-value {{
            background: linear-gradient(135deg, var(--danger-color) 0%, #dc2626 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .card-header.bg-warning + .card-body .metric-value {{
            background: linear-gradient(135deg, var(--warning-color) 0%, #d97706 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .card-header.bg-success + .card-body .metric-value {{
            background: linear-gradient(135deg, var(--success-color) 0%, #059669 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .metric-interpretation {{
            font-size: 1rem;
            color: #64748b;
            margin-bottom: 1.5rem;
            padding: 0.75rem;
            background: #f1f5f9;
            border-radius: 8px;
            border-left: 3px solid var(--primary-color);
        }}
        
        .metric-details {{
            background: #f8fafc;
            padding: 1.25rem;
            border-radius: 8px;
            border-left: 3px solid #e2e8f0;
        }}
        
        .metric-details p {{
            margin-bottom: 0.75rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.5rem 0;
            border-bottom: 1px solid #e2e8f0;
        }}
        
        .metric-details p:last-child {{
            margin-bottom: 0;
            border-bottom: none;
        }}
        
        .metric-details strong {{
            color: #475569;
            font-weight: 600;
        }}
        
        .metric-details hr {{
            margin: 1rem 0;
            border-color: #e2e8f0;
        }}
        
        /* Chart Section */
        .chart-info {{
            background: #f8fafc;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
        }}
        
        .chart-info p {{
            margin-bottom: 0.5rem;
            color: #475569;
        }}
        
        .chart-info p:last-child {{
            margin-bottom: 0;
        }}
        
        .chart-placeholder {{
            background: white;
            padding: 1.5rem;
            border-radius: 8px;
            border: 2px dashed #e2e8f0;
            text-align: center;
            margin-top: 1rem;
        }}
        
        .chart-placeholder img {{
            max-width: 100%;
            height: auto;
            border-radius: 4px;
        }}
        
        /* News Section */
        .article-card {{
            margin-bottom: 1rem;
        }}
        
        .article-card .card {{
            border-left: 4px solid var(--primary-color);
            transition: all 0.3s ease;
        }}
        
        .article-card .card:hover {{
            border-left-color: var(--primary-dark);
        }}
        
        .article-card .card-title {{
            font-size: 1.125rem;
            margin-bottom: 0.75rem;
            font-weight: 600;
        }}
        
        .article-card .card-title a {{
            color: #0f172a;
            text-decoration: none;
            transition: color 0.2s;
        }}
        
        .article-card .card-title a:hover {{
            color: var(--primary-color);
        }}
        
        .article-meta {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex-wrap: wrap;
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid #f1f5f9;
        }}
        
        .badge-info {{
            background: #dbeafe;
            color: #1e40af;
        }}
        
        .badge-secondary {{
            background: #f1f5f9;
            color: #475569;
        }}
        
        /* Alerts */
        .alert {{
            border: none;
            border-radius: 8px;
            padding: 1.25rem;
            border-left: 4px solid;
        }}
        
        .alert-info {{
            background: #dbeafe;
            border-left-color: var(--primary-color);
            color: #1e40af;
        }}
        
        .alert-info ul {{
            margin-bottom: 0;
            padding-left: 1.5rem;
        }}
        
        /* Footer */
        .report-footer {{
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            color: #e2e8f0;
            padding: 2.5rem 0;
            margin-top: 4rem;
            border-top: 4px solid var(--primary-color);
        }}
        
        .report-footer p {{
            margin-bottom: 0.5rem;
        }}
        
        .report-footer .text-muted {{
            color: #94a3b8 !important;
        }}
        
        /* Responsive */
        @media (max-width: 768px) {{
            .report-header h1 {{
                font-size: 1.875rem;
            }}
            
            .section-title {{
                font-size: 1.5rem;
            }}
            
            .metric-value {{
                font-size: 2.25rem;
            }}
        }}
        
        /* Print Styles */
        @media print {{
            body {{
                background: white;
            }}
            
            .card {{
                box-shadow: none;
                border: 1px solid #e2e8f0;
                page-break-inside: avoid;
            }}
            
            .report-header {{
                background: var(--primary-color);
                print-color-adjust: exact;
                -webkit-print-color-adjust: exact;
            }}
        }}
    </style>
</head>
<body>
    {header}
    
    <main>
        {metrics_section}
        {charts_section}
        {news_section}
        {data_section}
    </main>
    
    {footer}
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            // Add section title icons based on content
            const metricsTitle = document.querySelector('.section-title');
            if (metricsTitle && metricsTitle.textContent.includes('Métricas')) {{
                metricsTitle.classList.add('metrics');
            }}
            
            // Smooth scroll behavior
            document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
                anchor.addEventListener('click', function (e) {{
                    e.preventDefault();
                    const target = document.querySelector(this.getAttribute('href'));
                    if (target) {{
                        target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                    }}
                }});
            }});
            
            // Add timestamp
            const timestamp = document.createElement('div');
            timestamp.className = 'text-center text-muted mt-4 pb-3';
            timestamp.innerHTML = '<small>Relatório gerado em: {generation_timestamp}</small>';
            document.querySelector('main').appendChild(timestamp);
        }});
    </script>
</body>
</html>
        """
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de relatórios.
        
        Returns:
            Dict com status de saúde
        """
        try:
            status = {
                'status': 'healthy',
                'timestamp': self._get_current_datetime_br(),
                'timezone': 'America/Sao_Paulo (GMT-3)',
                'output_directory': str(self.output_dir),
                'output_dir_exists': self.output_dir.exists(),
                'output_dir_writable': os.access(self.output_dir, os.W_OK),
                'template_loaded': bool(self.html_template)
            }
            
            # Teste de criação de arquivo
            try:
                test_file = self.output_dir / 'health_check_test.html'
                test_file.write_text('<html><body>Test</body></html>')
                test_file.unlink()
                status['html_generation_test'] = 'ok'
            except Exception as e:
                status['html_generation_test'] = f'error: {str(e)}'
                status['status'] = 'degraded'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': self._get_current_datetime_br(),
                'timezone': 'America/Sao_Paulo (GMT-3)',
                'error': str(e)
            }