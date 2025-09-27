import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import json
import base64

from .base_tool import BaseTool
from ..utils.logger import get_logger

logger = get_logger(__name__)

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
        
        logger.info("ReportGeneratorTool inicializada")
    
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
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_date = metadata.get('report_date', 'unknown')
            html_file = self.output_dir / f"srag_report_{report_date}_{timestamp}.html"
            
            html_file.write_text(html_content, encoding='utf-8')
            
            # Gerar resumo executivo
            executive_summary = self._generate_executive_summary(metrics, news_analysis)
            
            # Preparar resposta
            report_info = {
                'html_file_path': str(html_file),
                'report_date': report_date,
                'generation_timestamp': datetime.now().isoformat(),
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
            
            return report_info
            
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
                generation_timestamp=datetime.now().strftime("%d/%m/%Y às %H:%M")
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"Erro na geração do HTML: {e}")
            raise
    
    def _generate_report_header(self, metadata: Dict[str, Any]) -> str:
        """Gera cabeçalho do relatório."""
        report_date = metadata.get('report_date', 'N/A')
        generation_time = metadata.get('generation_timestamp', 'N/A')
        total_records = metadata.get('total_records', 0)
        
        return f"""
        <div class="report-header">
            <div class="container">
                <div class="row">
                    <div class="col-md-8">
                        <h1 class="display-4">Relatório SRAG</h1>
                        <h2 class="text-muted">Síndrome Respiratória Aguda Grave</h2>
                        <p class="lead">Data de Referência: <strong>{report_date}</strong></p>
                    </div>
                    <div class="col-md-4 text-right">
                        <div class="report-info">
                            <p><strong>Registros Analisados:</strong> {total_records:,}</p>
                            <p><strong>Gerado em:</strong> {generation_time}</p>
                            <div class="badge badge-primary">ABC HealthCare Inc.</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def _generate_metrics_section(self, metrics: Dict[str, Any]) -> str:
        """Gera seção de métricas."""
        metrics_html = """
        <div class="section">
            <div class="container">
                <h3 class="section-title">📊 Métricas Principais</h3>
                <div class="row">
        """
        
        # Mapear nomes das métricas para exibição
        metric_names = {
            'case_increase_rate': ('Taxa de Aumento de Casos', '📈', 'primary'),
            'mortality_rate': ('Taxa de Mortalidade', '💀', 'danger'),
            'icu_occupancy_rate': ('Taxa de Ocupação UTI', '🏥', 'warning'),
            'vaccination_rate': ('Taxa de Vacinação', '💉', 'success')
        }
        
        for metric_key, metric_data in metrics.items():
            if isinstance(metric_data, dict) and 'rate' in metric_data:
                if metric_key in metric_names:
                    name, icon, color = metric_names[metric_key]
                    rate = metric_data.get('rate', 0)
                    interpretation = metric_data.get('interpretation', '')
                    
                    metrics_html += f"""
                    <div class="col-md-6 col-lg-3 mb-4">
                        <div class="metric-card card border-{color}">
                            <div class="card-body text-center">
                                <div class="metric-icon">{icon}</div>
                                <h4 class="metric-name">{name}</h4>
                                <div class="metric-value text-{color}">{rate}%</div>
                                <p class="metric-interpretation">{interpretation}</p>
                            </div>
                        </div>
                    </div>
                    """
        
        metrics_html += """
                </div>
            </div>
        </div>
        """
        
        return metrics_html
    
    def _generate_charts_section(self, charts: Dict[str, Any]) -> str:
        """Gera seção de gráficos."""
        if not charts or 'total_charts' not in charts or charts['total_charts'] == 0:
            return """
            <div class="section">
                <div class="container">
                    <h3 class="section-title">📈 Gráficos</h3>
                    <div class="alert alert-info">
                        Gráficos não disponíveis neste relatório.
                    </div>
                </div>
            </div>
            """
        
        charts_html = """
        <div class="section">
            <div class="container">
                <h3 class="section-title">📈 Visualizações</h3>
                <div class="row">
        """
        
        # Gráfico diário
        if 'daily_cases' in charts and isinstance(charts['daily_cases'], dict):
            daily_info = charts['daily_cases']
            total_cases = daily_info.get('total_cases', 0)
            avg_cases = daily_info.get('avg_daily_cases', 0)
            peak_date = daily_info.get('peak_date', 'N/A')
            peak_cases = daily_info.get('peak_cases', 0)
            
            charts_html += f"""
            <div class="col-md-6 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5>📅 Casos Diários (Últimos 30 dias)</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-info">
                            <p><strong>Total de Casos:</strong> {total_cases:,}</p>
                            <p><strong>Média Diária:</strong> {avg_cases}</p>
                            <p><strong>Pico:</strong> {peak_cases} casos em {peak_date}</p>
                        </div>
                        <div class="chart-placeholder">
                            <p class="text-muted">Gráfico salvo em: {daily_info.get('file_path', 'N/A')}</p>
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
            
            charts_html += f"""
            <div class="col-md-6 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5>📊 Casos Mensais (Últimos 12 meses)</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-info">
                            <p><strong>Total de Casos:</strong> {total_cases:,}</p>
                            <p><strong>Média Mensal:</strong> {avg_cases}</p>
                            <p><strong>Pico:</strong> {peak_cases} casos em {peak_month}</p>
                        </div>
                        <div class="chart-placeholder">
                            <p class="text-muted">Gráfico salvo em: {monthly_info.get('file_path', 'N/A')}</p>
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
        """Gera seção de análise de notícias."""
        if not news_analysis or 'summary' not in news_analysis:
            return """
            <div class="section">
                <div class="container">
                    <h3 class="section-title">📰 Análise de Notícias</h3>
                    <div class="alert alert-info">
                        Análise de notícias não disponível neste relatório.
                    </div>
                </div>
            </div>
            """
        
        summary = news_analysis.get('summary', '')
        articles_count = news_analysis.get('total_articles_analyzed', 0)
        context_score = news_analysis.get('context_score', 0)
        
        news_html = f"""
        <div class="section">
            <div class="container">
                <h3 class="section-title">📰 Análise de Notícias</h3>
                <div class="row">
                    <div class="col-md-8">
                        <div class="card">
                            <div class="card-header">
                                <h5>Contexto das Notícias</h5>
                            </div>
                            <div class="card-body">
                                <p>{summary}</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-4">
                        <div class="card">
                            <div class="card-header">
                                <h5>Estatísticas</h5>
                            </div>
                            <div class="card-body">
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
                <h3 class="section-title">📋 Resumo dos Dados</h3>
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
        generation_time = metadata.get('generation_timestamp', datetime.now().isoformat())
        
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
                    </div>
                </div>
            </div>
        </div>
        """
    
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
        """Carrega template HTML base."""
        return """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f8f9fa;
        }}
        .report-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem 0;
            margin-bottom: 2rem;
        }}
        .section {{
            margin-bottom: 3rem;
        }}
        .section-title {{
            color: #495057;
            border-bottom: 3px solid #007bff;
            padding-bottom: 0.5rem;
            margin-bottom: 2rem;
        }}
        .metric-card {{
            transition: transform 0.2s;
            height: 100%;
        }}
        .metric-card:hover {{
            transform: translateY(-5px);
        }}
        .metric-icon {{
            font-size: 2rem;
            margin-bottom: 1rem;
        }}
        .metric-value {{
            font-size: 2.5rem;
            font-weight: bold;
            margin: 1rem 0;
        }}
        .metric-interpretation {{
            font-size: 0.9rem;
            color: #6c757d;
        }}
        .chart-placeholder {{
            background-color: #e9ecef;
            padding: 2rem;
            border-radius: 0.5rem;
            text-align: center;
            margin-top: 1rem;
        }}
        .report-footer {{
            background-color: #343a40;
            color: white;
            padding: 2rem 0;
            margin-top: 3rem;
        }}
        .report-info {{
            background-color: rgba(255,255,255,0.1);
            padding: 1rem;
            border-radius: 0.5rem;
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
        // Adicionar timestamp de geração
        document.addEventListener('DOMContentLoaded', function() {{
            const timestamp = document.createElement('p');
            timestamp.textContent = 'Relatório gerado em: {generation_timestamp}';
            timestamp.className = 'text-muted text-center mt-3';
            document.body.appendChild(timestamp);
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
                'timestamp': datetime.now().isoformat(),
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
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }