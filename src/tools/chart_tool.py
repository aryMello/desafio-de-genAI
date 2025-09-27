import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import os
from pathlib import Path

from .base_tool import BaseTool
from ..utils.logger import get_logger

logger = get_logger(__name__)

class ChartGeneratorTool(BaseTool):
    """
    Ferramenta para geração de gráficos e visualizações de dados SRAG.
    
    Funcionalidades:
    - Gráfico de casos diários (últimos 30 dias)
    - Gráfico de casos mensais (últimos 12 meses)
    - Exportação em múltiplos formatos
    - Aplicação de tema e branding
    """
    
    def __init__(self):
        """Inicializa ferramenta de geração de gráficos."""
        super().__init__("ChartGeneratorTool")
        
        # Configurar estilo dos gráficos
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # Diretório de saída
        self.output_dir = Path("data/reports")
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Configurações de cores e tema
        self.color_scheme = {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e', 
            'success': '#2ca02c',
            'warning': '#d62728',
            'info': '#17a2b8',
            'background': '#f8f9fa'
        }
        
        logger.info("ChartGeneratorTool inicializada")
    
    async def generate_daily_cases_chart(
        self, 
        data: pd.DataFrame,
        chart_format: str = 'png'
    ) -> Dict[str, Any]:
        """
        Gera gráfico de casos diários (últimos 30 dias).
        
        Args:
            data: DataFrame com dados SRAG
            chart_format: Formato de saída ('png', 'html', 'svg')
            
        Returns:
            Dict com informações do gráfico gerado
        """
        execution_id = self.log_execution_start("generate_daily_cases_chart", {
            'data_records': len(data),
            'chart_format': chart_format
        })
        
        start_time = datetime.now()
        
        try:
            # Preparar dados para os últimos 30 dias
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
            # Filtrar período
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_data = data[
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= end_date)
            ]
            
            # Agrupar por dia
            daily_cases = period_data.groupby(
                period_data['DT_NOTIFIC'].dt.date
            ).size().reset_index(name='casos')
            daily_cases.columns = ['data', 'casos']
            
            # Preencher dias sem casos
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            full_range = pd.DataFrame({'data': date_range.date})
            daily_cases = full_range.merge(daily_cases, on='data', how='left')
            daily_cases['casos'] = daily_cases['casos'].fillna(0)
            
            # Gerar gráfico
            if chart_format.lower() == 'html':
                chart_info = self._create_daily_plotly_chart(daily_cases)
            else:
                chart_info = self._create_daily_matplotlib_chart(
                    daily_cases, chart_format
                )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                True,
                execution_time,
                f"Gráfico diário criado: {chart_info.get('file_path', 'N/A')}"
            )
            
            return chart_info
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                False,
                execution_time,
                error=str(e)
            )
            
            logger.error(f"Erro na geração do gráfico diário: {e}")
            raise
    
    async def generate_monthly_cases_chart(
        self, 
        data: pd.DataFrame,
        chart_format: str = 'png'
    ) -> Dict[str, Any]:
        """
        Gera gráfico de casos mensais (últimos 12 meses).
        
        Args:
            data: DataFrame com dados SRAG
            chart_format: Formato de saída ('png', 'html', 'svg')
            
        Returns:
            Dict com informações do gráfico gerado
        """
        execution_id = self.log_execution_start("generate_monthly_cases_chart", {
            'data_records': len(data),
            'chart_format': chart_format
        })
        
        start_time = datetime.now()
        
        try:
            # Preparar dados para os últimos 12 meses
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            if 'DT_NOTIFIC' not in data.columns:
                raise ValueError("Coluna DT_NOTIFIC não encontrada nos dados")
            
            # Filtrar período
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'])
            period_data = data[
                (data['DT_NOTIFIC'] >= start_date) & 
                (data['DT_NOTIFIC'] <= end_date)
            ]
            
            # Agrupar por mês
            monthly_cases = period_data.groupby([
                period_data['DT_NOTIFIC'].dt.year,
                period_data['DT_NOTIFIC'].dt.month
            ]).size().reset_index(name='casos')
            
            # Criar coluna de data
            monthly_cases['data'] = pd.to_datetime(
                monthly_cases[['DT_NOTIFIC', 'DT_NOTIFIC']].rename(
                    columns={'DT_NOTIFIC': 'year', 'DT_NOTIFIC': 'month'}
                ).assign(day=1)
            )
            
            # Ordenar por data
            monthly_cases = monthly_cases.sort_values('data')
            
            # Gerar gráfico
            if chart_format.lower() == 'html':
                chart_info = self._create_monthly_plotly_chart(monthly_cases)
            else:
                chart_info = self._create_monthly_matplotlib_chart(
                    monthly_cases, chart_format
                )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                True,
                execution_time,
                f"Gráfico mensal criado: {chart_info.get('file_path', 'N/A')}"
            )
            
            return chart_info
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id,
                False,
                execution_time,
                error=str(e)
            )
            
            logger.error(f"Erro na geração do gráfico mensal: {e}")
            raise
    
    def _create_daily_plotly_chart(self, daily_cases: pd.DataFrame) -> Dict[str, Any]:
        """Cria gráfico diário usando Plotly."""
        try:
            fig = go.Figure()
            
            # Adicionar linha dos casos diários
            fig.add_trace(go.Scatter(
                x=daily_cases['data'],
                y=daily_cases['casos'],
                mode='lines+markers',
                name='Casos Diários',
                line=dict(color=self.color_scheme['primary'], width=2),
                marker=dict(size=6)
            ))
            
            # Adicionar média móvel de 7 dias
            daily_cases['media_movel'] = daily_cases['casos'].rolling(window=7).mean()
            
            fig.add_trace(go.Scatter(
                x=daily_cases['data'],
                y=daily_cases['media_movel'],
                mode='lines',
                name='Média Móvel (7 dias)',
                line=dict(color=self.color_scheme['secondary'], width=2, dash='dash')
            ))
            
            # Configurar layout
            fig.update_layout(
                title={
                    'text': 'Casos Diários de SRAG - Últimos 30 Dias',
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title='Data',
                yaxis_title='Número de Casos',
                hovermode='x unified',
                showlegend=True,
                plot_bgcolor=self.color_scheme['background'],
                height=500
            )
            
            # Salvar arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = self.output_dir / f"daily_cases_{timestamp}.html"
            fig.write_html(str(file_path))
            
            return {
                'file_path': str(file_path),
                'chart_type': 'daily_cases',
                'format': 'html',
                'total_cases': int(daily_cases['casos'].sum()),
                'avg_daily_cases': round(daily_cases['casos'].mean(), 1),
                'peak_date': str(daily_cases.loc[daily_cases['casos'].idxmax(), 'data']),
                'peak_cases': int(daily_cases['casos'].max()),
                'generation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na criação do gráfico Plotly diário: {e}")
            raise
    
    def _create_daily_matplotlib_chart(
        self, 
        daily_cases: pd.DataFrame, 
        chart_format: str
    ) -> Dict[str, Any]:
        """Cria gráfico diário usando Matplotlib."""
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Gráfico de linha
            ax.plot(
                daily_cases['data'], 
                daily_cases['casos'],
                marker='o',
                linewidth=2,
                markersize=4,
                color=self.color_scheme['primary'],
                label='Casos Diários'
            )
            
            # Média móvel
            daily_cases['media_movel'] = daily_cases['casos'].rolling(window=7).mean()
            ax.plot(
                daily_cases['data'],
                daily_cases['media_movel'],
                linestyle='--',
                linewidth=2,
                color=self.color_scheme['secondary'],
                label='Média Móvel (7 dias)'
            )
            
            # Configurações do gráfico
            ax.set_title('Casos Diários de SRAG - Últimos 30 Dias', fontsize=14, fontweight='bold')
            ax.set_xlabel('Data', fontsize=12)
            ax.set_ylabel('Número de Casos', fontsize=12)
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Rotacionar labels do eixo x
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Salvar arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = self.output_dir / f"daily_cases_{timestamp}.{chart_format}"
            plt.savefig(str(file_path), dpi=300, bbox_inches='tight')
            plt.close()
            
            return {
                'file_path': str(file_path),
                'chart_type': 'daily_cases',
                'format': chart_format,
                'total_cases': int(daily_cases['casos'].sum()),
                'avg_daily_cases': round(daily_cases['casos'].mean(), 1),
                'peak_date': str(daily_cases.loc[daily_cases['casos'].idxmax(), 'data']),
                'peak_cases': int(daily_cases['casos'].max()),
                'generation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na criação do gráfico Matplotlib diário: {e}")
            raise
    
    def _create_monthly_plotly_chart(self, monthly_cases: pd.DataFrame) -> Dict[str, Any]:
        """Cria gráfico mensal usando Plotly."""
        try:
            fig = go.Figure()
            
            # Adicionar barras dos casos mensais
            fig.add_trace(go.Bar(
                x=monthly_cases['data'],
                y=monthly_cases['casos'],
                name='Casos Mensais',
                marker_color=self.color_scheme['primary'],
                text=monthly_cases['casos'],
                textposition='auto'
            ))
            
            # Configurar layout
            fig.update_layout(
                title={
                    'text': 'Casos Mensais de SRAG - Últimos 12 Meses',
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title='Mês',
                yaxis_title='Número de Casos',
                showlegend=False,
                plot_bgcolor=self.color_scheme['background'],
                height=500
            )
            
            # Configurar eixo x para mostrar meses
            fig.update_xaxes(
                tickformat='%b %Y',
                dtick='M1'
            )
            
            # Salvar arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = self.output_dir / f"monthly_cases_{timestamp}.html"
            fig.write_html(str(file_path))
            
            return {
                'file_path': str(file_path),
                'chart_type': 'monthly_cases',
                'format': 'html',
                'total_cases': int(monthly_cases['casos'].sum()),
                'avg_monthly_cases': round(monthly_cases['casos'].mean(), 1),
                'peak_month': str(monthly_cases.loc[monthly_cases['casos'].idxmax(), 'data'].strftime('%B %Y')),
                'peak_cases': int(monthly_cases['casos'].max()),
                'generation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na criação do gráfico Plotly mensal: {e}")
            raise
    
    def _create_monthly_matplotlib_chart(
        self, 
        monthly_cases: pd.DataFrame, 
        chart_format: str
    ) -> Dict[str, Any]:
        """Cria gráfico mensal usando Matplotlib."""
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Gráfico de barras
            bars = ax.bar(
                monthly_cases['data'],
                monthly_cases['casos'],
                color=self.color_scheme['primary'],
                alpha=0.8,
                width=20
            )
            
            # Adicionar valores nas barras
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(
                        bar.get_x() + bar.get_width()/2.,
                        height + max(monthly_cases['casos']) * 0.01,
                        f'{int(height)}',
                        ha='center',
                        va='bottom',
                        fontsize=9
                    )
            
            # Configurações do gráfico
            ax.set_title('Casos Mensais de SRAG - Últimos 12 Meses', fontsize=14, fontweight='bold')
            ax.set_xlabel('Mês', fontsize=12)
            ax.set_ylabel('Número de Casos', fontsize=12)
            ax.grid(True, alpha=0.3, axis='y')
            
            # Formatar eixo x
            ax.tick_params(axis='x', rotation=45)
            
            # Ajustar layout
            plt.tight_layout()
            
            # Salvar arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = self.output_dir / f"monthly_cases_{timestamp}.{chart_format}"
            plt.savefig(str(file_path), dpi=300, bbox_inches='tight')
            plt.close()
            
            return {
                'file_path': str(file_path),
                'chart_type': 'monthly_cases',
                'format': chart_format,
                'total_cases': int(monthly_cases['casos'].sum()),
                'avg_monthly_cases': round(monthly_cases['casos'].mean(), 1),
                'peak_month': str(monthly_cases.loc[monthly_cases['casos'].idxmax(), 'data'].strftime('%B %Y')),
                'peak_cases': int(monthly_cases['casos'].max()),
                'generation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na criação do gráfico Matplotlib mensal: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de gráficos.
        
        Returns:
            Dict com status de saúde
        """
        try:
            status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'output_directory': str(self.output_dir),
                'output_dir_exists': self.output_dir.exists(),
                'output_dir_writable': os.access(self.output_dir, os.W_OK)
            }
            
            # Verificar dependências
            try:
                import matplotlib
                status['matplotlib_version'] = matplotlib.__version__
            except ImportError as e:
                status['status'] = 'error'
                status['matplotlib_error'] = str(e)
            
            try:
                import plotly
                status['plotly_version'] = plotly.__version__
            except ImportError as e:
                status['status'] = 'error'
                status['plotly_error'] = str(e)
            
            # Teste de criação de arquivo simples
            try:
                test_file = self.output_dir / 'health_check_test.txt'
                test_file.write_text('test')
                test_file.unlink()
                status['file_creation_test'] = 'ok'
            except Exception as e:
                status['file_creation_test'] = f'error: {str(e)}'
                status['status'] = 'degraded'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }