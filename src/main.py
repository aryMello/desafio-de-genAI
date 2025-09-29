import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import asyncio

# Adicionar diretório raiz ao path ANTES dos imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR) if SCRIPT_DIR.endswith('src') else SCRIPT_DIR
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Agora importar módulos do projeto
try:
    from src.agents.orchestrator import SRAGOrchestrator
    from src.utils.logger import setup_logger
    from src.utils.config import Config
    from src.utils.guardrails import SRAGGuardrails
except ModuleNotFoundError:
    # Se ainda falhar, tentar imports relativos
    from agents.orchestrator import SRAGOrchestrator
    from utils.logger import setup_logger
    from utils.config import Config
    from utils.guardrails import SRAGGuardrails

# Configuração de logging
logger = setup_logger(__name__)

class SRAGApplication:
    """
    Aplicação principal do sistema de relatórios SRAG.
    Ponto de entrada e coordenação geral.
    """
    
    def __init__(self):
        """Inicializa a aplicação com configurações e dependências."""
        self.config = Config()
        self.orchestrator = SRAGOrchestrator()
        self.guardrails = SRAGGuardrails()
        
        logger.info("Aplicação SRAG iniciada")
        logger.info(f"Configuração carregada: {self.config.get_summary()}")
    
    async def generate_report(
        self, 
        report_date: Optional[str] = None,
        include_charts: bool = True,
        include_news: bool = True
    ) -> Dict[str, Any]:
        """
        Gera relatório completo de SRAG.
        
        Args:
            report_date: Data do relatório (YYYY-MM-DD). Se None, usa data atual.
            include_charts: Se deve incluir gráficos
            include_news: Se deve incluir análise de notícias
            
        Returns:
            Dict com o relatório gerado e metadados
            
        Raises:
            ValueError: Se parâmetros inválidos
            Exception: Para erros durante geração
        """
        try:
            # Validação de entrada
            if report_date is None:
                report_date = datetime.now().strftime("%Y-%m-%d")
            
            self._validate_report_date(report_date)
            
            logger.info(f"Iniciando geração de relatório para {report_date}")
            
            # Aplicar guardrails iniciais
            request_data = {
                'report_date': report_date,
                'include_charts': include_charts,
                'include_news': include_news,
                'timestamp': datetime.now().isoformat()
            }
            
            validated_request = self.guardrails.validate_request(request_data)
            
            # Delegar para o orquestrador
            report_result = await self.orchestrator.generate_report(
                validated_request
            )
            
            # Validação final dos resultados
            final_report = self.guardrails.validate_final_report(report_result)
            
            logger.info("Relatório gerado com sucesso")
            return final_report
            
        except Exception as e:
            logger.error(f"Erro na geração do relatório: {str(e)}")
            raise
    
    def _validate_report_date(self, report_date: str) -> None:
        """
        Valida se a data do relatório está em formato correto.
        
        Args:
            report_date: Data no formato YYYY-MM-DD
            
        Raises:
            ValueError: Se data inválida
        """
        try:
            parsed_date = datetime.strptime(report_date, "%Y-%m-%d")
            
            # Verificar se não é muito antiga (máximo 2 anos)
            max_age = datetime.now() - timedelta(days=730)
            if parsed_date < max_age:
                raise ValueError(f"Data muito antiga: {report_date}")
                
            # Verificar se não é futura
            if parsed_date > datetime.now():
                raise ValueError(f"Data futura não permitida: {report_date}")
                
        except ValueError as e:
            logger.error(f"Data de relatório inválida: {report_date}")
            raise
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Retorna status do sistema e componentes.
        
        Returns:
            Dict com informações de status
        """
        try:
            status = {
                'application': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'components': {}
            }
            
            # Verificar componentes
            status['components']['orchestrator'] = self.orchestrator.health_check()
            status['components']['config'] = self.config.is_valid()
            status['components']['data_access'] = self._check_data_access()
            
            # Status geral
            all_healthy = all(
                comp.get('status') == 'healthy' 
                for comp in status['components'].values()
            )
            
            status['overall_status'] = 'healthy' if all_healthy else 'degraded'
            
            return status
            
        except Exception as e:
            logger.error(f"Erro ao verificar status: {str(e)}")
            return {
                'application': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _check_data_access(self) -> Dict[str, Any]:
        """
        Verifica acesso aos dados SRAG.
        
        Returns:
            Dict com status de acesso aos dados
        """
        try:
            data_path = self.config.get('DATA_PATH', 'data/raw/srag_data.csv')
            
            if not os.path.exists(data_path):
                return {
                    'status': 'error',
                    'message': f'Arquivo de dados não encontrado: {data_path}'
                }
            
            # Verificar se arquivo não está vazio
            file_size = os.path.getsize(data_path)
            if file_size == 0:
                return {
                    'status': 'error',
                    'message': 'Arquivo de dados vazio'
                }
            
            return {
                'status': 'healthy',
                'file_path': data_path,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'last_modified': datetime.fromtimestamp(
                    os.path.getmtime(data_path)
                ).isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Erro ao acessar dados: {str(e)}'
            }


async def main():
    """Função principal para execução da aplicação."""
    try:
        # Inicializar aplicação
        app = SRAGApplication()
        
        # Verificar status do sistema
        logger.info("Verificando status do sistema...")
        system_status = app.get_system_status()
        
        if system_status['overall_status'] != 'healthy':
            logger.warning(f"Sistema com problemas: {system_status}")
            if system_status.get('application') == 'error':
                logger.error("Aplicação com erro crítico. Encerrando.")
                return 1
        
        # Gerar relatório
        logger.info("Gerando relatório de SRAG...")
        report = await app.generate_report()
        
        # Exibir resumo
        print("\n" + "="*60)
        print("RELATÓRIO SRAG GERADO COM SUCESSO")
        print("="*60)
        
        if 'summary' in report:
            summary = report['summary']
            print(f"Data do Relatório: {summary.get('report_date', 'N/A')}")
            print(f"Total de Registros: {summary.get('total_records', 'N/A')}")
            print(f"Período Analisado: {summary.get('analysis_period', 'N/A')}")
            
        if 'metrics' in report:
            metrics = report['metrics']
            print("\nMÉTRICAS CALCULADAS:")
            print(f"• Taxa de Aumento de Casos: {metrics.get('case_increase_rate', 'N/A')}%")
            print(f"• Taxa de Mortalidade: {metrics.get('mortality_rate', 'N/A')}%")
            print(f"• Taxa de Ocupação UTI: {metrics.get('icu_occupancy_rate', 'N/A')}%")
            print(f"• Taxa de Vacinação: {metrics.get('vaccination_rate', 'N/A')}%")
        
        if 'output_files' in report:
            print(f"\nArquivos Gerados:")
            for file_type, file_path in report['output_files'].items():
                print(f"• {file_type}: {file_path}")
        
        print("\n" + "="*60)
        logger.info("Aplicação executada com sucesso")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Aplicação interrompida pelo usuário")
        return 0
        
    except Exception as e:
        logger.error(f"Erro crítico na aplicação: {str(e)}")
        return 1


if __name__ == "__main__":
    """Ponto de entrada da aplicação."""
    import argparse
    
    # Parser de argumentos da linha de comando
    parser = argparse.ArgumentParser(
        description="Sistema de Relatórios Automatizados SRAG"
    )
    parser.add_argument(
        '--date', 
        type=str, 
        help='Data do relatório (YYYY-MM-DD). Padrão: hoje'
    )
    parser.add_argument(
        '--no-charts', 
        action='store_true',
        help='Não incluir gráficos no relatório'
    )
    parser.add_argument(
        '--no-news', 
        action='store_true',
        help='Não incluir análise de notícias'
    )
    parser.add_argument(
        '--status-only', 
        action='store_true',
        help='Apenas verificar status do sistema'
    )
    
    args = parser.parse_args()
    
    try:
        if args.status_only:
            # Apenas verificar status
            app = SRAGApplication()
            status = app.get_system_status()
            print("\nSTATUS DO SISTEMA:")
            print("="*40)
            print(f"Status Geral: {status['overall_status']}")
            
            for component, comp_status in status['components'].items():
                print(f"{component}: {comp_status.get('status', 'unknown')}")
                
            sys.exit(0 if status['overall_status'] == 'healthy' else 1)
        
        # Executar aplicação principal
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Erro fatal: {str(e)}")
        sys.exit(1)