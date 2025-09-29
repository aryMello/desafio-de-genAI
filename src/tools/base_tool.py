"""
Classe base para ferramentas do Sistema SRAG
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional
import uuid

from ..utils.logger import get_logger

class BaseTool(ABC):
    """
    Classe base para todas as ferramentas do sistema.
    
    Define interface comum e funcionalidades compartilhadas
    como logging, cache e monitoramento.
    """
    
    def __init__(self, tool_name: str):
        """
        Inicializa ferramenta base.
        
        Args:
            tool_name: Nome identificador da ferramenta
        """
        self.tool_name = tool_name
        self.tool_id = str(uuid.uuid4())
        self.created_at = datetime.now()
        self.usage_count = 0
        self.last_used = None
        
        # Logger específico da ferramenta
        self.logger = get_logger(f"tool.{tool_name}")
        
        # Estatísticas de uso
        self.execution_stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'average_execution_time': 0.0,
            'last_execution_time': None
        }
        
        self.logger.info(f"Ferramenta {tool_name} inicializada", tool_id=self.tool_id)
    
    def log_execution_start(self, operation: str, params: Dict[str, Any]) -> str:
        """
        Registra início de execução da ferramenta.
        
        Args:
            operation: Nome da operação
            params: Parâmetros da operação
            
        Returns:
            ID da execução para tracking
        """
        execution_id = str(uuid.uuid4())
        
        self.logger.info("Execução iniciada",
                        execution_id=execution_id,
                        operation=operation,
                        tool_name=self.tool_name)
        
        self.usage_count += 1
        self.last_used = datetime.now()
        
        return execution_id
    
    def log_execution_end(
        self, 
        execution_id: str, 
        success: bool, 
        execution_time: float,
        result_summary: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Registra fim de execução da ferramenta.
        
        Args:
            execution_id: ID da execução
            success: Se execução foi bem-sucedida
            execution_time: Tempo de execução em segundos
            result_summary: Resumo do resultado
            error: Mensagem de erro se houver
        """
        # Atualizar estatísticas
        self.execution_stats['total_executions'] += 1
        
        if success:
            self.execution_stats['successful_executions'] += 1
        else:
            self.execution_stats['failed_executions'] += 1
        
        # Calcular tempo médio de execução
        current_avg = self.execution_stats['average_execution_time']
        total_execs = self.execution_stats['total_executions']
        
        new_avg = ((current_avg * (total_execs - 1)) + execution_time) / total_execs
        self.execution_stats['average_execution_time'] = new_avg
        self.execution_stats['last_execution_time'] = execution_time
        
        # Log estruturado
        self.logger.info("Execução finalizada",
                        execution_id=execution_id,
                        success=success,
                        execution_time=execution_time,
                        error=error)
    
    def get_tool_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas da ferramenta."""
        success_rate = 0.0
        if self.execution_stats['total_executions'] > 0:
            success_rate = (
                self.execution_stats['successful_executions'] / 
                self.execution_stats['total_executions']
            ) * 100
        
        return {
            'tool_name': self.tool_name,
            'tool_id': self.tool_id,
            'created_at': self.created_at.isoformat(),
            'usage_count': self.usage_count,
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'execution_stats': self.execution_stats.copy(),
            'success_rate_percent': round(success_rate, 2),
            'uptime_seconds': (datetime.now() - self.created_at).total_seconds()
        }
    
    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta.
        Deve ser implementado por cada ferramenta específica.
        
        Returns:
            Dict com status de saúde
        """
        pass