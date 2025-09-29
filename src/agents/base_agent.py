"""
Classe base para agentes do Sistema SRAG
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, List
import uuid

from ..utils.logger import get_logger

class BaseAgent(ABC):
    """
    Classe base para todos os agentes do sistema.
    
    Define interface comum e funcionalidades compartilhadas
    como logging, auditoria e controle de estado.
    """
    
    def __init__(self, agent_name: str):
        """
        Inicializa agente base.
        
        Args:
            agent_name: Nome identificador do agente
        """
        self.agent_name = agent_name
        self.agent_id = str(uuid.uuid4())
        self.created_at = datetime.now()
        self.execution_count = 0
        
        # Logger específico do agente
        self.logger = get_logger(f"agent.{agent_name}")
        
        # Sistema de auditoria
        self.audit_trail = []
        
        self.logger.info(f"Agente {agent_name} inicializado", agent_id=self.agent_id)
    
    def log_decision(
        self, 
        decision_type: str, 
        decision: str, 
        reasoning: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Registra decisão do agente para auditoria.
        
        Args:
            decision_type: Tipo da decisão
            decision: Descrição da decisão
            reasoning: Justificativa da decisão
            metadata: Dados adicionais
        """
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'agent_name': self.agent_name,
            'agent_id': self.agent_id,
            'decision_type': decision_type,
            'decision': decision,
            'reasoning': reasoning,
            'metadata': metadata or {},
            'execution_count': self.execution_count
        }
        
        self.audit_trail.append(audit_entry)
        
        self.logger.info("Decisão registrada", 
                        decision_type=decision_type,
                        decision=decision)
    
    def get_agent_status(self) -> Dict[str, Any]:
        """Retorna status atual do agente."""
        return {
            'agent_name': self.agent_name,
            'agent_id': self.agent_id,
            'created_at': self.created_at.isoformat(),
            'execution_count': self.execution_count,
            'audit_entries': len(self.audit_trail),
            'uptime_seconds': (datetime.now() - self.created_at).total_seconds()
        }
    
    def get_audit_trail(self) -> List[Dict[str, Any]]:
        """Retorna trilha de auditoria completa."""
        return self.audit_trail.copy()
    
    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde do agente.
        Deve ser implementado por cada agente específico.
        
        Returns:
            Dict com status de saúde
        """
        pass