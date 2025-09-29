"""
Sistema de logging estruturado para o Sistema SRAG
"""

import logging
import structlog
from datetime import datetime
import os

def setup_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Configura logger estruturado para o componente.
    
    Args:
        name: Nome do componente
        
    Returns:
        Logger configurado
    """
    # Criar diretório de logs se não existir
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configurar logging básico
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{log_dir}/srag_system.log")
        ]
    )
    
    # Configurar structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger(name)

def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Obtém logger para o componente especificado."""
    return structlog.get_logger(name)