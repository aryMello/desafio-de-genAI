"""
Sistema de logging estruturado e legível para o Sistema SRAG
"""

import logging
import structlog
from datetime import datetime
import os
from pathlib import Path
import json

class ColorFormatter(logging.Formatter):
    """Formatter com cores e layout limpo para melhor legibilidade"""
    
    # Cores ANSI
    COLORS = {
        'DEBUG': '\033[96m',      # Cyan claro
        'INFO': '\033[92m',       # Verde claro
        'WARNING': '\033[93m',    # Amarelo
        'ERROR': '\033[91m',      # Vermelho claro
        'CRITICAL': '\033[95m',   # Magenta
    }
    
    # Ícones para cada nível
    ICONS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠',
        'ERROR': '✗',
        'CRITICAL': '🔥',
    }
    
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        icon = self.ICONS.get(levelname, '•')
        
        # Timestamp com cor dim
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        timestamp_str = f"{self.DIM}[{timestamp}]{self.RESET}"
        
        # Level com cor e ícone
        level_str = f"{color}{icon} {levelname:8}{self.RESET}"
        
        # Module name
        module_name = record.name.split('.')[-1]
        module_str = f"{self.BOLD}{module_name}{self.RESET}"
        
        # Mensagem
        message = record.getMessage()
        
        # Formatar linha principal
        formatted = f"{timestamp_str} {level_str} {module_str}: {message}"
        
        # Adicionar exceção se existir
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            formatted += f"\n{self.DIM}{exc_text}{self.RESET}"
        
        return formatted


class StructuredConsoleRenderer:
    """Renderiza logs estruturados de forma limpa e organizada"""
    
    # Cores para diferentes tipos de dados
    KEY_COLOR = '\033[94m'      # Azul para chaves
    VALUE_COLOR = '\033[97m'    # Branco para valores
    NUMBER_COLOR = '\033[93m'   # Amarelo para números
    STRING_COLOR = '\033[92m'   # Verde para strings
    RESET = '\033[0m'
    DIM = '\033[2m'
    BOLD = '\033[1m'
    
    def __call__(self, logger, method_name, event_dict):
        """Renderiza evento de log estruturado"""
        
        # Extrair campos principais
        event = event_dict.pop('event', '')
        timestamp = event_dict.pop('timestamp', '')
        level = event_dict.pop('level', 'info').upper()
        
        # Campos menos importantes para remover do output principal
        event_dict.pop('logger', None)
        event_dict.pop('extra', None)
        
        # Construir mensagem principal
        parts = [event]
        
        # Campos prioritários para exibir inline
        priority_fields = ['status', 'error', 'interpretation', 'message', 'result']
        inline_data = {}
        
        for key in priority_fields:
            if key in event_dict:
                value = event_dict.pop(key)
                inline_data[key] = value
        
        # Adicionar dados inline de forma legível
        if inline_data:
            inline_parts = []
            for key, value in inline_data.items():
                formatted_value = self._format_value(value)
                inline_parts.append(f"{self.KEY_COLOR}{key}{self.RESET}={formatted_value}")
            
            parts.append(f"({', '.join(inline_parts)})")
        
        message = ' '.join(parts)
        
        # Se houver dados numéricos restantes, adicionar de forma compacta
        numeric_data = {}
        for key in list(event_dict.keys()):
            value = event_dict[key]
            if isinstance(value, (int, float)) or key in ['rate', 'cases', 'records', 
                                                            'count', 'total', 'size_mb',
                                                            'execution_time', 'charts']:
                numeric_data[key] = event_dict.pop(key)
        
        if numeric_data:
            numeric_parts = []
            for key, value in numeric_data.items():
                formatted_value = self._format_value(value)
                numeric_parts.append(f"{self.DIM}{key}={formatted_value}{self.RESET}")
            
            message += f" [{', '.join(numeric_parts)}]"
        
        # Se houver dados complexos restantes, formatar em linhas separadas
        if event_dict:
            # Remover campos técnicos desnecessários
            event_dict.pop('execution_id', None)
            event_dict.pop('tool_id', None)
            event_dict.pop('tool_name', None)
            
            if event_dict:
                message += "\n" + self._format_nested_dict(event_dict, indent=2)
        
        return message
    
    def _format_value(self, value):
        """Formata um valor com cores apropriadas"""
        if isinstance(value, bool):
            color = '\033[92m' if value else '\033[91m'  # Verde/Vermelho
            return f"{color}{value}{self.RESET}"
        elif isinstance(value, (int, float)):
            return f"{self.NUMBER_COLOR}{value}{self.RESET}"
        elif isinstance(value, str):
            # Strings curtas inline, longas sem cor
            if len(value) < 50:
                return f"{self.STRING_COLOR}{value}{self.RESET}"
            else:
                return value
        elif isinstance(value, (list, dict)):
            return f"{self.DIM}{str(value)[:100]}...{self.RESET}" if len(str(value)) > 100 else str(value)
        else:
            return str(value)
    
    def _format_nested_dict(self, data, indent=0):
        """Formata dicionário aninhado de forma legível"""
        lines = []
        prefix = " " * indent
        
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(f"{prefix}{self.KEY_COLOR}{key}{self.RESET}:")
                lines.append(self._format_nested_dict(value, indent + 2))
            elif isinstance(value, list):
                lines.append(f"{prefix}{self.KEY_COLOR}{key}{self.RESET}: {self._format_value(value)}")
            else:
                formatted_value = self._format_value(value)
                lines.append(f"{prefix}{self.KEY_COLOR}{key}{self.RESET}: {formatted_value}")
        
        return "\n".join(lines)


class JSONFileRenderer:
    """Renderiza logs como JSON para arquivo"""
    
    def __call__(self, logger, method_name, event_dict):
        """Renderiza como JSON compacto"""
        return json.dumps(event_dict, default=str, ensure_ascii=False)


def setup_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Configura logger estruturado com output legível e organizado.
    
    Args:
        name: Nome do componente
        
    Returns:
        Logger configurado
    """
    # Criar diretório de logs se não existir
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True, parents=True)
    
    # Obter configuração do ambiente
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Configurar logging Python padrão
    python_logger = logging.getLogger()
    python_logger.setLevel(log_level)
    
    # Remover handlers antigos
    for handler in python_logger.handlers[:]:
        python_logger.removeHandler(handler)
    
    # Handler para console (colorido e legível)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter())
    console_handler.setLevel(log_level)
    python_logger.addHandler(console_handler)
    
    # Handler para arquivo (JSON estruturado)
    file_handler = logging.FileHandler(log_dir / "srag_system.log", encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    ))
    file_handler.setLevel(logging.DEBUG)  # Sempre DEBUG no arquivo
    python_logger.addHandler(file_handler)
    
    # Handler para erros separado
    error_handler = logging.FileHandler(log_dir / "srag_errors.log", encoding='utf-8')
    error_handler.setFormatter(ColorFormatter())
    error_handler.setLevel(logging.ERROR)
    python_logger.addHandler(error_handler)
    
    # Silenciar loggers verbosos de bibliotecas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    
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
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configurar formatters diferentes para console e arquivo
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=StructuredConsoleRenderer(),
        )
    )
    
    file_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=JSONFileRenderer(),
        )
    )
    
    return structlog.get_logger(name)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Obtém logger para o componente especificado.
    
    Args:
        name: Nome do componente
        
    Returns:
        Logger configurado
    """
    return structlog.get_logger(name)


# Funções helper para logging consistente
def log_execution_start(logger, operation: str, **kwargs):
    """Log padronizado para início de execução"""
    logger.info(
        f"Iniciando {operation}",
        operation=operation,
        **kwargs
    )


def log_execution_end(logger, operation: str, success: bool, execution_time: float, **kwargs):
    """Log padronizado para fim de execução"""
    if success:
        logger.info(
            f"Concluído {operation}",
            operation=operation,
            status="sucesso",
            execution_time=f"{execution_time:.2f}s",
            **kwargs
        )
    else:
        logger.error(
            f"Falhou {operation}",
            operation=operation,
            status="falha",
            execution_time=f"{execution_time:.2f}s",
            **kwargs
        )


def log_metric(logger, metric_name: str, value, **kwargs):
    """Log padronizado para métricas"""
    logger.info(
        f"Métrica calculada: {metric_name}",
        metric=metric_name,
        value=value,
        **kwargs
    )


def log_data_info(logger, description: str, **kwargs):
    """Log padronizado para informações de dados"""
    logger.debug(
        description,
        **kwargs
    )