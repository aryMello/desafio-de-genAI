import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv

class Config:
    """Classe para gerenciamento de configurações do sistema."""
    
    def __init__(self):
        """Inicializa configurações carregando do .env"""
        load_dotenv()
        
        self._config = {
            # Dados
            'DATA_PATH': os.getenv('DATA_PATH', 'data/raw/srag_data.csv'),
            
            # APIs
            'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
            'NEWS_API_KEY': os.getenv('NEWS_API_KEY'),
            
            # Logs
            'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
            'LOG_FILE': os.getenv('LOG_FILE', 'logs/srag_system.log'),
            
            # Cache
            'CACHE_TTL': int(os.getenv('CACHE_TTL', '3600')),
            'REDIS_URL': os.getenv('REDIS_URL', 'redis://localhost:6379'),
            
            # Sistema
            'MAX_RETRY_ATTEMPTS': int(os.getenv('MAX_RETRY_ATTEMPTS', '3')),
            'TIMEOUT_SECONDS': int(os.getenv('TIMEOUT_SECONDS', '300')),
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Obtém valor de configuração."""
        return self._config.get(key, default)
    
    def is_valid(self) -> Dict[str, Any]:
        """Verifica se configurações essenciais estão presentes."""
        status = {'status': 'healthy', 'issues': []}
        
        # Verificar chave da OpenAI
        if not self._config.get('OPENAI_API_KEY'):
            status['issues'].append('OPENAI_API_KEY não configurada')
            status['status'] = 'warning'
        
        # Verificar caminho dos dados
        data_path = self._config.get('DATA_PATH')
        if not data_path or not os.path.exists(data_path):
            status['issues'].append(f'Arquivo de dados não encontrado: {data_path}')
            status['status'] = 'error'
        
        return status
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumo das configurações (sem dados sensíveis)."""
        return {
            'data_path_exists': os.path.exists(self._config.get('DATA_PATH', '')),
            'openai_configured': bool(self._config.get('OPENAI_API_KEY')),
            'news_api_configured': bool(self._config.get('NEWS_API_KEY')),
            'log_level': self._config.get('LOG_LEVEL'),
            'cache_ttl': self._config.get('CACHE_TTL'),
        }