import os
from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path

class LogLevel(Enum):
    """Níveis de log disponíveis."""
    DEBUG = "DEBUG"
    INFO = "INFO" 
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class SecurityLevel(Enum):
    """Níveis de segurança do sistema."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

@dataclass
class DatabaseSettings:
    """Configurações de acesso a dados."""
    data_path: str = "data/raw/srag_data.csv"
    processed_data_path: str = "data/processed/"
    chunk_size: int = 10000
    cache_ttl: int = 3600  # 1 hora em segundos
    max_memory_usage_mb: int = 2048
    encoding: str = "latin-1"
    separator: str = ";"
    
    # Colunas essenciais do DataSUS SRAG
    essential_columns: List[str] = field(default_factory=lambda: [
        'DT_NOTIFIC',      # Data de notificação
        'SG_UF',           # Estado
        'ID_MUNICIP',      # Município 
        'CS_SEXO',         # Sexo
        'NU_IDADE_N',      # Idade
        'UTI',             # Internação em UTI
        'SUPORT_VEN',      # Suporte ventilatório
        'EVOLUCAO',        # Evolução do caso
        'DT_EVOLUCA',      # Data da evolução
        'VACINA_COV',      # Vacinação COVID
        'DOSE_1_COV',      # 1ª dose
        'DOSE_2_COV',      # 2ª dose
        'DOSE_REF',        # Dose de reforço
        'FEBRE',           # Sintoma: febre
        'TOSSE',           # Sintoma: tosse
        'DISPNEIA',        # Sintoma: dispneia
        'DESC_RESP',       # Desconforto respiratório
        'SATURACAO',       # Saturação O2
        'DIARREIA',        # Sintoma: diarreia
        'VOMITO'           # Sintoma: vômito
    ])
    
    # Mapeamento de códigos para valores legíveis
    code_mappings: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        'EVOLUCAO': {
            '1': 'Cura',
            '2': 'Óbito por SRAG',
            '3': 'Óbito por outras causas',
            '9': 'Ignorado'
        },
        'CS_SEXO': {
            'M': 'Masculino',
            'F': 'Feminino',
            'I': 'Ignorado'
        },
        'UTI': {
            '1': 'Sim',
            '2': 'Não',
            '9': 'Ignorado'
        },
        'VACINA_COV': {
            '1': 'Sim',
            '2': 'Não',
            '9': 'Ignorado'
        }
    })

@dataclass
class NewsSettings:
    """Configurações de busca de notícias."""
    max_articles: int = 10
    search_days: int = 30
    cache_ttl: int = 1800  # 30 minutos
    timeout_seconds: int = 30
    max_retry_attempts: int = 3
    
    # URLs de feeds RSS confiáveis
    rss_feeds: List[str] = field(default_factory=lambda: [
        'https://g1.globo.com/rss/g1/ciencia-e-saude/',
        'https://www1.folha.uol.com.br/rss/cotidiano.xml',
        'https://saude.estadao.com.br/rss.xml',
        'https://agencia.fiocruz.br/rss.xml',
        'https://portal.fiocruz.br/rss.xml'
    ])
    
    # Termos de busca para SRAG
    search_terms: List[str] = field(default_factory=lambda: [
        'SRAG',
        'Síndrome Respiratória Aguda Grave',
        'síndrome respiratória',
        'internação respiratória',
        'UTI respiratório',
        'casos respiratórios',
        'surto respiratório',
        'epidemia respiratória',
        'vigilância epidemiológica',
        'notificação compulsória'
    ])
    
    # Fontes confiáveis para filtro
    reliable_sources: List[str] = field(default_factory=lambda: [
        'g1.com',
        'folha.uol.com.br', 
        'estadao.com.br',
        'bbc.com',
        'gov.br',
        'saude.gov.br',
        'fiocruz.br',
        'butantan.gov.br',
        'anvisa.gov.br',
        'sus.gov.br'
    ])

@dataclass
class MetricsSettings:
    """Configurações para cálculo de métricas."""
    
    # Períodos padrão para análise (em dias)
    default_periods: Dict[str, int] = field(default_factory=lambda: {
        'case_increase_rate': 30,
        'mortality_rate': 90, 
        'icu_occupancy_rate': 30,
        'vaccination_rate': 90
    })
    
    # Limites de alerta para métricas
    alert_thresholds: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        'case_increase_rate': {
            'warning': 50.0,    # Aumento > 50%
            'critical': 100.0   # Aumento > 100%
        },
        'mortality_rate': {
            'warning': 15.0,    # Mortalidade > 15%
            'critical': 25.0    # Mortalidade > 25%
        },
        'icu_occupancy_rate': {
            'warning': 60.0,    # Ocupação UTI > 60%
            'critical': 80.0    # Ocupação UTI > 80%
        },
        'vaccination_rate': {
            'low_coverage': 50.0,      # Cobertura < 50%
            'good_coverage': 80.0      # Cobertura > 80%
        }
    })
    
    # Validações de limites para detectar erros
    validation_limits: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        'mortality_rate': {'min': 0.0, 'max': 50.0},
        'icu_occupancy_rate': {'min': 0.0, 'max': 100.0},
        'vaccination_rate': {'min': 0.0, 'max': 100.0},
        'case_increase_rate': {'min': -99.0, 'max': 1000.0}
    })

@dataclass
class ChartSettings:
    """Configurações para geração de gráficos."""
    output_formats: List[str] = field(default_factory=lambda: ['png', 'svg', 'html'])
    default_format: str = 'png'
    dpi: int = 300
    figure_size: tuple = (12, 6)
    
    # Configurações visuais
    color_scheme: Dict[str, str] = field(default_factory=lambda: {
        'primary': '#1f77b4',
        'secondary': '#ff7f0e',
        'success': '#2ca02c', 
        'warning': '#d62728',
        'info': '#17a2b8',
        'background': '#f8f9fa'
    })
    
    # Períodos para gráficos
    daily_chart_days: int = 30
    monthly_chart_months: int = 12

@dataclass
class SecuritySettings:
    """Configurações de segurança e privacidade."""
    security_level: SecurityLevel = SecurityLevel.HIGH
    enable_data_anonymization: bool = True
    enable_guardrails: bool = True
    enable_audit_logging: bool = True
    enable_report_signing: bool = True
    
    # Colunas consideradas sensíveis
    sensitive_columns: List[str] = field(default_factory=lambda: [
        'NU_NOTIFIC',      # Número da notificação
        'CO_UNI_NOT',      # Código da unidade notificadora
        'NM_UNIDADE',      # Nome da unidade
        'NM_MUNIC',        # Nome do município
        'NM_PACIENT',      # Nome do paciente
        'CPF',             # CPF
        'IDENTIDADE',      # RG
        'NM_MAE_PAC',      # Nome da mãe
        'DT_NASC',         # Data de nascimento específica
        'NU_TEL',          # Telefone
        'CO_CARTAO_CNS'    # Cartão SUS
    ])
    
    # Termos proibidos em conteúdo de notícias
    prohibited_terms: List[str] = field(default_factory=lambda: [
        'fake news',
        'teoria da conspiração',
        'negacionismo',
        'anti-vacina',
        'desinformação',
        'hoax',
        'golpe',
        'farsa'
    ])

@dataclass  
class SystemSettings:
    """Configurações gerais do sistema."""
    
    # Informações da aplicação
    app_name: str = "Sistema de Relatórios SRAG"
    app_version: str = "1.0.0"
    organization: str = "ABC HealthCare Inc."
    
    # Configurações de performance
    max_workers: int = 4
    timeout_seconds: int = 300
    max_retry_attempts: int = 3
    memory_limit_mb: int = 2048
    
    # Diretórios do sistema
    base_dir: Path = Path(".")
    data_dir: Path = Path("data")
    logs_dir: Path = Path("logs")
    reports_dir: Path = Path("data/reports")
    cache_dir: Path = Path("data/cache")
    temp_dir: Path = Path("data/temp")
    
    # Configurações de logging
    log_level: LogLevel = LogLevel.INFO
    log_file: str = "logs/srag_system.log"
    log_format: str = "JSON"
    log_rotation_size_mb: int = 100
    log_retention_days: int = 30
    
    def __post_init__(self):
        """Criar diretórios necessários após inicialização."""
        for directory in [self.data_dir, self.logs_dir, self.reports_dir, 
                         self.cache_dir, self.temp_dir]:
            directory.mkdir(exist_ok=True, parents=True)

class SRAGSettings:
    """Classe principal para gerenciar todas as configurações do sistema."""
    
    def __init__(self):
        """Inicializa todas as configurações."""
        self.system = SystemSettings()
        self.database = DatabaseSettings()
        self.news = NewsSettings()
        self.metrics = MetricsSettings()
        self.charts = ChartSettings()
        self.security = SecuritySettings()
        
        # Aplicar configurações do ambiente se disponível
        self._load_environment_settings()
    
    def _load_environment_settings(self):
        """Carrega configurações das variáveis de ambiente."""
        
        # Configurações do sistema
        if os.getenv('LOG_LEVEL'):
            try:
                self.system.log_level = LogLevel(os.getenv('LOG_LEVEL'))
            except ValueError:
                pass
        
        if os.getenv('LOG_FILE'):
            self.system.log_file = os.getenv('LOG_FILE')
        
        if os.getenv('MAX_WORKERS'):
            try:
                self.system.max_workers = int(os.getenv('MAX_WORKERS'))
            except ValueError:
                pass
        
        if os.getenv('TIMEOUT_SECONDS'):
            try:
                self.system.timeout_seconds = int(os.getenv('TIMEOUT_SECONDS'))
            except ValueError:
                pass
        
        # Configurações de dados
        if os.getenv('DATA_PATH'):
            self.database.data_path = os.getenv('DATA_PATH')
        
        if os.getenv('CSV_CHUNK_SIZE'):
            try:
                self.database.chunk_size = int(os.getenv('CSV_CHUNK_SIZE'))
            except ValueError:
                pass
        
        if os.getenv('CACHE_TTL'):
            try:
                self.database.cache_ttl = int(os.getenv('CACHE_TTL'))
            except ValueError:
                pass
        
        # Configurações de notícias
        if os.getenv('MAX_NEWS_ARTICLES'):
            try:
                self.news.max_articles = int(os.getenv('MAX_NEWS_ARTICLES'))
            except ValueError:
                pass
        
        if os.getenv('NEWS_SEARCH_DAYS'):
            try:
                self.news.search_days = int(os.getenv('NEWS_SEARCH_DAYS'))
            except ValueError:
                pass
        
        # Configurações de segurança
        if os.getenv('SECURITY_LEVEL'):
            try:
                self.security.security_level = SecurityLevel(os.getenv('SECURITY_LEVEL'))
            except ValueError:
                pass
        
        if os.getenv('ENABLE_DATA_ANONYMIZATION'):
            self.security.enable_data_anonymization = os.getenv('ENABLE_DATA_ANONYMIZATION').lower() == 'true'
        
        if os.getenv('ENABLE_GUARDRAILS'):
            self.security.enable_guardrails = os.getenv('ENABLE_GUARDRAILS').lower() == 'true'
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumo das configurações (sem dados sensíveis)."""
        return {
            'app_name': self.system.app_name,
            'app_version': self.system.app_version,
            'organization': self.system.organization,
            'log_level': self.system.log_level.value,
            'security_level': self.security.security_level.value,
            'data_path_exists': os.path.exists(self.database.data_path),
            'anonymization_enabled': self.security.enable_data_anonymization,
            'guardrails_enabled': self.security.enable_guardrails,
            'max_workers': self.system.max_workers,
            'timeout_seconds': self.system.timeout_seconds,
            'max_news_articles': self.news.max_articles,
            'essential_columns_count': len(self.database.essential_columns),
            'rss_feeds_count': len(self.news.rss_feeds)
        }
    
    def validate_settings(self) -> Dict[str, Any]:
        """Valida se todas as configurações estão corretas."""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Validar arquivo de dados
        if not os.path.exists(self.database.data_path):
            validation_result['errors'].append(
                f"Arquivo de dados não encontrado: {self.database.data_path}"
            )
            validation_result['valid'] = False
        
        # Validar diretórios necessários
        for dir_name, directory in [
            ('logs', self.system.logs_dir),
            ('reports', self.system.reports_dir),
            ('cache', self.system.cache_dir)
        ]:
            if not directory.exists():
                try:
                    directory.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    validation_result['errors'].append(
                        f"Não foi possível criar diretório {dir_name}: {e}"
                    )
                    validation_result['valid'] = False
        
        # Validar configurações numéricas
        if self.system.max_workers < 1:
            validation_result['warnings'].append("max_workers muito baixo, usando padrão")
            self.system.max_workers = 4
        
        if self.database.chunk_size < 1000:
            validation_result['warnings'].append("chunk_size muito baixo, pode afetar performance")
        
        if self.news.max_articles > 50:
            validation_result['warnings'].append("max_articles muito alto, pode ser lento")
        
        # Validar URLs de RSS
        for url in self.news.rss_feeds:
            if not url.startswith(('http://', 'https://')):
                validation_result['warnings'].append(f"URL RSS suspeita: {url}")
        
        return validation_result
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte configurações para dicionário."""
        return {
            'system': {
                'app_name': self.system.app_name,
                'app_version': self.system.app_version,
                'organization': self.system.organization,
                'max_workers': self.system.max_workers,
                'timeout_seconds': self.system.timeout_seconds,
                'log_level': self.system.log_level.value
            },
            'database': {
                'data_path': self.database.data_path,
                'chunk_size': self.database.chunk_size,
                'cache_ttl': self.database.cache_ttl,
                'essential_columns_count': len(self.database.essential_columns)
            },
            'news': {
                'max_articles': self.news.max_articles,
                'search_days': self.news.search_days,
                'rss_feeds_count': len(self.news.rss_feeds)
            },
            'metrics': {
                'default_periods': self.metrics.default_periods,
                'has_alert_thresholds': bool(self.metrics.alert_thresholds)
            },
            'security': {
                'security_level': self.security.security_level.value,
                'anonymization_enabled': self.security.enable_data_anonymization,
                'guardrails_enabled': self.security.enable_guardrails
            }
        }

# Instância global das configurações
settings = SRAGSettings()

# Constantes úteis
SRAG_COLUMNS_MAPPING = settings.database.code_mappings
DEFAULT_CHART_COLORS = settings.charts.color_scheme
SENSITIVE_COLUMNS = settings.security.sensitive_columns
ESSENTIAL_COLUMNS = settings.database.essential_columns

# Funções utilitárias
def get_data_path() -> str:
    """Retorna caminho para dados SRAG."""
    return settings.database.data_path

def get_reports_path() -> Path:
    """Retorna caminho para relatórios."""
    return settings.system.reports_dir

def is_column_sensitive(column_name: str) -> bool:
    """Verifica se coluna contém dados sensíveis."""
    return column_name.upper() in [col.upper() for col in SENSITIVE_COLUMNS]

def get_metric_threshold(metric_name: str, threshold_type: str) -> Optional[float]:
    """Obtém limiar de alerta para métrica específica."""
    return settings.metrics.alert_thresholds.get(metric_name, {}).get(threshold_type)

def validate_metric_value(metric_name: str, value: float) -> Dict[str, Any]:
    """Valida se valor de métrica está dentro dos limites esperados."""
    limits = settings.metrics.validation_limits.get(metric_name, {})
    
    if not limits:
        return {'valid': True, 'message': 'Sem limites definidos'}
    
    min_val = limits.get('min', float('-inf'))
    max_val = limits.get('max', float('inf'))
    
    if value < min_val:
        return {
            'valid': False, 
            'message': f'Valor {value} abaixo do mínimo {min_val}'
        }
    
    if value > max_val:
        return {
            'valid': False,
            'message': f'Valor {value} acima do máximo {max_val}'
        }
    
    return {'valid': True, 'message': 'Valor dentro dos limites'}