import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import re
import hashlib

from .logger import get_logger

logger = get_logger(__name__)

class SRAGGuardrails:
    """
    Sistema de proteção e validação para dados SRAG.
    
    Responsabilidades:
    - Proteção de dados pessoais sensíveis
    - Validação de qualidade dos dados
    - Aplicação de regras de negócio
    - Controle de acesso e auditoria
    """
    
    def __init__(self):
        """Inicializa o sistema de guardrails."""
        
        # Colunas que podem conter dados pessoais identificáveis
        self.sensitive_columns = [
            'ID_MUNICIP',  # Pode ser muito específico
            'NU_NOTIFIC',  # Número da notificação
            'CO_UNI_NOT',  # Código da unidade notificadora
            'NM_UNIDADE',  # Nome da unidade
            'NM_MUNIC',    # Nome do município (específico demais)
            'NM_PACIENT',  # Nome do paciente (se existir)
            'CPF',         # CPF (se existir)
            'IDENTIDADE',  # RG (se existir)
        ]
        
        # Limites para métricas (valores suspeitos)
        self.metric_limits = {
            'mortality_rate': {'min': 0.0, 'max': 50.0, 'warning_threshold': 30.0},
            'icu_occupancy_rate': {'min': 0.0, 'max': 100.0, 'warning_threshold': 80.0},
            'vaccination_rate': {'min': 0.0, 'max': 100.0, 'warning_threshold': 95.0},
            'case_increase_rate': {'min': -100.0, 'max': 1000.0, 'warning_threshold': 200.0}
        }
        
        # Palavras proibidas em conteúdo de notícias
        self.prohibited_news_terms = [
            'fake news', 'teoria da conspiração', 'negacionismo',
            'anti-vacina', 'desinformação', 'hoax'
        ]
        
        logger.info("Sistema de Guardrails inicializado")
    
    def validate_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida solicitação de geração de relatório.
        
        Args:
            request_data: Dados da solicitação
            
        Returns:
            Dict com dados validados
            
        Raises:
            ValueError: Se solicitação inválida
        """
        try:
            logger.info("Validando solicitação de relatório")
            
            validated_request = request_data.copy()
            
            # Validar data do relatório
            report_date = validated_request.get('report_date')
            if not report_date:
                raise ValueError("Data do relatório é obrigatória")
            
            self._validate_date_format(report_date)
            self._validate_date_range(report_date)
            
            # Validar parâmetros booleanos
            for param in ['include_charts', 'include_news']:
                if param in validated_request:
                    if not isinstance(validated_request[param], bool):
                        validated_request[param] = bool(validated_request[param])
            
            # Registrar validação
            validated_request['validation_timestamp'] = datetime.now().isoformat()
            validated_request['validated_by'] = 'SRAGGuardrails'
            
            logger.info("Solicitação validada com sucesso")
            return validated_request
            
        except Exception as e:
            logger.error(f"Erro na validação da solicitação: {e}")
            raise
    
    def validate_health_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Valida e protege dados de saúde sensíveis.
        
        Args:
            data: DataFrame com dados SRAG
            
        Returns:
            DataFrame com dados validados e protegidos
        """
        try:
            logger.info(f"Validando dados de saúde: {len(data)} registros")
            
            # Aplicar anonimização
            anonymized_data = self._anonymize_personal_data(data)
            
            # Validar integridade dos dados
            validated_data = self._validate_data_integrity(anonymized_data)
            
            # Aplicar filtros de qualidade
            filtered_data = self._apply_quality_filters(validated_data)
            
            # Registrar estatísticas de validação
            original_count = len(data)
            final_count = len(filtered_data)
            filtered_count = original_count - final_count
            
            if filtered_count > 0:
                filter_rate = (filtered_count / original_count) * 100
                logger.info(f"Filtrados {filtered_count} registros ({filter_rate:.1f}%)")
                
                # Alerta se muitos dados foram filtrados
                if filter_rate > 20:
                    logger.warning(f"Alta taxa de filtragem: {filter_rate:.1f}%")
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"Erro na validação de dados de saúde: {e}")
            raise
    
    def validate_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida métricas calculadas e identifica valores suspeitos.
        
        Args:
            metrics: Dict com métricas calculadas
            
        Returns:
            Dict com métricas validadas
        """
        try:
            logger.info("Validando métricas calculadas")
            
            validated_metrics = metrics.copy()
            validation_warnings = []
            
            for metric_name, metric_data in validated_metrics.items():
                if isinstance(metric_data, dict) and 'rate' in metric_data:
                    rate_value = metric_data['rate']
                    
                    if metric_name in self.metric_limits:
                        limits = self.metric_limits[metric_name]
                        
                        # Verificar limites mínimos e máximos
                        if rate_value < limits['min']:
                            warning = f"{metric_name}: valor abaixo do mínimo ({rate_value} < {limits['min']})"
                            validation_warnings.append(warning)
                            
                        elif rate_value > limits['max']:
                            warning = f"{metric_name}: valor acima do máximo ({rate_value} > {limits['max']})"
                            validation_warnings.append(warning)
                            
                        # Verificar limiar de alerta
                        elif rate_value > limits['warning_threshold']:
                            warning = f"{metric_name}: valor alto, requer atenção ({rate_value}%)"
                            validation_warnings.append(warning)
                    
                    # Validar se é um número válido
                    if not isinstance(rate_value, (int, float)) or np.isnan(rate_value):
                        warning = f"{metric_name}: valor inválido ({rate_value})"
                        validation_warnings.append(warning)
                        metric_data['rate'] = 0.0
                        metric_data['interpretation'] = "Valor indisponível devido a erro de cálculo"
            
            # Adicionar avisos de validação
            validated_metrics['validation_warnings'] = validation_warnings
            validated_metrics['validation_timestamp'] = datetime.now().isoformat()
            
            if validation_warnings:
                logger.warning(f"Avisos de validação: {len(validation_warnings)}")
                for warning in validation_warnings:
                    logger.warning(warning)
            
            return validated_metrics
            
        except Exception as e:
            logger.error(f"Erro na validação de métricas: {e}")
            raise
    
    def filter_news_content(self, news_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filtra conteúdo de notícias para remover desinformação.
        
        Args:
            news_analysis: Dict com análise de notícias
            
        Returns:
            Dict com conteúdo filtrado
        """
        try:
            logger.info("Filtrando conteúdo de notícias")
            
            filtered_analysis = news_analysis.copy()
            
            if 'articles' in filtered_analysis:
                original_count = len(filtered_analysis['articles'])
                filtered_articles = []
                
                for article in filtered_analysis['articles']:
                    if self._is_article_appropriate(article):
                        filtered_articles.append(article)
                    else:
                        logger.info(f"Artigo filtrado: conteúdo inadequado")
                
                filtered_analysis['articles'] = filtered_articles
                filtered_count = original_count - len(filtered_articles)
                
                if filtered_count > 0:
                    logger.info(f"Filtrados {filtered_count} artigos de notícias")
            
            # Filtrar análise textual
            if 'analysis' in filtered_analysis and isinstance(filtered_analysis['analysis'], str):
                filtered_analysis['analysis'] = self._filter_text_content(
                    filtered_analysis['analysis']
                )
            
            filtered_analysis['content_filter_timestamp'] = datetime.now().isoformat()
            
            return filtered_analysis
            
        except Exception as e:
            logger.error(f"Erro no filtro de notícias: {e}")
            # Em caso de erro, retornar análise vazia por segurança
            return {
                'articles': [],
                'analysis': "Análise de notícias indisponível devido a filtros de segurança",
                'error': str(e)
            }
    
    def validate_final_report(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validação final do relatório antes da entrega.
        
        Args:
            report: Dict com relatório completo
            
        Returns:
            Dict com relatório validado
        """
        try:
            logger.info("Validando relatório final")
            
            validated_report = report.copy()
            
            # Verificar estrutura obrigatória
            required_sections = ['metadata', 'metrics']
            missing_sections = [section for section in required_sections 
                              if section not in validated_report]
            
            if missing_sections:
                raise ValueError(f"Seções obrigatórias ausentes: {missing_sections}")
            
            # Validar metadados
            if 'metadata' in validated_report:
                metadata = validated_report['metadata']
                if not metadata.get('report_date'):
                    raise ValueError("Data do relatório ausente nos metadados")
                
                # Adicionar informações de validação
                metadata['final_validation_timestamp'] = datetime.now().isoformat()
                metadata['validated_by_guardrails'] = True
            
            # Garantir que não há dados pessoais no relatório
            validated_report = self._ensure_no_personal_data(validated_report)
            
            # Adicionar assinatura de validação
            validated_report['guardrails_signature'] = self._generate_validation_signature(
                validated_report
            )
            
            logger.info("Relatório final validado com sucesso")
            return validated_report
            
        except Exception as e:
            logger.error(f"Erro na validação final: {e}")
            raise
    
    def _validate_date_format(self, date_string: str) -> None:
        """
        Valida formato de data.
        
        Args:
            date_string: String da data (YYYY-MM-DD)
            
        Raises:
            ValueError: Se formato inválido
        """
        try:
            datetime.strptime(date_string, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Formato de data inválido: {date_string}. Use YYYY-MM-DD")
    
    def _validate_date_range(self, date_string: str) -> None:
        """
        Valida se data está em faixa aceitável.
        
        Args:
            date_string: String da data
            
        Raises:
            ValueError: Se data fora da faixa
        """
        date_obj = datetime.strptime(date_string, "%Y-%m-%d")
        
        # Data não pode ser muito antiga (máximo 3 anos)
        min_date = datetime.now() - timedelta(days=1095)
        if date_obj < min_date:
            raise ValueError(f"Data muito antiga: {date_string}")
        
        # Data não pode ser futura
        if date_obj > datetime.now():
            raise ValueError(f"Data futura não permitida: {date_string}")
    
    def _anonymize_personal_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Anonimiza dados pessoais identificáveis.
        
        Args:
            data: DataFrame com dados
            
        Returns:
            DataFrame anonimizado
        """
        try:
            # Lista COMPLETA de colunas sensíveis
            sensitive_columns = [
                'CPF', 'NM_PACIENT', 'NU_TEL', 
                'NM_MAE_PAC', 'ENDERECO', 'BAIRRO', 
                'CEP', 'EMAIL', 'NM_FANTASI', 'TELEFONE'
            ]
            
            # Remover colunas sensíveis existentes
            columns_to_drop = [col for col in sensitive_columns if col in data.columns]
            if columns_to_drop:
                data = data.drop(columns=columns_to_drop)
                logger.info(f"Removidas {len(columns_to_drop)} colunas sensíveis: {columns_to_drop}")
            
            # Generalizar municípios para apenas UF
            if 'ID_MUNICIP' in anonymized_data.columns and 'SG_UF' in anonymized_data.columns:
                # Manter apenas UF, remover identificação específica do município
                anonymized_data = anonymized_data.drop(columns=['ID_MUNICIP'])
            
            # Generalizar idades para faixas etárias
            if 'NU_IDADE_N' in anonymized_data.columns:
                # Substituir idade exata por faixa etária
                anonymized_data['FAIXA_ETARIA'] = pd.cut(
                    anonymized_data['NU_IDADE_N'],
                    bins=[0, 12, 18, 60, 120],
                    labels=['0-12', '13-18', '19-60', '60+'],
                    include_lowest=True
                )
                # Manter idade original apenas se necessária para cálculos
                # mas marcar como sensível
                anonymized_data['NU_IDADE_N'] = anonymized_data['NU_IDADE_N'].apply(
                    lambda x: int(x/5)*5 if pd.notna(x) else x  # Arredondar para múltiplos de 5
                )
            
            return anonymized_data
            
        except Exception as e:
            logger.error(f"Erro na anonimização: {e}")
            return data
    
    def _validate_data_integrity(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Valida integridade básica dos dados.
        
        Args:
            data: DataFrame a ser validado
            
        Returns:
            DataFrame validado
        """
        try:
            validated_data = data.copy()
            
            # Remover linhas com todos os valores essenciais nulos
            essential_cols = ['DT_NOTIFIC', 'SG_UF']
            available_essential = [col for col in essential_cols if col in validated_data.columns]
            
            if available_essential:
                validated_data = validated_data.dropna(subset=available_essential, how='all')
            
            # Validar consistência de datas
            if all(col in validated_data.columns for col in ['DT_NOTIFIC', 'DT_EVOLUCA']):
                # Data de evolução não pode ser anterior à notificação
                date_mask = (
                    validated_data['DT_EVOLUCA'].isna() |
                    (validated_data['DT_EVOLUCA'] >= validated_data['DT_NOTIFIC'])
                )
                validated_data = validated_data[date_mask]
            
            # Validar campos categóricos
            if 'EVOLUCAO' in validated_data.columns:
                valid_evolucao = validated_data['EVOLUCAO'].isin(['1', '2', '3', '9'])
                validated_data = validated_data[valid_evolucao | validated_data['EVOLUCAO'].isna()]
            
            return validated_data
            
        except Exception as e:
            logger.error(f"Erro na validação de integridade: {e}")
            return data
    
    def _apply_quality_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica filtros de qualidade dos dados.
        
        Args:
            data: DataFrame a ser filtrado
            
        Returns:
            DataFrame filtrado
        """
        try:
            filtered_data = data.copy()
            
            # Filtrar registros com data de notificação muito antiga ou futura
            if 'DT_NOTIFIC' in filtered_data.columns:
                current_date = datetime.now()
                min_date = current_date - timedelta(days=1095)  # 3 anos
                
                date_mask = (
                    (filtered_data['DT_NOTIFIC'] >= min_date) &
                    (filtered_data['DT_NOTIFIC'] <= current_date)
                )
                filtered_data = filtered_data[date_mask]
            
            # Filtrar idades impossíveis
            if 'NU_IDADE_N' in filtered_data.columns:
                age_mask = (
                    (filtered_data['NU_IDADE_N'] >= 0) &
                    (filtered_data['NU_IDADE_N'] <= 120)
                ) | filtered_data['NU_IDADE_N'].isna()
                
                filtered_data = filtered_data[age_mask]
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"Erro nos filtros de qualidade: {e}")
            return data
    
    def _is_article_appropriate(self, article: Dict[str, Any]) -> bool:
        """
        Verifica se artigo é apropriado para inclusão.
        
        Args:
            article: Dict com dados do artigo
            
        Returns:
            bool: True se apropriado
        """
        try:
            # Verificar título e conteúdo
            text_to_check = ""
            if 'title' in article:
                text_to_check += article['title'].lower()
            if 'content' in article:
                text_to_check += " " + article['content'].lower()
            
            # Procurar termos proibidos
            for term in self.prohibited_news_terms:
                if term in text_to_check:
                    return False
            
            # Verificar se é de fonte confiável (lista básica)
            reliable_sources = [
                'g1.com', 'folha.uol.com.br', 'estadao.com.br', 
                'bbc.com', 'gov.br', 'saude.gov.br',
                'fiocruz.br', 'butantan.gov.br'
            ]
            
            if 'source' in article:
                source = article['source'].lower()
                if any(reliable in source for reliable in reliable_sources):
                    return True
            
            # Se não tem fonte identificada ou não é claramente confiável,
            # aplicar filtros mais rigorosos
            suspicious_terms = ['milagre', 'cura definitiva', '100% eficaz']
            for term in suspicious_terms:
                if term in text_to_check:
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Erro na validação de artigo: {e}")
            return False
    
    def _filter_text_content(self, text: str) -> str:
        """
        Filtra conteúdo textual removendo partes inadequadas.
        
        Args:
            text: Texto a ser filtrado
            
        Returns:
            Texto filtrado
        """
        try:
            filtered_text = text
            
            # Remover menções a termos proibidos
            for term in self.prohibited_news_terms:
                # Substituir por termo neutro
                filtered_text = re.sub(
                    re.escape(term), 
                    '[conteúdo filtrado]', 
                    filtered_text, 
                    flags=re.IGNORECASE
                )
            
            return filtered_text
            
        except Exception as e:
            logger.error(f"Erro no filtro de texto: {e}")
            return text
    
    def _ensure_no_personal_data(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Garante que não há dados pessoais no relatório final.
        
        Args:
            report: Dict com relatório
            
        Returns:
            Dict sem dados pessoais
        """
        try:
            clean_report = report.copy()
            
            # Lista de campos que podem conter dados pessoais
            personal_data_fields = [
                'cpf', 'rg', 'nome', 'endereco', 'telefone', 
                'email', 'identidade', 'paciente'
            ]
            
            # Função recursiva para limpar dicionários aninhados
            def clean_dict(obj):
                if isinstance(obj, dict):
                    cleaned = {}
                    for key, value in obj.items():
                        key_lower = key.lower()
                        if any(field in key_lower for field in personal_data_fields):
                            # Substituir por informação anonimizada
                            cleaned[key] = '[informação removida por privacidade]'
                        else:
                            cleaned[key] = clean_dict(value)
                    return cleaned
                elif isinstance(obj, list):
                    return [clean_dict(item) for item in obj]
                elif isinstance(obj, str):
                    # Verificar se string contém padrões de dados pessoais
                    return self._anonymize_string_patterns(obj)
                else:
                    return obj
            
            return clean_dict(clean_report)
            
        except Exception as e:
            logger.error(f"Erro na remoção de dados pessoais: {e}")
            return report
    
    def _anonymize_string_patterns(self, text: str) -> str:
        """
        Anonimiza padrões de dados pessoais em strings.
        
        Args:
            text: Texto a ser anonimizado
            
        Returns:
            Texto anonimizado
        """
        try:
            anonymized_text = text
            
            # Padrão de CPF (XXX.XXX.XXX-XX)
            cpf_pattern = r'\d{3}\.\d{3}\.\d{3}-\d{2}'
            anonymized_text = re.sub(cpf_pattern, '[CPF removido]', anonymized_text)
            
            # Padrão de telefone
            phone_pattern = r'\(\d{2}\)\s?\d{4,5}-?\d{4}'
            anonymized_text = re.sub(phone_pattern, '[telefone removido]', anonymized_text)
            
            # Padrão de email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            anonymized_text = re.sub(email_pattern, '[email removido]', anonymized_text)
            
            return anonymized_text
            
        except Exception as e:
            logger.error(f"Erro na anonimização de padrões: {e}")
            return text
    
    def _generate_validation_signature(self, report: Dict[str, Any]) -> str:
        """
        Gera assinatura de validação para o relatório.
        
        Args:
            report: Dict com relatório
            
        Returns:
            String com assinatura hash
        """
        try:
            import json
            import hashlib
            
            # Serializar de forma DETERMINÍSTICA
            report_str = json.dumps(report, sort_keys=True, default=str)
            
            # Gerar hash
            hash_obj = hashlib.sha256(report_str.encode()) 
            signature = f"SRAG-{hash_obj.hexdigest()[:16]}"
            
            return signature
            
        except Exception as e:
            logger.error(f"Erro ao gerar assinatura: {e}")
            # Usar hash do timestamp como fallback
            import hashlib
            fallback = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:16]
            return f"SRAG-{fallback}"