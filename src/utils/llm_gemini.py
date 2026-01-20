"""
Módulo de integração com Google Gemini LLM
"""

import warnings
# Suppress the FutureWarning about deprecated google.generativeai package
warnings.filterwarnings('ignore', category=FutureWarning, module='google.generativeai')

import google.generativeai as genai
from typing import Optional, Dict, Any, List
import asyncio
from functools import lru_cache

from .config import Config
from .logger import get_logger

logger = get_logger(__name__)


class GeminiLLM:
    """
    Wrapper para Google Gemini API.
    
    Fornece interface simplificada para:
    - Análise de contexto de notícias
    - Geração de explicações e comentários
    - Síntese de dados e métricas
    """
    
    def __init__(self):
        """Inicializa cliente Gemini com configurações."""
        self.config = Config()
        self.api_key = self.config.get('GEMINI_API_KEY')
        self.model_name = self.config.get('GEMINI_MODEL', 'gemini-2.5-flash')
        self.temperature = self.config.get('GEMINI_TEMPERATURE', 0.7)
        self.max_tokens = self.config.get('GEMINI_MAX_TOKENS', 2048)
        
        if not self.api_key:
            logger.warning("GEMINI_API_KEY não configurada. Modo fallback ativado.")
            self.model = None
        else:
            try:
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel(self.model_name)
                logger.info(f"Gemini LLM inicializado com modelo: {self.model_name}")
            except Exception as e:
                logger.error(f"Erro ao inicializar Gemini: {e}")
                self.model = None
    
    async def generate_news_analysis(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> str:
        """
        Gera análise contextualizada das notícias em relação às métricas.
        
        Args:
            articles: Lista de artigos de notícias
            metrics: Dict com métricas calculadas
            
        Returns:
            String com análise gerada pelo Gemini
        """
        try:
            # Preparar contexto
            news_context = self._prepare_news_context(articles)
            metrics_context = self._prepare_metrics_context(metrics)
            
            prompt = f"""
Você é um especialista em saúde pública e análise epidemiológica.
Analise as notícias abaixo em relação aos dados de SRAG (Síndrome Respiratória Aguda Grave) fornecidos.
Forneça uma análise concisa e informativa que explique o cenário epidemiológico atual.

MÉTRICAS ATUAIS DE SRAG:
{metrics_context}

NOTÍCIAS RECENTES:
{news_context}

Por favor, forneça:
1. Uma avaliação do cenário epidemiológico descrito pelas notícias
2. Correlação entre as notícias e as métricas apresentadas
3. Possíveis implicações para a saúde pública
4. Recomendações de monitoramento

Mantenha a análise objetiva e baseada nos dados apresentados.
"""
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                self._generate_with_timeout, 
                prompt
            )
            
            logger.info("Análise de notícias gerada com sucesso via Gemini")
            return response
            
        except Exception as e:
            logger.error(f"Erro ao gerar análise com Gemini: {e}")
            return self._generate_fallback_analysis(articles, metrics)
    
    async def generate_metrics_explanation(
        self, 
        metrics: Dict[str, Any],
        previous_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Gera explicações em linguagem natural para as métricas.
        
        Args:
            metrics: Métricas atuais
            previous_metrics: Métricas anteriores para comparação (opcional)
            
        Returns:
            Dict com explicações para cada métrica
        """
        try:
            metrics_context = self._prepare_metrics_context(metrics)
            
            comparison = ""
            if previous_metrics:
                comparison = f"\nMÉTRICAS ANTERIORES PARA COMPARAÇÃO:\n{self._prepare_metrics_context(previous_metrics)}"
            
            prompt = f"""
Você é um especialista em epidemiologia e saúde pública.
Analise as seguintes métricas de SRAG e forneça explicações claras e compreensíveis em português.

MÉTRICAS ATUAIS:
{metrics_context}
{comparison}

Para cada métrica, forneça:
1. O que o valor significa
2. Se o valor é preocupante, normal ou positivo
3. O que pode estar causando esse valor
4. Quais ações podem ser recomendadas

Formato a resposta como um JSON com as chaves sendo o nome da métrica e os valores sendo as explicações.
Exemplo:
{{
    "case_increase_rate": "explicação aqui...",
    "mortality_rate": "explicação aqui...",
    "icu_occupancy_rate": "explicação aqui...",
    "vaccination_rate": "explicação aqui..."
}}
"""
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                self._generate_with_timeout, 
                prompt
            )
            
            # Tentar parsear como JSON
            import json
            try:
                explanations = json.loads(response)
            except json.JSONDecodeError:
                # Se não for JSON válido, retornar com a resposta bruta
                explanations = {
                    "general": response,
                    "raw_response": True
                }
            
            logger.info("Explicações de métricas geradas com sucesso via Gemini")
            return explanations
            
        except Exception as e:
            logger.error(f"Erro ao gerar explicações: {e}")
            return self._generate_fallback_explanations(metrics)
    
    async def generate_report_insights(
        self, 
        data_summary: Dict[str, Any],
        metrics: Dict[str, Any],
        news_analysis: Dict[str, Any]
    ) -> str:
        """
        Gera insights consolidados para o relatório final.
        
        Args:
            data_summary: Resumo dos dados analisados
            metrics: Métricas calculadas
            news_analysis: Análise das notícias
            
        Returns:
            String com insights do relatório
        """
        try:
            prompt = f"""
Você é um especialista em saúde pública com experiência em análise de epidemias.
Com base nos dados abaixo, gere um resumo executivo com os principais insights.

RESUMO DOS DADOS:
Total de registros: {data_summary.get('total_records', 0)}
Período analisado: {data_summary.get('date_range', 'N/A')}

PRINCIPAIS MÉTRICAS:
- Taxa de aumento de casos: {metrics.get('case_increase_rate', {}).get('rate', 'N/A')}%
- Taxa de mortalidade: {metrics.get('mortality_rate', {}).get('rate', 'N/A')}%
- Taxa de ocupação UTI: {metrics.get('icu_occupancy_rate', {}).get('rate', 'N/A')}%
- Taxa de vacinação: {metrics.get('vaccination_rate', {}).get('rate', 'N/A')}%

CONTEXTO DE NOTÍCIAS:
{news_analysis.get('summary', 'Sem análise disponível')[:1000]}

Por favor, forneça:
1. Uma avaliação geral da situação epidemiológica
2. Os 3 principais pontos de atenção
3. Recomendações imediatas
4. Perspectivas futuras

Mantenha o texto conciso e profissional, adequado para relatório executivo.
"""
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                self._generate_with_timeout, 
                prompt
            )
            
            logger.info("Insights de relatório gerados com sucesso via Gemini")
            return response
            
        except Exception as e:
            logger.error(f"Erro ao gerar insights: {e}")
            return "Insights não disponíveis neste momento."
    
    def _generate_with_timeout(self, prompt: str) -> str:
        """
        Gera resposta com timeout.
        
        Args:
            prompt: Prompt para o modelo
            
        Returns:
            Resposta do modelo
        """
        try:
            if not self.model:
                raise RuntimeError("Modelo Gemini não inicializado")
            
            generation_config = genai.types.GenerationConfig(
                temperature=self.temperature,
                max_output_tokens=self.max_tokens,
            )
            
            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            if response.text:
                return response.text
            else:
                logger.warning("Resposta vazia do Gemini")
                return ""
                
        except Exception as e:
            logger.error(f"Erro na geração com Gemini: {e}")
            raise
    
    def _prepare_news_context(self, articles: List[Dict[str, Any]]) -> str:
        """Prepara contexto das notícias para o prompt."""
        if not articles:
            return "Nenhuma notícia disponível."
        
        context_parts = []
        for i, article in enumerate(articles[:10], 1):  # Limitar a 10 notícias
            title = article.get('title', 'Sem título')
            source = article.get('source', 'Fonte desconhecida')
            summary = article.get('summary', 'Sem resumo')[:200]
            published = article.get('published', 'Data desconhecida')
            
            context_parts.append(
                f"{i}. [{source}] {title}\n"
                f"   Data: {published}\n"
                f"   Resumo: {summary}\n"
            )
        
        return "\n".join(context_parts)
    
    def _prepare_metrics_context(self, metrics: Dict[str, Any]) -> str:
        """Prepara contexto das métricas para o prompt."""
        context_parts = []
        
        metric_names = {
            'case_increase_rate': 'Taxa de Aumento de Casos',
            'mortality_rate': 'Taxa de Mortalidade',
            'icu_occupancy_rate': 'Taxa de Ocupação de UTI',
            'vaccination_rate': 'Taxa de Vacinação'
        }
        
        for key, name in metric_names.items():
            if key in metrics and isinstance(metrics[key], dict):
                rate = metrics[key].get('rate', 'N/A')
                interpretation = metrics[key].get('interpretation', '')
                period_days = metrics[key].get('period_days', 'N/A')
                
                context_parts.append(
                    f"- {name}: {rate}%\n"
                    f"  Interpretação: {interpretation}\n"
                    f"  Período: {period_days} dias\n"
                )
        
        return "\n".join(context_parts) if context_parts else "Nenhuma métrica disponível."
    
    def _generate_fallback_analysis(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> str:
        """Fallback quando Gemini não está disponível."""
        logger.warning("Usando análise fallback (regras heurísticas)")
        
        if not articles:
            return "Nenhuma notícia relevante encontrada para análise."
        
        # Análise baseada em regras heurísticas
        analysis_parts = [
            f"Análise de Contexto de Notícias ({len(articles)} artigos analisados):\n"
        ]
        
        # Identificar temas
        themes = {'increase': 0, 'deaths': 0, 'hospitalization': 0, 'vaccination': 0}
        
        for article in articles:
            title = article.get('title', '').lower()
            summary = article.get('summary', '').lower()
            content = f"{title} {summary}"
            
            if any(word in content for word in ['aumento', 'alta', 'crescimento']):
                themes['increase'] += 1
            if any(word in content for word in ['óbito', 'morte']):
                themes['deaths'] += 1
            if any(word in content for word in ['uti', 'internação', 'hospital']):
                themes['hospitalization'] += 1
            if any(word in content for word in ['vacina', 'vacinação']):
                themes['vaccination'] += 1
        
        analysis_parts.append(
            f"\nTemas identificados nas notícias:\n"
            f"- Aumento de casos: {themes['increase']} artigos\n"
            f"- Mortalidade: {themes['deaths']} artigos\n"
            f"- Hospitalização: {themes['hospitalization']} artigos\n"
            f"- Vacinação: {themes['vaccination']} artigos\n"
        )
        
        # Correlação com métricas
        case_rate = metrics.get('case_increase_rate', {}).get('rate', 0)
        if case_rate > 10 and themes['increase'] > 0:
            analysis_parts.append(
                "\nAs notícias refletem o aumento de casos observado nas métricas."
            )
        
        return "\n".join(analysis_parts)
    
    def _generate_fallback_explanations(
        self, 
        metrics: Dict[str, Any]
    ) -> Dict[str, str]:
        """Fallback para explicações de métricas."""
        logger.warning("Usando explicações fallback (regras heurísticas)")
        
        explanations = {}
        
        if 'case_increase_rate' in metrics:
            rate = metrics['case_increase_rate'].get('rate', 0)
            if rate > 10:
                explanations['case_increase_rate'] = (
                    f"A taxa de aumento de {rate}% é significativa e indica crescimento "
                    "acelerado nos casos. Recomenda-se intensificar monitoramento."
                )
            elif rate > 0:
                explanations['case_increase_rate'] = (
                    f"A taxa de aumento de {rate}% indica crescimento moderado. "
                    "Manutenção de vigilância é recomendada."
                )
            else:
                explanations['case_increase_rate'] = (
                    "A taxa de aumento é estável ou em declínio, indicando tendência positiva."
                )
        
        if 'mortality_rate' in metrics:
            rate = metrics['mortality_rate'].get('rate', 0)
            explanations['mortality_rate'] = (
                f"A taxa de mortalidade de {rate}% reflete a gravidade dos casos. "
                "Monitorar recursos de saúde é essencial."
            )
        
        if 'icu_occupancy_rate' in metrics:
            rate = metrics['icu_occupancy_rate'].get('rate', 0)
            if rate > 80:
                explanations['icu_occupancy_rate'] = (
                    f"Ocupação de UTI de {rate}% está crítica. "
                    "Ação imediata para aumento de capacidade é necessária."
                )
            else:
                explanations['icu_occupancy_rate'] = (
                    f"Ocupação de UTI de {rate}% está dentro dos limites operacionais."
                )
        
        if 'vaccination_rate' in metrics:
            rate = metrics['vaccination_rate'].get('rate', 0)
            if rate > 70:
                explanations['vaccination_rate'] = (
                    f"Taxa de vacinação de {rate}% é boa e fornece proteção populacional."
                )
            else:
                explanations['vaccination_rate'] = (
                    f"Taxa de vacinação de {rate}% indica necessidade de intensificar campanhas."
                )
        
        return explanations


# Instância global do cliente Gemini
_gemini_client: Optional[GeminiLLM] = None


def get_gemini_client() -> GeminiLLM:
    """
    Obtém instância singleton do cliente Gemini.
    
    Returns:
        Instância de GeminiLLM
    """
    global _gemini_client
    if _gemini_client is None:
        _gemini_client = GeminiLLM()
    return _gemini_client
