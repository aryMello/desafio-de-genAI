import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import feedparser
import requests
from bs4 import BeautifulSoup
import re

from .base_tool import BaseTool
from ..utils.logger import get_logger
from ..utils.config import Config

logger = get_logger(__name__)

class NewsSearchTool(BaseTool):
    """
    Ferramenta para busca e análise de notícias relacionadas a SRAG.
    
    Funcionalidades:
    - Busca em múltiplas fontes de notícias
    - Filtros de relevância e confiabilidade
    - Análise de contexto com LLM
    - Rate limiting e cache
    """
    
    def __init__(self):
        """Inicializa ferramenta de busca de notícias."""
        super().__init__("NewsSearchTool")
        
        self.config = Config()
        self.news_api_key = self.config.get('NEWS_API_KEY')
        
        # URLs de RSS feeds confiáveis
        self.rss_feeds = [
            'https://g1.globo.com/rss/g1/ciencia-e-saude/',
            'https://www1.folha.uol.com.br/rss/cotidiano.xml',
            'https://saude.estadao.com.br/rss.xml',
            'https://agencia.fiocruz.br/rss.xml'
        ]
        
        # Termos de busca relacionados a SRAG
        self.search_terms = [
            'SRAG', 'Síndrome Respiratória Aguda Grave',
            'síndrome respiratória', 'internação respiratória',
            'UTI respiratório', 'casos respiratórios',
            'surto respiratório', 'epidemia respiratória'
        ]
        
        # Cache de notícias
        self.news_cache = {}
        
        logger.info("NewsSearchTool inicializada")
    
    async def search_srag_news(
        self, 
        max_articles: int = 10,
        date_range_days: int = 30,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias sobre SRAG em múltiplas fontes.
        
        Args:
            max_articles: Número máximo de artigos
            date_range_days: Período de busca em dias
            sources: Lista de fontes específicas (opcional)
            
        Returns:
            Lista de artigos encontrados
        """
        execution_id = self.log_execution_start("search_srag_news", {
            'max_articles': max_articles,
            'date_range_days': date_range_days
        })
        
        start_time = datetime.now()
        articles = []
        
        try:
            # Buscar em RSS feeds
            rss_articles = await self._search_rss_feeds(date_range_days)
            articles.extend(rss_articles)
            
            # Buscar via News API se configurada
            if self.news_api_key:
                api_articles = await self._search_news_api(date_range_days)
                articles.extend(api_articles)
            
            # Remover duplicatas e filtrar por relevância
            articles = self._deduplicate_articles(articles)
            articles = self._filter_relevant_articles(articles)
            
            # Limitar quantidade
            articles = articles[:max_articles]
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                True, 
                execution_time,
                f"Encontrados {len(articles)} artigos relevantes"
            )
            
            return articles
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                False, 
                execution_time,
                error=str(e)
            )
            
            logger.error(f"Erro na busca de notícias: {e}")
            return []
    
    async def analyze_news_context(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analisa contexto das notícias em relação às métricas.
        
        Args:
            articles: Lista de artigos
            metrics: Métricas calculadas
            
        Returns:
            Dict com análise contextualizada
        """
        execution_id = self.log_execution_start("analyze_news_context", {
            'articles_count': len(articles),
            'metrics_available': list(metrics.keys())
        })
        
        start_time = datetime.now()
        
        try:
            if not articles:
                return {
                    'summary': 'Nenhuma notícia relevante encontrada para análise',
                    'articles': [],
                    'context_score': 0.0
                }
            
            # Extrair textos principais
            news_texts = []
            for article in articles:
                text = f"{article.get('title', '')} {article.get('summary', '')}"
                news_texts.append(text)
            
            # Calcular score de contexto baseado em palavras-chave
            context_score = self._calculate_context_score(news_texts, metrics)
            
            # Gerar resumo das notícias
            summary = self._generate_news_summary(articles, metrics)
            
            analysis = {
                'summary': summary,
                'articles': articles,
                'context_score': context_score,
                'analysis_timestamp': datetime.now().isoformat(),
                'total_articles_analyzed': len(articles)
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                True, 
                execution_time,
                f"Análise de contexto concluída - Score: {context_score}"
            )
            
            return analysis
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                False, 
                execution_time,
                error=str(e)
            )
            
            logger.error(f"Erro na análise de contexto: {e}")
            return {
                'summary': 'Erro na análise de contexto das notícias',
                'articles': articles,
                'context_score': 0.0,
                'error': str(e)
            }
    
    async def _search_rss_feeds(self, date_range_days: int) -> List[Dict[str, Any]]:
        """
        Busca notícias em feeds RSS.
        
        Args:
            date_range_days: Período de busca
            
        Returns:
            Lista de artigos dos RSS feeds
        """
        articles = []
        cutoff_date = datetime.now() - timedelta(days=date_range_days)
        
        for rss_url in self.rss_feeds:
            try:
                # Parse do RSS feed
                feed = feedparser.parse(rss_url)
                
                for entry in feed.entries:
                    # Verificar data da publicação
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6])
                    
                    if pub_date and pub_date < cutoff_date:
                        continue
                    
                    # Verificar relevância do título
                    title = entry.get('title', '').lower()
                    if any(term.lower() in title for term in self.search_terms):
                        article = {
                            'title': entry.get('title', ''),
                            'link': entry.get('link', ''),
                            'summary': entry.get('summary', ''),
                            'published': pub_date.isoformat() if pub_date else None,
                            'source': rss_url,
                            'source_type': 'rss'
                        }
                        articles.append(article)
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.warning(f"Erro ao processar RSS {rss_url}: {e}")
        
        return articles
    
    async def _search_news_api(self, date_range_days: int) -> List[Dict[str, Any]]:
        """
        Busca notícias usando News API.
        
        Args:
            date_range_days: Período de busca
            
        Returns:
            Lista de artigos da News API
        """
        if not self.news_api_key:
            return []
        
        articles = []
        
        try:
            # Configurar parâmetros da busca
            from_date = (datetime.now() - timedelta(days=date_range_days)).strftime('%Y-%m-%d')
            
            # Buscar para cada termo
            for term in self.search_terms[:3]:  # Limitar para evitar rate limit
                url = 'https://newsapi.org/v2/everything'
                params = {
                    'q': term,
                    'from': from_date,
                    'language': 'pt',
                    'sortBy': 'relevancy',
                    'apiKey': self.news_api_key,
                    'pageSize': 5
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            for article_data in data.get('articles', []):
                                article = {
                                    'title': article_data.get('title', ''),
                                    'link': article_data.get('url', ''),
                                    'summary': article_data.get('description', ''),
                                    'published': article_data.get('publishedAt', ''),
                                    'source': article_data.get('source', {}).get('name', 'News API'),
                                    'source_type': 'news_api'
                                }
                                articles.append(article)
                
                # Rate limiting
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erro na busca via News API: {e}")
        
        return articles
    
    def _deduplicate_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove artigos duplicados baseado no título.
        
        Args:
            articles: Lista de artigos
            
        Returns:
            Lista sem duplicatas
        """
        seen_titles = set()
        unique_articles = []
        
        for article in articles:
            title = article.get('title', '').strip().lower()
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_articles.append(article)
        
        return unique_articles
    
    def _filter_relevant_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filtra artigos por relevância ao tema SRAG.
        
        Args:
            articles: Lista de artigos
            
        Returns:
            Lista de artigos relevantes
        """
        relevant_articles = []
        
        # Palavras-chave que indicam relevância
        relevant_keywords = [
            'srag', 'respiratória', 'respiratorio', 'uti', 'internação',
            'internacao', 'hospital', 'casos', 'surto', 'epidemia',
            'pneumonia', 'covid', 'influenza', 'h1n1', 'gripe'
        ]
        
        for article in articles:
            title = article.get('title', '').lower()
            summary = article.get('summary', '').lower()
            text_content = f"{title} {summary}"
            
            # Calcular score de relevância
            relevance_score = sum(
                1 for keyword in relevant_keywords 
                if keyword in text_content
            )
            
            # Incluir se tem pelo menos 2 palavras-chave relevantes
            if relevance_score >= 2:
                article['relevance_score'] = relevance_score
                relevant_articles.append(article)
        
        # Ordenar por relevância
        relevant_articles.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        return relevant_articles
    
    def _calculate_context_score(
        self, 
        news_texts: List[str], 
        metrics: Dict[str, Any]
    ) -> float:
        """
        Calcula score de contexto das notícias em relação às métricas.
        
        Args:
            news_texts: Textos das notícias
            metrics: Métricas calculadas
            
        Returns:
            Score de 0.0 a 10.0
        """
        if not news_texts or not metrics:
            return 0.0
        
        combined_text = ' '.join(news_texts).lower()
        context_score = 0.0
        
        # Verificar menções a elementos das métricas
        metric_keywords = {
            'cases': ['casos', 'case', 'notificações', 'registros'],
            'mortality': ['óbito', 'morte', 'mortalidade', 'falecimento'],
            'icu': ['uti', 'terapia intensiva', 'internação'],
            'vaccination': ['vacina', 'vacinação', 'imunização']
        }
        
        for category, keywords in metric_keywords.items():
            mentions = sum(1 for keyword in keywords if keyword in combined_text)
            context_score += min(mentions * 0.5, 2.0)  # Máximo 2 pontos por categoria
        
        # Bonus por recência das notícias
        recent_bonus = len([text for text in news_texts if text]) * 0.2
        context_score += min(recent_bonus, 2.0)  # Máximo 2 pontos de bonus
        
        return min(context_score, 10.0)
    
    def _generate_news_summary(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> str:
        """
        Gera resumo contextualizado das notícias.
        
        Args:
            articles: Lista de artigos
            metrics: Métricas calculadas
            
        Returns:
            Resumo em texto
        """
        if not articles:
            return "Nenhuma notícia relevante encontrada no período analisado."
        
        # Extrair temas principais
        themes = {}
        for article in articles:
            title = article.get('title', '').lower()
            
            if 'aumento' in title or 'alta' in title or 'crescimento' in title:
                themes['increase'] = themes.get('increase', 0) + 1
            
            if 'óbito' in title or 'morte' in title:
                themes['deaths'] = themes.get('deaths', 0) + 1
            
            if 'uti' in title or 'internação' in title:
                themes['hospitalization'] = themes.get('hospitalization', 0) + 1
            
            if 'vacina' in title:
                themes['vaccination'] = themes.get('vaccination', 0) + 1
        
        # Gerar resumo baseado nos temas
        summary_parts = [
            f"Análise de {len(articles)} notícias relevantes sobre SRAG:"
        ]
        
        if themes.get('increase', 0) > 0:
            summary_parts.append(
                f"- {themes['increase']} notícias mencionam aumento ou alta nos casos"
            )
        
        if themes.get('deaths', 0) > 0:
            summary_parts.append(
                f"- {themes['deaths']} notícias abordam questões de mortalidade"
            )
        
        if themes.get('hospitalization', 0) > 0:
            summary_parts.append(
                f"- {themes['hospitalization']} notícias tratam de internações e UTI"
            )
        
        if themes.get('vaccination', 0) > 0:
            summary_parts.append(
                f"- {themes['vaccination']} notícias mencionam vacinação"
            )
        
        # Relacionar com métricas se possível
        if 'case_increase_rate' in metrics:
            rate = metrics['case_increase_rate'].get('rate', 0)
            if rate > 0 and themes.get('increase', 0) > 0:
                summary_parts.append(
                    f"As notícias corroboram o aumento de {rate}% observado nas métricas."
                )
        
        return ' '.join(summary_parts)
    
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica saúde da ferramenta de notícias.
        
        Returns:
            Dict com status de saúde
        """
        try:
            status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'rss_feeds_count': len(self.rss_feeds),
                'news_api_configured': bool(self.news_api_key),
                'cache_entries': len(self.news_cache)
            }
            
            # Testar conectividade com um RSS feed
            try:
                test_feed = feedparser.parse(self.rss_feeds[0])
                status['rss_connectivity'] = 'ok' if test_feed.entries else 'no_entries'
            except Exception as e:
                status['rss_connectivity'] = f'error: {str(e)}'
                status['status'] = 'degraded'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }