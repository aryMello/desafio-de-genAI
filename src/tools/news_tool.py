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
from ..utils.llm_gemini import get_gemini_client

logger = get_logger(__name__)

class NewsSearchTool(BaseTool):
    """
    Ferramenta para busca e análise de notícias relacionadas a SRAG.
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
            'https://agencia.fiocruz.br/rss.xml',
            'https://portal.fiocruz.br/rss.xml',
            'https://www.gov.br/saude/pt-br/assuntos/noticias/rss.xml',
            'https://www.anvisa.gov.br/institucional/rss/noticias.xml',
            'https://www.butantan.gov.br/noticias/rss.xml'
        ]
        
        # Termos de busca relacionados a SRAG 
        self.search_terms = [
            'SRAG',
            'Síndrome Respiratória Aguda Grave',
            'síndrome respiratória',
            'internação respiratória',
            'UTI respiratório',
            'casos respiratórios',
            'surto respiratório',
            'epidemia respiratória',
            'vigilância epidemiológica',
            'notificação compulsória',
            'doença respiratória grave',
            'doença respiratória aguda',
            'vírus respiratório',
            'infecção respiratória',
            'pandemia respiratória',
            'crise respiratória',
            'emergência respiratória',
            'saúde respiratória'
        ]
        
        # Cache de notícias
        self.news_cache = {}
        
        logger.info("NewsSearchTool inicializada")
    
    async def search_srag_news(
        self, 
        max_articles: int = 10,
        date_range_days: int = 120,
        sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias sobre SRAG em múltiplas fontes.
        """
        execution_id = self.log_execution_start("search_srag_news", {
            'max_articles': max_articles,
            'date_range_days': date_range_days
        })
        
        start_time = datetime.now()
        articles = []
        
        try:
            logger.info(f"Iniciando busca de notícias - Range: {date_range_days} dias")
            
            # Buscar em RSS feeds
            rss_articles = await self._search_rss_feeds(date_range_days)
            logger.info(f"RSS feeds retornaram {len(rss_articles)} artigos")
            articles.extend(rss_articles)
            
            # Buscar via News API se configurada
            if self.news_api_key:
                api_articles = await self._search_news_api(date_range_days)
                logger.info(f"News API retornou {len(api_articles)} artigos")
                articles.extend(api_articles)
            else:
                logger.warning("News API key não configurada")
            
            logger.info(f"Total de artigos antes de filtros: {len(articles)}")
            
            # Remover duplicatas e filtrar por relevância
            articles = self._deduplicate_articles(articles)
            logger.info(f"Artigos após deduplicação: {len(articles)}")
            
            articles = self._filter_relevant_articles(articles)
            logger.info(f"Artigos após filtro de relevância: {len(articles)}")
            
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
            
            logger.error(f"Erro na busca de notícias: {e}", exc_info=True)
            return []
    
    async def _search_rss_feeds(self, date_range_days: int) -> List[Dict[str, Any]]:
        """
        Busca notícias em feeds RSS.
        """
        articles = []
        cutoff_date = datetime.now() - timedelta(days=date_range_days)
        
        logger.info(f"Buscando em {len(self.rss_feeds)} RSS feeds")
        logger.info(f"Data de corte: {cutoff_date.isoformat()}")
        
        for rss_url in self.rss_feeds:
            try:
                logger.info(f"Processando RSS: {rss_url}")
                
                # Parse do RSS feed com timeout
                feed = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: feedparser.parse(rss_url)
                )
                
                logger.info(f"Feed {rss_url} retornou {len(feed.entries)} entradas")
                
                for entry in feed.entries:
                    # Verificar data da publicação
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            pub_date = datetime(*entry.published_parsed[:6])
                        except Exception as e:
                            logger.debug(f"Erro ao parsear data: {e}")
                    
                    # Aceita artigos sem data OU dentro do range
                    if pub_date and pub_date < cutoff_date:
                        logger.debug(f"Artigo muito antigo: {entry.get('title', '')[:50]}")
                        continue
                    
                    # Verificar relevância do título
                    title = entry.get('title', '').lower()
                    summary = entry.get('summary', '').lower()
                    content = f"{title} {summary}"
                    
                    # Log de verificação de termos
                    matching_terms = [term for term in self.search_terms if term.lower() in content]
                    
                    if matching_terms:
                        logger.debug(f"Artigo relevante encontrado: {entry.get('title', '')[:50]} - Termos: {matching_terms[:3]}")
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
                logger.error(f"Erro ao processar RSS {rss_url}: {e}", exc_info=True)
        
        logger.info(f"Total de artigos coletados dos RSS feeds: {len(articles)}")
        return articles
    
    async def _search_news_api(self, date_range_days: int) -> List[Dict[str, Any]]:
        """
        Busca notícias usando News API.
        """
        if not self.news_api_key:
            return []
        
        articles = []
        
        try:
            from_date = (datetime.now() - timedelta(days=date_range_days)).strftime('%Y-%m-%d')
            
            logger.info(f"Buscando via News API desde {from_date}")
            
            # Buscar para cada termo
            for term in self.search_terms[:3]:
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
                                # Skip articles with missing critical fields
                                if not article_data.get('title'):
                                    logger.debug("Skipping article without title from News API")
                                    continue
                                
                                article = {
                                    'title': article_data.get('title', ''),
                                    'link': article_data.get('url', ''),
                                    'summary': article_data.get('description', ''),
                                    'published': article_data.get('publishedAt', ''),
                                    'source': article_data.get('source', {}).get('name', 'News API'),
                                    'source_type': 'news_api'
                                }
                                articles.append(article)
                        else:
                            logger.warning(f"News API retornou status {response.status}")
                
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Erro na busca via News API: {e}", exc_info=True)
        
        return articles
    
    def _deduplicate_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove artigos duplicados baseado no título."""
        seen_titles = set()
        unique_articles = []
        
        for article in articles:
            title = article.get('title') or ''  # Handle None values
            if not isinstance(title, str):
                title = str(title) if title else ''
            
            title = title.strip().lower()
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_articles.append(article)
        
        return unique_articles
    
    def _filter_relevant_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filtra artigos por relevância ao tema SRAG.
        """
        relevant_articles = []
        
        relevant_keywords = [
            'srag', 'respiratória', 'respiratorio', 'respiratória', 'respiratório',
            'uti', 'internação', 'internacao', 'hospital', 'casos', 'surto', 'epidemia',
            'pneumonia', 'covid', 'influenza', 'h1n1', 'gripe', 'vírus', 'virus',
            'saúde', 'saude', 'doença', 'doenca', 'óbito', 'obito', 'morte'
        ]
        
        for article in articles:
            # Handle None values safely
            title = article.get('title') or ''
            summary = article.get('summary') or ''
            
            # Ensure they're strings
            if not isinstance(title, str):
                title = str(title) if title else ''
            if not isinstance(summary, str):
                summary = str(summary) if summary else ''
            
            text_content = f"{title.lower()} {summary.lower()}"
            
            # Calcular score de relevância
            relevance_score = sum(
                1 for keyword in relevant_keywords 
                if keyword in text_content
            )
            
            if relevance_score >= 1:
                article['relevance_score'] = relevance_score
                relevant_articles.append(article)
                logger.debug(f"Artigo relevante (score {relevance_score}): {title[:50]}")
            else:
                logger.debug(f"Artigo descartado (score {relevance_score}): {title[:50]}")
        
        # Ordenar por relevância
        relevant_articles.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        return relevant_articles
    
    def _calculate_context_score(
        self, 
        news_texts: List[str], 
        metrics: Dict[str, Any]
    ) -> float:
        """Calcula score de contexto das notícias em relação às métricas."""
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
            context_score += min(mentions * 0.5, 2.0)
        
        # Bonus por recência das notícias
        recent_bonus = len([text for text in news_texts if text]) * 0.2
        context_score += min(recent_bonus, 2.0)
        
        return min(context_score, 10.0)
    
    def _generate_news_summary(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> str:
        """Gera resumo contextualizado das notícias."""
        if not articles:
            return "Nenhuma notícia relevante encontrada no período analisado."
        
        # Extrair temas principais com artigos específicos
        themes = {
            'increase': {'count': 0, 'articles': []},
            'deaths': {'count': 0, 'articles': []},
            'hospitalization': {'count': 0, 'articles': []},
            'vaccination': {'count': 0, 'articles': []},
            'alert': {'count': 0, 'articles': []},
            'prevention': {'count': 0, 'articles': []}
        }
        
        for article in articles:
            title = article.get('title', '').lower()
            summary = article.get('summary', '').lower()
            content = f"{title} {summary}"
            
            article_info = {
                'title': article.get('title', ''),
                'link': article.get('link', ''),
                'source': article.get('source', ''),
                'published': article.get('published', '')
            }
            
            if any(word in content for word in ['aumento', 'alta', 'crescimento', 'dispara', 'cresce', 'sobe']):
                themes['increase']['count'] += 1
                themes['increase']['articles'].append(article_info)
            
            if any(word in content for word in ['óbito', 'morte', 'mortalidade', 'falecimento', 'vítima']):
                themes['deaths']['count'] += 1
                themes['deaths']['articles'].append(article_info)
            
            if any(word in content for word in ['uti', 'internação', 'internacao', 'hospital', 'leito']):
                themes['hospitalization']['count'] += 1
                themes['hospitalization']['articles'].append(article_info)
            
            if any(word in content for word in ['vacina', 'vacinação', 'imunização', 'dose']):
                themes['vaccination']['count'] += 1
                themes['vaccination']['articles'].append(article_info)
            
            if any(word in content for word in ['alerta', 'atenção', 'preocupa', 'risco', 'cuidado']):
                themes['alert']['count'] += 1
                themes['alert']['articles'].append(article_info)
            
            if any(word in content for word in ['prevenção', 'prevenir', 'evitar', 'proteção', 'cuidados']):
                themes['prevention']['count'] += 1
                themes['prevention']['articles'].append(article_info)
        
        # Gerar resumo detalhado
        summary_parts = [
            f"<h3>Panorama Geral</h3>",
            f"<p>Foram analisadas {len(articles)} notícias relevantes sobre SRAG e doenças respiratórias no período. "
        ]
        
        # Análise temática
        active_themes = []
        if themes['increase']['count'] > 0:
            active_themes.append(f"aumento de casos ({themes['increase']['count']})")
        if themes['deaths']['count'] > 0:
            active_themes.append(f"mortalidade ({themes['deaths']['count']})")
        if themes['hospitalization']['count'] > 0:
            active_themes.append(f"internações ({themes['hospitalization']['count']})")
        if themes['vaccination']['count'] > 0:
            active_themes.append(f"vacinação ({themes['vaccination']['count']})")
        
        if active_themes:
            summary_parts.append(f"Os principais temas cobertos pela mídia incluem: {', '.join(active_themes)}.</p>")
        else:
            summary_parts.append("</p>")
        
        # Análise detalhada por tema
        if themes['increase']['count'] > 0:
            summary_parts.append(f"<h4>Aumento de Casos</h4>")
            summary_parts.append(f"<p>{themes['increase']['count']} {'notícia menciona' if themes['increase']['count'] == 1 else 'notícias mencionam'} aumento nos casos respiratórios. ")
            
            # Relacionar com métricas
            if 'case_increase_rate' in metrics:
                rate = metrics['case_increase_rate'].get('rate', 0)
                if rate > 0:
                    summary_parts.append(f"<strong>Isto está alinhado com os dados oficiais que mostram um crescimento de {rate:.1f}% no período analisado.</strong> ")
            
            summary_parts.append("As reportagens destacam:")
            summary_parts.append("<ul>")
            for art in themes['increase']['articles'][:3]:  # Mostrar até 3
                summary_parts.append(f"<li><a href='{art['link']}' target='_blank'>{art['title']}</a></li>")
            summary_parts.append("</ul></p>")
        
        if themes['hospitalization']['count'] > 0:
            summary_parts.append(f"<h4>Internações e Hospitalização</h4>")
            summary_parts.append(f"<p>{themes['hospitalization']['count']} {'notícia aborda' if themes['hospitalization']['count'] == 1 else 'notícias abordam'} questões relacionadas a internações e ocupação hospitalar. ")
            
            # Relacionar com métricas UTI
            if 'icu_occupancy_rate' in metrics:
                icu_rate = metrics['icu_occupancy_rate'].get('rate', 0)
                if icu_rate > 0:
                    summary_parts.append(f"Os dados mostram taxa de ocupação de UTI em {icu_rate:.1f}%. ")
            
            summary_parts.append("Destaques:</p>")
            summary_parts.append("<ul>")
            for art in themes['hospitalization']['articles'][:3]:
                summary_parts.append(f"<li><a href='{art['link']}' target='_blank'>{art['title']}</a></li>")
            summary_parts.append("</ul>")
        
        if themes['deaths']['count'] > 0:
            summary_parts.append(f"<h4>Mortalidade</h4>")
            summary_parts.append(f"<p>{themes['deaths']['count']} {'notícia menciona' if themes['deaths']['count'] == 1 else 'notícias mencionam'} questões de mortalidade. ")
            
            if 'mortality_rate' in metrics:
                mort_rate = metrics['mortality_rate'].get('rate', 0)
                deaths = metrics['mortality_rate'].get('deaths', 0)
                if deaths > 0:
                    summary_parts.append(f"Os dados oficiais registram {deaths} óbitos no período, resultando em taxa de mortalidade de {mort_rate:.2f}%. ")
                else:
                    summary_parts.append("Os dados oficiais não registram óbitos no período analisado. ")
            
            summary_parts.append("Reportagens relacionadas:</p>")
            summary_parts.append("<ul>")
            for art in themes['deaths']['articles'][:3]:
                summary_parts.append(f"<li><a href='{art['link']}' target='_blank'>{art['title']}</a></li>")
            summary_parts.append("</ul>")
        
        if themes['vaccination']['count'] > 0:
            summary_parts.append(f"<h4>Vacinação</h4>")
            summary_parts.append(f"<p>{themes['vaccination']['count']} {'notícia aborda' if themes['vaccination']['count'] == 1 else 'notícias abordam'} campanhas de vacinação e imunização. ")
            
            if 'vaccination_rate' in metrics:
                vac_rate = metrics['vaccination_rate'].get('rate', 0)
                summary_parts.append(f"A taxa de vacinação no período é de {vac_rate:.1f}%. ")
            
            summary_parts.append("Notícias sobre vacinação:</p>")
            summary_parts.append("<ul>")
            for art in themes['vaccination']['articles'][:3]:
                summary_parts.append(f"<li><a href='{art['link']}' target='_blank'>{art['title']}</a></li>")
            summary_parts.append("</ul>")
        
        if themes['alert']['count'] > 0:
            summary_parts.append(f"<h4>Alertas e Recomendações</h4>")
            summary_parts.append(f"<p>{themes['alert']['count']} {'notícia contém' if themes['alert']['count'] == 1 else 'notícias contêm'} alertas ou recomendações de autoridades de saúde.</p>")
        
        # Considerações finais
        summary_parts.append("<h4>Considerações</h4>")
        summary_parts.append("<p>")
        
        if themes['increase']['count'] > 0 and 'case_increase_rate' in metrics:
            rate = metrics['case_increase_rate'].get('rate', 0)
            if rate > 10:
                summary_parts.append(f"<strong>A cobertura midiática reflete adequadamente o cenário epidemiológico atual, com aumento significativo de {rate:.1f}% nos casos.</strong> ")
        
        if themes['prevention']['count'] > 0:
            summary_parts.append("Há ênfase em medidas preventivas nas reportagens. ")
        
        if themes['vaccination']['count'] > 0:
            summary_parts.append("A mídia está dando destaque às campanhas de vacinação, o que é positivo para conscientização pública. ")
        
        summary_parts.append("</p>")
        
        # Lista completa de fontes
        summary_parts.append("<h4>Todas as Notícias Analisadas</h4>")
        summary_parts.append("<ol>")
        for article in articles:
            title = article.get('title', 'Sem título')
            link = article.get('link', '#')
            source = article.get('source', 'Fonte desconhecida')
            published = article.get('published', '')
            
            # Formatar data se disponível
            date_str = ''
            if published:
                try:
                    if 'T' in published:
                        date_obj = datetime.fromisoformat(published.replace('Z', '+00:00'))
                        date_str = f" - {date_obj.strftime('%d/%m/%Y')}"
                except:
                    pass
            
            summary_parts.append(f"<li><a href='{link}' target='_blank'>{title}</a><br><small>{source}{date_str}</small></li>")
        summary_parts.append("</ol>")
        
        return '\n'.join(summary_parts)
    
    async def analyze_news_context(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analisa contexto das notícias em relação às métricas."""
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
            
            # Calcular score de contexto
            context_score = self._calculate_context_score(news_texts, metrics)
            
            # Gerar resumo das notícias (agora em HTML com links)
            summary = self._generate_news_summary(articles, metrics)
            
            # Adicionar metadados úteis aos artigos
            enriched_articles = []
            for article in articles:
                enriched = article.copy()
                
                # Identificar temas do artigo
                content = f"{article.get('title', '')} {article.get('summary', '')}".lower()
                themes = []
                
                if any(word in content for word in ['aumento', 'alta', 'crescimento', 'dispara']):
                    themes.append('Aumento de casos')
                if any(word in content for word in ['óbito', 'morte', 'mortalidade']):
                    themes.append('Mortalidade')
                if any(word in content for word in ['uti', 'internação', 'hospital']):
                    themes.append('Hospitalização')
                if any(word in content for word in ['vacina', 'vacinação', 'imunização']):
                    themes.append('Vacinação')
                if any(word in content for word in ['alerta', 'preocupa', 'risco']):
                    themes.append('Alerta')
                if any(word in content for word in ['prevenção', 'cuidados', 'proteção']):
                    themes.append('Prevenção')
                
                enriched['identified_themes'] = themes
                enriched_articles.append(enriched)
            
            analysis = {
                'summary': summary,
                'articles': enriched_articles,
                'context_score': context_score,
                'analysis_timestamp': datetime.now().isoformat(),
                'total_articles_analyzed': len(articles),
                'themes_breakdown': self._get_themes_breakdown(enriched_articles),
                'sources_breakdown': self._get_sources_breakdown(articles)
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                True, 
                execution_time,
                f"Análise de contexto concluída - Score: {context_score:.1f}/10"
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
            
            logger.error(f"Erro na análise de contexto: {e}", exc_info=True)
            return {
                'summary': 'Erro na análise de contexto das notícias',
                'articles': articles,
                'context_score': 0.0,
                'error': str(e)
            }
    
    def _get_themes_breakdown(self, articles: List[Dict[str, Any]]) -> Dict[str, int]:
        """Retorna contagem de temas identificados."""
        themes_count = {}
        for article in articles:
            for theme in article.get('identified_themes', []):
                themes_count[theme] = themes_count.get(theme, 0) + 1
        return themes_count
    
    def _get_sources_breakdown(self, articles: List[Dict[str, Any]]) -> Dict[str, int]:
        """Retorna contagem por fonte."""
        sources_count = {}
        for article in articles:
            source = article.get('source', 'Desconhecido')
            # Simplificar nome da fonte
            if 'g1.globo.com' in source:
                source = 'G1'
            elif 'folha' in source.lower():
                source = 'Folha de S.Paulo'
            elif 'estadao' in source.lower():
                source = 'Estadão'
            elif 'fiocruz' in source.lower():
                source = 'Fiocruz'
            elif 'news api' in source.lower():
                source = 'News API'
            
            sources_count[source] = sources_count.get(source, 0) + 1
        return sources_count
    
    async def analyze_news_with_gemini(
        self, 
        articles: List[Dict[str, Any]], 
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Realiza análise avançada das notícias usando Google Gemini.
        
        Args:
            articles: Lista de artigos de notícias
            metrics: Dict com métricas calculadas
            
        Returns:
            Dict com análise aprimorada via Gemini
        """
        execution_id = self.log_execution_start("analyze_news_with_gemini", {
            'articles_count': len(articles),
            'metrics_available': list(metrics.keys())
        })
        
        start_time = datetime.now()
        
        try:
            if not articles:
                return {
                    'gemini_analysis': 'Nenhuma notícia relevante encontrada para análise',
                    'articles': [],
                    'analysis_type': 'gemini',
                    'fallback': False
                }
            
            # Obter cliente Gemini
            gemini = get_gemini_client()
            
            # Gerar análise com Gemini
            gemini_analysis = await gemini.generate_news_analysis(articles, metrics)
            
            # Adicionar metadados úteis aos artigos (análise local)
            enriched_articles = []
            for article in articles:
                enriched = article.copy()
                
                # Identificar temas do artigo
                content = f"{article.get('title', '')} {article.get('summary', '')}".lower()
                themes = []
                
                if any(word in content for word in ['aumento', 'alta', 'crescimento', 'dispara']):
                    themes.append('Aumento de casos')
                if any(word in content for word in ['óbito', 'morte', 'mortalidade']):
                    themes.append('Mortalidade')
                if any(word in content for word in ['uti', 'internação', 'hospital']):
                    themes.append('Hospitalização')
                if any(word in content for word in ['vacina', 'vacinação', 'imunização']):
                    themes.append('Vacinação')
                if any(word in content for word in ['alerta', 'preocupa', 'risco']):
                    themes.append('Alerta')
                if any(word in content for word in ['prevenção', 'cuidados', 'proteção']):
                    themes.append('Prevenção')
                
                enriched['identified_themes'] = themes
                enriched_articles.append(enriched)
            
            analysis = {
                'gemini_analysis': gemini_analysis,
                'articles': enriched_articles,
                'analysis_type': 'gemini',
                'analysis_timestamp': datetime.now().isoformat(),
                'total_articles_analyzed': len(articles),
                'themes_breakdown': self._get_themes_breakdown(enriched_articles),
                'sources_breakdown': self._get_sources_breakdown(articles),
                'fallback': False
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self.log_execution_end(
                execution_id, 
                True, 
                execution_time,
                f"Análise Gemini concluída para {len(articles)} artigos"
            )
            
            return analysis
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.warning(f"Erro na análise com Gemini, usando fallback: {e}")
            
            self.log_execution_end(
                execution_id, 
                False, 
                execution_time,
                error=str(e)
            )
            
            # Fallback para análise tradicional
            return await self.analyze_news_context(articles, metrics)
    
    def health_check(self) -> Dict[str, Any]:
        """Verifica saúde da ferramenta de notícias."""
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
