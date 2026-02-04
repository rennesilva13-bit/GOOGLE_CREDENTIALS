#!/usr/bin/env python3
"""
Screener Fundamentalista BR - VERSÃO RESILIENTE
Funciona mesmo com bloqueios do Status Invest (usa yfinance como fonte primária)
"""
import os
import json
import time
import random
import sys
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import gspread


class ScreenerResilienteBR:
    """Coleta dados reais com fallback robusto contra bloqueios"""
    
    def __init__(self):
        # Lista validada de tickers que funcionam no yfinance (com sufixo .SA)
        self.tickers_validos = [
            'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'BBAS3.SA', 
            'ABEV3.SA', 'WEGE3.SA', 'TAEE11.SA', 'BBSE3.SA', 'HYPE3.SA',
            'RENT3.SA', 'LREN3.SA', 'CIEL3.SA', 'GGBR4.SA', 'EMBR3.SA',
            'VIIA3.SA', 'B3SA3.SA', 'SULA11.SA', 'UGPA3.SA', 'ENGI11.SA',
            'ENEV3.SA', 'EQTL3.SA', 'EGIE3.SA', 'YDUQ3.SA', 'NTCO3.SA', 'PCAR3.SA'
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://www.google.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
        self.rate_limit = float(os.getenv('FUNDAMENTUS_RATE_LIMIT', '4.0'))  # Aumentado para 4s
    
    def coletar_yfinance_completo(self, ticker: str) -> dict:
        """Coleta dados completos via yfinance (fonte primária confiável)"""
        try:
            acao = yf.Ticker(ticker)
            info = acao.info
            
            # Extrair métricas com fallbacks
            preco = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
            dy = info.get('dividendYield')
            if dy is None and info.get('trailingAnnualDividendYield'):
                dy = info.get('trailingAnnualDividendYield')
            dy = dy * 100 if dy else None
            
            dados = {
                'ticker': ticker.replace('.SA', ''),
                'preco': preco,
                'pl': info.get('trailingPE'),
                'pvp': info.get('priceToBook'),
                'dy': dy,
                'roe': info.get('returnOnEquity') * 100 if info.get('returnOnEquity') else None,
                'roic': info.get('returnOnAssets') * 100 if info.get('returnOnAssets') else None,  # ROIC aproximado
                'volume': info.get('averageVolume'),
                'market_cap': info.get('marketCap'),
                'dividend_rate': info.get('dividendRate'),
                'payout_ratio': info.get('payoutRatio') * 100 if info.get('payoutRatio') else None
            }
            
            # Calcular dívida líquida/EBITDA aproximado (não disponível diretamente no yfinance)
            # Fallback: usar debtToEquity se disponível
            if info.get('debtToEquity'):
                dados['div_liq_ebitda'] = info['debtToEquity'] / 100  # Conversão aproximada
            
            return dados
            
        except Exception as e:
            print(f"  ⚠️ yfinance {ticker}: {str(e)[:50]}")
            return {'ticker': ticker.replace('.SA', '')}
    
    def tentar_status_invest(self, ticker: str) -> dict:
        """Tenta coletar dados do Status Invest (opcional - não falha se bloqueado)"""
        try:
            ticker_sem_sa = ticker.replace('.SA', '').lower()
            url = f"https://statusinvest.com.br/acoes/{ticker_sem_sa}"
            
            # Apenas tentar se não estiver em ambiente GitHub Actions (evita desperdício de tempo)
            if 'GITHUB_ACTIONS' in os.environ:
                print(f"  🌐 {ticker_sem_sa:6}... ⏸️  Skip Status Invest (GitHub Actions)")
                return {}
            
            print(f"  🌐 {ticker_sem_sa:6}...", end=' ', flush=True)
            
            # Requisição com timeout curto (evita travar execução)
            response = requests.get(url, headers=self.headers, timeout=8)
            
            if response.status_code == 403:
                print("🔒 Bloqueado")
                return {}
            elif response.status_code != 200:
                print(f"⚠️ {response.status_code}")
                return {}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrair ROE (exemplo simplificado - adaptar conforme estrutura atual do site)
            roe = None
            roe_elem = soup.find('div', class_='item', string=lambda x: x and 'ROE' in x.upper())
            if roe_elem:
                valor_elem = roe_elem.find_next('strong', class_='value')
                if valor_elem:
                    try:
                        roe = float(valor_elem.text.strip().replace('%', '').replace(',', '.'))
                    except:
                        pass
            
            print(f"✅ ROE: {roe:.1f}%" if roe else "✅ Parcial")
            return {'roe': roe} if roe else {}
            
        except Exception as e:
            # Não falhar - Status Invest é opcional
            return {}
    
    def calcular_score(self, dados: dict) -> float:
        """Calcula score com dados parciais (não requer todas as métricas)"""
        score = 0.0
        
        # P/L (20 pontos) - dados do yfinance
        pl = dados.get('pl')
        if pl and 0 < pl <= 15:
            score += 20 * (1 - min(pl / 15, 1))
        elif pl is None:
            score += 10  # Bônus parcial por falta de dado (conservador)
        
        # P/VP (20 pontos)
        pvp = dados.get('pvp')
        if pvp and 0 < pvp <= 1.5:
            score += 20 * (1 - min(pvp / 1.5, 1))
        elif pvp is None:
            score += 10
        
        # DY (25 pontos)
        dy = dados.get('dy')
        if dy and dy >= 4.0:
            score += 25 * min(dy / 4.0, 2.0)
        elif dy is None:
            score += 12.5
        
        # ROE (25 pontos)
        roe = dados.get('roe')
        if roe and roe >= 12.0:
            score += 25 * min(roe / 12.0, 2.0)
        elif roe is None:
            score += 12.5
        
        # Dívida (10 pontos) - dados aproximados
        div = dados.get('div_liq_ebitda')
        if div is not None and div <= 3.0:
            score += 10 * (1 - min(div / 3.0, 1))
        elif div is None:
            score += 5  # Bônus parcial
        
        return min(score, 100.0)
    
    def classificar(self, score: float) -> str:
        if score >= 80:
            return 'EXCELENTE'
        elif score >= 60:
            return 'BOM'
        elif score >= 40:
            return 'ACEITÁVEL'
        else:
            return 'ESPECULATIVO'
    
    def rodar_screener(self) -> pd.DataFrame:
        print("="*70)
        print("🤖 SCREENER FUNDAMENTALISTA BR - VERSÃO RESILIENTE")
        print("="*70)
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌐 Fonte primária: yfinance (confiável em GitHub Actions)")
        print(f"🌐 Fonte secundária: Status Invest (opcional, fallback automático)")
        print(f"⏳ Rate limit: {self.rate_limit}s (proteção contra bloqueios)")
        print("="*70)
        print(f"\n🔍 Analisando {len(self.tickers_validos)} tickers...\n")
        
        resultados = []
        coletados = 0
        
        for i, ticker in enumerate(self.tickers_validos, 1):
            ticker_limpo = ticker.replace('.SA', '')
            print(f"[{i:2d}/{len(self.tickers_validos)}] {ticker_limpo:6}", end=' ')
            
            # Passo 1: Coletar dados PRIMÁRIOS do yfinance (sempre funciona)
            dados = self.coletar_yfinance_completo(ticker)
            
            # Passo 2: Tentar enriquecer com Status Invest (opcional - não falha)
            if random.random() > 0.7:  # Tentar apenas 30% das vezes para reduzir bloqueios
                time.sleep(random.uniform(1.5, 3.0))
                dados_status = self.tentar_status_invest(ticker)
                dados.update(dados_status)
            
            # Calcular score mesmo com dados parciais
            if dados.get('preco') is not None or dados.get('dy') is not None:
                dados['score_final'] = self.calcular_score(dados)
                dados['classificacao'] = self.classificar(dados['score_final'])
                resultados.append(dados)
                coletados += 1
            
            # Rate limiting robusto
            if i < len(self.tickers_validos):
                time.sleep(self.rate_limit + random.uniform(1.0, 2.5))
        
        print("\n" + "="*70)
        print(f"✅ Coletados com sucesso: {coletados}/{len(self.tickers_validos)} tickers")
        print(f"💡 Dica: Status Invest bloqueia IPs de datacenter. yfinance é fonte primária confiável.")
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
    
    def atualizar_sheets(self, df: pd.DataFrame) -> bool:
        """Atualiza Google Sheets com tratamento robusto de erros"""
        try:
            if not os.path.exists('credentials.json'):
                print("❌ ERRO: credentials.json não encontrado")
                return False
            
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = os.getenv('SPREADSHEET_ID')
            if not spreadsheet_id:
                print("❌ ERRO: SPREADSHEET_ID não configurado")
                return False
            
            try:
                sheet = client.open_by_key(spreadsheet_id).sheet1
                print(f"✅ Conectado à planilha: {sheet.title}")
            except gspread.exceptions.APIError as e:
                if "403" in str(e):
                    print("❌ ERRO 403: Permissão negada")
                    print("   → Verifique email da Service Account na planilha")
                    return False
                raise
            
            # Preparar dados
            headers = ['Data/Hora', 'Ticker', 'Score', 'Classificação', 'Preço (R$)', 'P/L', 'P/VP', 'DY%', 'ROE%', 'Volume']
            dados_linhas = []
            
            for _, row in df.iterrows():
                dados_linhas.append([
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    row['ticker'],
                    round(row.get('score_final', 0), 1),
                    row.get('classificacao', ''),
                    round(row.get('preco', 0), 2) if row.get('preco') else '',
                    round(row.get('pl', 0), 2) if row.get('pl') else '',
                    round(row.get('pvp', 0), 2) if row.get('pvp') else '',
                    round(row.get('dy', 0), 2) if row.get('dy') else '',
                    round(row.get('roe', 0), 2) if row.get('roe') else '',
                    f"{row.get('volume', 0):,.0f}" if row.get('volume') else ''
                ])
            
            # Atualizar
            sheet.clear()
            sheet.append_row(headers)
            sheet.append_rows(dados_linhas)
            
            print(f"✅ Google Sheets ATUALIZADA com {len(df)} ações!")
            top_acao = df.nlargest(1, 'score_final').iloc[0]
            print(f"📊 Melhor oportunidade: {top_acao['ticker']} | Score: {top_acao['score_final']:.1f} | DY: {top_acao.get('dy', 0):.1f}%")
            return True
            
        except Exception as e:
            print(f"❌ ERRO ao atualizar Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def salvar_resultados(self, df: pd.DataFrame):
        resultados = {
            'data_execucao': datetime.now().isoformat(),
            'total_analisadas': len(df),
            'aprovadas': len(df[df['score_final'] >= 60]) if not df.empty else 0,
            'acoes': df.to_dict('records')
        }
        
        with open('resultados.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        
        print(f"💾 resultados.json salvo com {len(df)} ações")


def main():
    screener = ScreenerResilienteBR()
    df = screener.rodar_screener()
    
    if df.empty:
        print("\n❌ FALHA: Nenhum dado coletado")
        print("   → Verifique conexão com yfinance")
        print("   → Lista de tickers válidos no Brasil")
        sys.exit(1)
    
    # Resumo
    print(f"\n📊 RESUMO DA EXECUÇÃO")
    print("="*70)
    print(f"Total analisadas: {len(df)}")
    print(f"Aprovadas (score ≥ 60): {len(df[df['score_final'] >= 60])}")
    
    # TOP 10
    print(f"\n🏆 TOP 10 OPORTUNIDADES:")
    print("-"*70)
    top10 = df.nlargest(10, 'score_final')
    for i, (_, row) in enumerate(top10.iterrows(), 1):
        print(f"{i:2d}. {row['ticker']:6} | "
              f"Score: {row['score_final']:5.1f} | "
              f"P/L: {row.get('pl', 'N/A'):5.1f} | "
              f"DY: {row.get('dy', 'N/A'):4.1f}% | "
              f"ROE: {row.get('roe', 'N/A'):5.1f}% | "
              f"{row['classificacao']}")
    
    # Salvar e atualizar
    screener.salvar_resultados(df)
    
    print("\n☁️  Atualizando Google Sheets...")
    if not screener.atualizar_sheets(df):
        print("⚠️  Planilha não atualizada, mas resultados salvos localmente")
    
    print("\n" + "="*70)
    print("✅ EXECUÇÃO CONCLUÍDA COM DADOS REAIS DO YAHOO FINANCE!")
    print("="*70)
    print("\n💡 Notas importantes:")
    print("   • yfinance é fonte confiável mesmo em GitHub Actions")
    print("   • Status Invest bloqueia IPs de datacenter (comportamento esperado)")
    print("   • Score calculado com dados parciais mantém utilidade da análise")
    print("   • Execute após 18h BRT para dados do fechamento do mercado")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Execução interrompida")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
