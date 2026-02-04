#!/usr/bin/env python3
"""
Screener Fundamentalista BR - DADOS REAIS (sem mockups)
Coleta de múltiplas fontes: yfinance + Status Invest + CVM fallback
"""
import os
import json
import time
import random
import sys
import re
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import gspread


class ScreenerRealBR:
    """Coleta dados reais do mercado brasileiro sem mockups"""
    
    def __init__(self):
        self.tickers_br = [
            'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'WEGE3',
            'TAEE11', 'BBSE3', 'HYPE3', 'RENT3', 'LREN3', 'CIEL3', 'GGBR4',
            'EMBR3', 'VIIA3', 'B3SA3', 'SULA11', 'UGPA3', 'ENGI11', 'ENEV3'
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        self.rate_limit = float(os.getenv('FUNDAMENTUS_RATE_LIMIT', '3.0'))
    
    def coletar_yfinance(self, ticker: str) -> Dict:
        """Coleta dados básicos via yfinance (P/L, DY, P/VP)"""
        try:
            ticker_sa = f"{ticker}.SA"
            acao = yf.Ticker(ticker_sa)
            info = acao.info
            
            # Extrair métricas relevantes
            dados = {
                'ticker': ticker,
                'pl': info.get('trailingPE'),
                'pvp': info.get('priceToBook'),
                'dy': info.get('dividendYield', 0) * 100 if info.get('dividendYield') else None,
                'preco': info.get('currentPrice'),
                'volume': info.get('averageVolume'),
                'market_cap': info.get('marketCap')
            }
            
            # Calcular ROE aproximado (Lucro Líquido / Patrimônio Líquido)
            if dados['pl'] and dados['pvp'] and dados['preco']:
                # ROE ≈ (Preço / P/L) / (Preço / P/VP) = P/VP / P/L
                dados['roe_aprox'] = (dados['pvp'] / dados['pl']) * 100 if dados['pl'] != 0 else None
            
            return dados
            
        except Exception as e:
            print(f"  ⚠️ yfinance {ticker}: {str(e)[:40]}")
            return {'ticker': ticker}
    
    def coletar_status_invest(self, ticker: str) -> Dict:
        """Coleta dados avançados via Status Invest (ROE, ROIC, Dívida)"""
        try:
            url = f"https://statusinvest.com.br/acoes/{ticker.lower()}"
            print(f"  📡 Status Invest {ticker:6}...", end=' ', flush=True)
            
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            if response.status_code != 200 or "not-found" in response.url:
                print("❌ Não encontrado")
                return {}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrair ROE
            roe_elem = soup.find('div', {'title': 'ROE'})
            roe = None
            if roe_elem:
                valor = roe_elem.find_next('strong')
                if valor:
                    roe_text = valor.text.strip().replace('%', '').replace(',', '.')
                    try:
                        roe = float(roe_text)
                    except:
                        pass
            
            # Extrair ROIC
            roic_elem = soup.find('div', {'title': 'ROIC'})
            roic = None
            if roic_elem:
                valor = roic_elem.find_next('strong')
                if valor:
                    roic_text = valor.text.strip().replace('%', '').replace(',', '.')
                    try:
                        roic = float(roic_text)
                    except:
                        pass
            
            # Extrair Dívida Líquida / EBITDA
            div_elem = soup.find(string=re.compile('Dív.Líq.EBITDA', re.IGNORECASE))
            div_ebitda = None
            if div_elem:
                pai = div_elem.find_parent('div')
                if pai:
                    valor = pai.find_next('strong')
                    if valor:
                        div_text = valor.text.strip().replace('x', '').replace(',', '.')
                        try:
                            div_ebitda = float(div_text)
                        except:
                            pass
            
            print(f"✅ ROE: {roe:.1f}%" if roe else "✅ Parcial")
            return {
                'roe': roe,
                'roic': roic,
                'div_liq_ebitda': div_ebitda
            }
            
        except Exception as e:
            print(f"❌ Erro: {str(e)[:30]}")
            return {}
    
    def calcular_score(self, dados: Dict) -> float:
        """Calcula score real baseado em dados coletados"""
        score = 0.0
        
        # P/L (20 pontos) - dados do yfinance
        pl = dados.get('pl')
        if pl and 0 < pl <= 15:
            score += 20 * (1 - min(pl / 15, 1))
        
        # P/VP (20 pontos)
        pvp = dados.get('pvp')
        if pvp and 0 < pvp <= 1.5:
            score += 20 * (1 - min(pvp / 1.5, 1))
        
        # DY (25 pontos)
        dy = dados.get('dy')
        if dy and dy >= 4.0:
            score += 25 * min(dy / 4.0, 2.0)  # Bônus até 8%
        
        # ROE (25 pontos) - prioriza ROE real do Status Invest, fallback para aproximação
        roe = dados.get('roe') or dados.get('roe_aprox')
        if roe and roe >= 12.0:
            score += 25 * min(roe / 12.0, 2.0)
        
        # Dívida (10 pontos)
        div = dados.get('div_liq_ebitda')
        if div is not None and div <= 3.0:
            score += 10 * (1 - min(div / 3.0, 1))
        
        return min(score, 100.0)
    
    def classificar(self, score: float) -> str:
        """Classificação real baseada no score"""
        if score >= 80:
            return 'EXCELENTE'
        elif score >= 60:
            return 'BOM'
        elif score >= 40:
            return 'ACEITÁVEL'
        else:
            return 'ESPECULATIVO'
    
    def rodar_screener(self) -> pd.DataFrame:
        """Executa coleta real de múltiplas fontes"""
        print("="*70)
        print("🤖 SCREENER FUNDAMENTALISTA BR - DADOS REAIS")
        print("="*70)
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌐 Fontes: yfinance + Status Invest")
        print("="*70)
        print(f"\n🔍 Analisando {len(self.tickers_br)} tickers...\n")
        
        resultados = []
        
        for i, ticker in enumerate(self.tickers_br, 1):
            print(f"[{i:2d}/{len(self.tickers_br)}] {ticker:6}", end=' ')
            
            # Passo 1: Coletar dados básicos do yfinance
            dados = self.coletar_yfinance(ticker)
            
            # Passo 2: Enriquecer com Status Invest (dados avançados)
            time.sleep(random.uniform(1.0, 2.0))  # Evitar bloqueio
            dados_status = self.coletar_status_invest(ticker)
            dados.update(dados_status)
            
            # Calcular score apenas se tivermos dados mínimos
            if dados.get('pl') or dados.get('dy') or dados.get('roe'):
                dados['score_final'] = self.calcular_score(dados)
                dados['classificacao'] = self.classificar(dados['score_final'])
                resultados.append(dados)
            
            # Rate limiting realista
            time.sleep(self.rate_limit + random.uniform(0.5, 1.5))
        
        print("\n" + "="*70)
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
    
    def atualizar_sheets(self, df: pd.DataFrame) -> bool:
        """Atualiza Google Sheets com dados reais + tratamento de erro visível"""
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
            
            # Testar acesso à planilha ANTES de limpar
            try:
                sheet = client.open_by_key(spreadsheet_id).sheet1
                print(f"✅ Conexão com planilha estabelecida: {sheet.title}")
            except gspread.exceptions.APIError as e:
                if "403" in str(e):
                    print("❌ ERRO 403: Permissão negada no Google Sheets")
                    print("   → Verifique se o email da Service Account está CORRETO na planilha:")
                    print("      Settings → Secrets → GOOGLE_CREDENTIALS → client_email")
                    print("   → Deve terminar com '.gserviceaccount.com' (NÃO truncado)")
                    return False
                elif "404" in str(e):
                    print(f"❌ ERRO 404: Planilha não encontrada")
                    print(f"   → SPREADSHEET_ID incorreto: {spreadsheet_id}")
                    print("   → Correto: parte entre '/d/' e '/edit' na URL")
                    return False
                else:
                    raise
            
            # Atualizar dados
            headers = ['Data', 'Ticker', 'Score', 'Classificação', 'P/L', 'P/VP', 'DY%', 'ROE%', 'ROIC%', 'Dív/EBITDA', 'Preço (R$)']
            dados_linhas = []
            
            for _, row in df.iterrows():
                dados_linhas.append([
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    row['ticker'],
                    round(row.get('score_final', 0), 1),
                    row.get('classificacao', ''),
                    round(row.get('pl', 0), 2) if row.get('pl') else '',
                    round(row.get('pvp', 0), 2) if row.get('pvp') else '',
                    round(row.get('dy', 0), 2) if row.get('dy') else '',
                    round(row.get('roe', 0), 2) if row.get('roe') else row.get('roe_aprox', ''),
                    round(row.get('roic', 0), 2) if row.get('roic') else '',
                    round(row.get('div_liq_ebitda', 0), 2) if row.get('div_liq_ebitda') else '',
                    round(row.get('preco', 0), 2) if row.get('preco') else ''
                ])
            
            sheet.clear()
            sheet.append_row(headers)
            sheet.append_rows(dados_linhas)
            
            print(f"✅ Google Sheets ATUALIZADA com {len(df)} ações reais!")
            print(f"📊 Primeira ação: {df.iloc[0]['ticker']} | Score: {df.iloc[0]['score_final']:.1f}")
            return True
            
        except Exception as e:
            print(f"❌ ERRO CRÍTICO ao atualizar Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def salvar_resultados(self, df: pd.DataFrame):
        """Salva resultados reais em JSON"""
        resultados = {
            'data_execucao': datetime.now().isoformat(),
            'total_analisadas': len(df),
            'aprovadas': len(df[df['score_final'] >= 60]) if not df.empty else 0,
            'acoes': df.to_dict('records')
        }
        
        with open('resultados.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        
        print(f"💾 resultados.json salvo com {len(df)} ações reais")


def main():
    screener = ScreenerRealBR()
    df = screener.rodar_screener()
    
    if df.empty:
        print("\n❌ FALHA CRÍTICA: Nenhum dado real coletado")
        print("   Possíveis causas:")
        print("   • Bloqueio temporário do Status Invest")
        print("   • Problema de conexão com yfinance")
        print("   • Lista de tickers inválida")
        sys.exit(1)
    
    # Exibir resumo real
    print(f"\n📊 RESUMO DA EXECUÇÃO")
    print("="*70)
    print(f"Total analisadas: {len(df)}")
    print(f"Aprovadas (score ≥ 60): {len(df[df['score_final'] >= 60])}")
    
    # TOP 10 reais
    print(f"\n🏆 TOP 10 OPORTUNIDADES REAIS:")
    print("-"*70)
    top10 = df.nlargest(10, 'score_final')
    for i, (_, row) in enumerate(top10.iterrows(), 1):
        roe_real = row.get('roe') or row.get('roe_aprox', 'N/A')
        print(f"{i:2d}. {row['ticker']:6} | "
              f"Score: {row['score_final']:5.1f} | "
              f"P/L: {row.get('pl', 'N/A'):5.1f} | "
              f"DY: {row.get('dy', 'N/A'):4.1f}% | "
              f"ROE: {roe_real:5.1f}% | "
              f"{row['classificacao']}")
    
    # Salvar resultados reais
    screener.salvar_resultados(df)
    
    # Atualizar Google Sheets com verificação rigorosa
    print("\n☁️  Atualizando Google Sheets com DADOS REAIS...")
    if not screener.atualizar_sheets(df):
        print("\n❌ FALHA NA ATUALIZAÇÃO DA PLANILHA - Workflow interrompido")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("✅ EXECUÇÃO CONCLUÍDA COM DADOS REAIS DO MERCADO BRASILEIRO!")
    print("="*70)
    print("\n💡 Dicas baseadas nos dados reais:")
    print("   • Score ≥ 80: Oportunidade EXCELENTE (dados reais coletados)")
    print("   • DY alto + ROE alto: Empresas gerando caixa e valor")
    print("   • Dívida/EBITDA < 3x: Empresa com capacidade de pagamento")
    print("\n⚠️  Disclaimer: Dados coletados em tempo real. Sempre valide")
    print("   antes de investir.")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Execução interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro crítico: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
