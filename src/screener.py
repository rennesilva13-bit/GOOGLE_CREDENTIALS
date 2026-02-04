#!/usr/bin/env python3
"""
Screener Fundamentalista BR - DADOS REAIS DO MERCADO BRASILEIRO
Fontes: yfinance (preços/básicos) + Status Invest (ROE/ROIC/dívida)
"""
import os
import json
import time
import random
import sys
import re
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import gspread


class ScreenerRealBR:
    """Coleta dados reais do mercado brasileiro sem mockups"""
    
    def __init__(self):
        # Lista atualizada de tickers líquidos da B3
        self.tickers_br = [
            'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'WEGE3',
            'TAEE11', 'BBSE3', 'HYPE3', 'RENT3', 'LREN3', 'CIEL3', 'GGBR4',
            'EMBR3', 'VIIA3', 'B3SA3', 'SULA11', 'UGPA3', 'ENGI11', 'ENEV3',
            'EQTL3', 'EGIE3', 'YDUQ3', 'NTCO3', 'PCAR3'
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive"
        }
        self.rate_limit = float(os.getenv('FUNDAMENTUS_RATE_LIMIT', '3.0'))
    
    def coletar_yfinance(self, ticker: str) -> dict:
        """Coleta dados básicos via yfinance (P/L, DY, P/VP, preço)"""
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
            
            # Calcular ROE aproximado (P/VP / P/L) * 100
            if dados['pl'] and dados['pvp']:
                dados['roe_aprox'] = (dados['pvp'] / dados['pl']) * 100 if dados['pl'] != 0 else None
            
            return dados
            
        except Exception as e:
            print(f"  ⚠️ yfinance {ticker}: {str(e)[:40]}")
            return {'ticker': ticker}
    
    def coletar_status_invest(self, ticker: str) -> dict:
        """Coleta dados avançados via Status Invest (ROE, ROIC, Dívida/EBITDA)"""
        try:
            url = f"https://statusinvest.com.br/acoes/{ticker.lower()}"
            print(f"  📡 {ticker:6}...", end=' ', flush=True)
            
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # Verificar se ação existe
            if "not-found" in response.url or "Não encontramos" in response.text:
                print("❌ Não encontrado")
                return {}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extrair ROE
            roe = None
            roe_div = soup.find('div', class_='value', string=re.compile(r'ROE', re.IGNORECASE))
            if roe_div:
                valor_elem = roe_div.find_next_sibling('div', class_='value')
                if valor_elem:
                    roe_text = valor_elem.text.strip().replace('%', '').replace(',', '.')
                    try:
                        roe = float(roe_text)
                    except:
                        pass
            
            # Extrair ROIC
            roic = None
            roic_div = soup.find('div', class_='value', string=re.compile(r'ROIC', re.IGNORECASE))
            if roic_div:
                valor_elem = roic_div.find_next_sibling('div', class_='value')
                if valor_elem:
                    roic_text = valor_elem.text.strip().replace('%', '').replace(',', '.')
                    try:
                        roic = float(roic_text)
                    except:
                        pass
            
            # Extrair Dívida Líquida / EBITDA
            div_ebitda = None
            div_elem = soup.find(string=re.compile('Dív.Líq.EBITDA|Dív Líq EBITDA', re.IGNORECASE))
            if div_elem:
                pai = div_elem.find_parent('div', class_='item')
                if pai:
                    valor_elem = pai.find('strong', class_='value')
                    if valor_elem:
                        div_text = valor_elem.text.strip().replace('x', '').replace(',', '.')
                        try:
                            div_ebitda = float(div_text)
                        except:
                            pass
            
            print(f"✅ ROE: {roe:.1f}%" if roe else "✅ Dados coletados")
            return {
                'roe': roe,
                'roic': roic,
                'div_liq_ebitda': div_ebitda
            }
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Conexão: {str(e)[:30]}")
            return {}
        except Exception as e:
            print(f"❌ Erro: {str(e)[:30]}")
            return {}
    
    def calcular_score(self, dados: dict) -> float:
        """Calcula score real baseado em dados coletados (critérios Graham adaptados)"""
        score = 0.0
        
        # P/L (20 pontos) - quanto menor melhor (máx 15x)
        pl = dados.get('pl')
        if pl and 0 < pl <= 15:
            score += 20 * (1 - min(pl / 15, 1))
        
        # P/VP (20 pontos) - quanto menor melhor (máx 1.5x)
        pvp = dados.get('pvp')
        if pvp and 0 < pvp <= 1.5:
            score += 20 * (1 - min(pvp / 1.5, 1))
        
        # DY (25 pontos) - quanto maior melhor (mín 4%)
        dy = dados.get('dy')
        if dy and dy >= 4.0:
            score += 25 * min(dy / 4.0, 2.0)  # Bônus até 8%
        
        # ROE (25 pontos) - prioriza ROE real, fallback para aproximação
        roe = dados.get('roe') or dados.get('roe_aprox')
        if roe and roe >= 12.0:
            score += 25 * min(roe / 12.0, 2.0)
        
        # Dívida Líq/EBITDA (10 pontos) - quanto menor melhor (máx 3x)
        div = dados.get('div_liq_ebitda')
        if div is not None and div <= 3.0:
            score += 10 * (1 - min(div / 3.0, 1))
        
        return min(score, 100.0)
    
    def classificar(self, score: float) -> str:
        """Classificação baseada no score"""
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
        print("🤖 SCREENER FUNDAMENTALISTA BR - DADOS REAIS DO MERCADO")
        print("="*70)
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌐 Fontes: yfinance + Status Invest")
        print(f"⏳ Rate limit: {self.rate_limit}s entre requisições")
        print("="*70)
        print(f"\n🔍 Analisando {len(self.tickers_br)} tickers...\n")
        
        resultados = []
        coletados = 0
        
        for i, ticker in enumerate(self.tickers_br, 1):
            print(f"[{i:2d}/{len(self.tickers_br)}] {ticker:6}", end=' ')
            
            # Passo 1: Coletar dados básicos do yfinance
            dados = self.coletar_yfinance(ticker)
            
            # Passo 2: Enriquecer com Status Invest (dados avançados)
            time.sleep(random.uniform(1.0, 2.0))  # Jitter para evitar bloqueio
            dados_status = self.coletar_status_invest(ticker)
            dados.update(dados_status)
            
            # Calcular score apenas se tivermos dados mínimos significativos
            if dados.get('pl') is not None or dados.get('dy') is not None or dados.get('roe') is not None:
                dados['score_final'] = self.calcular_score(dados)
                dados['classificacao'] = self.classificar(dados['score_final'])
                resultados.append(dados)
                coletados += 1
            
            # Rate limiting realista para Status Invest
            if i < len(self.tickers_br):  # Não esperar após o último
                time.sleep(self.rate_limit + random.uniform(0.5, 1.5))
        
        print("\n" + "="*70)
        print(f"✅ Coletados com sucesso: {coletados}/{len(self.tickers_br)} tickers")
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
    
    def atualizar_sheets(self, df: pd.DataFrame) -> bool:
        """Atualiza Google Sheets com dados reais + tratamento rigoroso de erros"""
        try:
            if not os.path.exists('credentials.json'):
                print("❌ ERRO: credentials.json não encontrado")
                return False
            
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = os.getenv('SPREADSHEET_ID')
            if not spreadsheet_id:
                print("❌ ERRO: SPREADSHEET_ID não configurado nas variáveis de ambiente")
                return False
            
            # Testar acesso à planilha ANTES de limpar
            try:
                sheet = client.open_by_key(spreadsheet_id).sheet1
                print(f"✅ Conectado à planilha: {sheet.title}")
            except gspread.exceptions.APIError as e:
                if "403" in str(e):
                    print("❌ ERRO 403: PERMISSÃO NEGADA NO GOOGLE SHEETS")
                    print("   → Verifique se o email da Service Account está CORRETO na planilha:")
                    print("      client_email do credentials.json deve estar compartilhado como 'Editor'")
                    print("   → Email deve terminar com '.gserviceaccount.com' (NÃO truncado)")
                    return False
                elif "404" in str(e):
                    print(f"❌ ERRO 404: PLANILHA NÃO ENCONTRADA")
                    print(f"   → SPREADSHEET_ID incorreto: {spreadsheet_id}")
                    print("   → Correto: parte entre '/d/' e '/edit' na URL do Google Sheets")
                    return False
                else:
                    raise
            
            # Preparar dados para atualização
            headers = ['Data/Hora', 'Ticker', 'Score', 'Classificação', 'P/L', 'P/VP', 'DY%', 'ROE%', 'ROIC%', 'Dív/EBITDA', 'Preço (R$)']
            dados_linhas = []
            
            for _, row in df.iterrows():
                roe_exibir = row.get('roe') if row.get('roe') is not None else row.get('roe_aprox')
                dados_linhas.append([
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    row['ticker'],
                    round(row.get('score_final', 0), 1),
                    row.get('classificacao', ''),
                    round(row.get('pl', 0), 2) if pd.notna(row.get('pl')) else '',
                    round(row.get('pvp', 0), 2) if pd.notna(row.get('pvp')) else '',
                    round(row.get('dy', 0), 2) if pd.notna(row.get('dy')) else '',
                    round(roe_exibir, 2) if roe_exibir and pd.notna(roe_exibir) else '',
                    round(row.get('roic', 0), 2) if row.get('roic') and pd.notna(row.get('roic')) else '',
                    round(row.get('div_liq_ebitda', 0), 2) if row.get('div_liq_ebitda') and pd.notna(row.get('div_liq_ebitda')) else '',
                    round(row.get('preco', 0), 2) if row.get('preco') and pd.notna(row.get('preco')) else ''
                ])
            
            # Atualizar planilha
            sheet.clear()
            sheet.append_row(headers)
            sheet.append_rows(dados_linhas)
            
            print(f"✅ Google Sheets ATUALIZADA com {len(df)} ações reais!")
            print(f"📊 Melhor oportunidade: {df.iloc[0]['ticker']} | Score: {df.iloc[0]['score_final']:.1f}")
            return True
            
        except Exception as e:
            print(f"❌ ERRO CRÍTICO ao atualizar Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def salvar_resultados(self, df: pd.DataFrame):
        """Salva resultados reais em JSON para histórico"""
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
        roe_real = row.get('roe') or row.get('roe_aprox', 0)
        print(f"{i:2d}. {row['ticker']:6} | "
              f"Score: {row['score_final']:5.1f} | "
              f"P/L: {row.get('pl', 0):5.1f} | "
              f"DY: {row.get('dy', 0):4.1f}% | "
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
    print("\n💡 Insights baseados nos dados reais:")
    print("   • Score ≥ 80: Empresas com valuation atrativo + rentabilidade sólida")
    print("   • DY alto + ROE alto: Empresas gerando caixa e valor para acionistas")
    print("   • Dívida/EBITDA < 3x: Empresa com saúde financeira para crises")
    print("\n⚠️  Disclaimer: Dados coletados em tempo real. Sempre faça")
    print("   sua própria análise antes de investir.")
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
