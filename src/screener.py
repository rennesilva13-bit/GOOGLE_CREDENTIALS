#!/usr/bin/env python3
"""
Screener Fundamentalista Automatizado - Mercado Brasileiro
"""
import os
import json
import time
import sys
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import gspread

class FundamentalistaBR:
    def __init__(self):
        self.base_url = "https://www.fundamentus.com.br/detalhes.php?papel={}"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.rate_limit = float(os.getenv('FUNDAMENTUS_RATE_LIMIT', '2.5'))
        self.tickers = ['PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'WEGE3', 'TAEE11', 'BBSE3', 'HYPE3']
    
    def buscar_dados_papel(self, ticker):
        try:
            url = self.base_url.format(ticker)
            print(f"  📥 {ticker:6}...", end=' ', flush=True)
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            if "temporariamente indisponível" in response.text.lower() or len(response.text) < 1000:
                print("❌ Bloqueado")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            dados = {'ticker': ticker}
            
            tabelas = soup.find_all('table')
            for tabela in tabelas[:2]:
                for linha in tabela.find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) == 2:
                        chave = cols[0].text.strip().replace(':', '').replace(' ', '_').replace('.', '').replace('/', '_').lower()
                        valor = cols[1].text.strip()
                        try:
                            if '%' in valor:
                                valor = float(valor.replace('%', '').replace('.', '').replace(',', '.'))
                            elif 'R$' in valor or '.' in valor:
                                valor = float(valor.replace('R$', '').replace('.', '', valor.count('.')-1).replace(',', '.'))
                            else:
                                valor = float(valor.replace('.', '').replace(',', '.'))
                            dados[chave] = valor
                        except:
                            pass
            
            resultado = {
                'ticker': ticker,
                'pl': dados.get('p_l') or dados.get('pl'),
                'pvp': dados.get('p_vp') or dados.get('pvp'),
                'dy': dados.get('dy') or dados.get('dividend_yield'),
                'roe': dados.get('roe'),
                'div_brut_patrim': dados.get('div_brut_patrim') or dados.get('div_bruta_patrim')
            }
            
            score = 0
            if resultado['pl'] and resultado['pl'] <= 15: score += 20
            if resultado['pvp'] and resultado['pvp'] <= 1.5: score += 20
            if resultado['dy'] and resultado['dy'] >= 4: score += 25
            if resultado['roe'] and resultado['roe'] >= 12: score += 25
            if resultado['div_brut_patrim'] and resultado['div_brut_patrim'] <= 0.8: score += 10
            
            resultado['score_final'] = score
            resultado['classificacao'] = 'EXCELENTE' if score >= 80 else 'BOM' if score >= 60 else 'ACEITÁVEL' if score >= 40 else 'ESPECULATIVO'
            
            print(f"✅ {score:.0f} ({resultado['classificacao']})")
            return resultado
            
        except Exception as e:
            print(f"❌ Erro")
            return None
    
    def rodar_screener(self):
        print(f"\n🔍 Analisando {len(self.tickers)} tickers...\n")
        resultados = []
        for ticker in self.tickers:
            dados = self.buscar_dados_papel(ticker)
            if dados:
                resultados.append(dados)
            time.sleep(self.rate_limit)
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
    
    def atualizar_sheets(self, df):
        try:
            if not os.path.exists('credentials.json'):
                print("⚠️  credentials.json não encontrado")
                return False
            
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(creds)
            
            spreadsheet_id = os.getenv('SPREADSHEET_ID')
            if not spreadsheet_id:
                print("⚠️  SPREADSHEET_ID não configurado")
                return False
            
            sheet = client.open_by_key(spreadsheet_id).sheet1
            headers = ['Data', 'Ticker', 'Score', 'Classificação', 'P/L', 'P/VP', 'DY%', 'ROE%', 'Dív/PL']
            sheet.clear()
            sheet.append_row(headers)
            
            for _, row in df.iterrows():
                sheet.append_row([
                    datetime.now().strftime('%Y-%m-%d %H:%M'),
                    row['ticker'],
                    round(row.get('score_final', 0), 1),
                    row.get('classificacao', ''),
                    row.get('pl', ''),
                    row.get('pvp', ''),
                    row.get('dy', ''),
                    row.get('roe', ''),
                    row.get('div_brut_patrim', '')
                ])
            
            print(f"✅ Google Sheets atualizada com {len(df)} ações")
            return True
        except Exception as e:
            print(f"❌ Erro Sheets: {e}")
            return False
    
    def salvar_resultados(self, df):
        resultados = {
            'data_execucao': datetime.now().isoformat(),
            'total_analisadas': len(df),
            'aprovadas': len(df[df['score_final'] >= 60]) if not df.empty else 0,
            'acoes': df.to_dict('records')
        }
        with open('resultados.json', 'w') as f:
            json.dump(resultados, f, indent=2)
        print(f"💾 Resultados salvos")

def main():
    print("="*70)
    print("🤖 SCREENER FUNDAMENTALISTA BR")
    print("="*70)
    
    screener = FundamentalistaBR()
    df = screener.rodar_screener()
    
    if df.empty:
        print("\n❌ Nenhum dado coletado")
        sys.exit(1)
    
    print("\n" + "="*70)
    print(f"📊 Total analisadas: {len(df)} | Aprovadas (≥60): {len(df[df['score_final'] >= 60])}")
    print("="*70)
    print("\n🏆 TOP 5:")
    for _, row in df.nlargest(5, 'score_final').iterrows():
        print(f" • {row['ticker']:6} | Score: {row['score_final']:5.1f} | P/L: {row.get('pl', 'N/A'):5} | DY: {row.get('dy', 'N/A'):4.1f}% | {row['classificacao']}")
    
    screener.salvar_resultados(df)
    
    if os.path.exists('credentials.json') and os.getenv('SPREADSHEET_ID'):
        print("\n☁️  Atualizando Google Sheets...")
        screener.atualizar_sheets(df)
    
    print("\n✅ Execução concluída!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrompido")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        sys.exit(1)
