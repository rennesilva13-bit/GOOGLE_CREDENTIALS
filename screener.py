#!/usr/bin/env python3
"""
Screener Fundamentalista Automatizado - Mercado Brasileiro
===========================================================
Coleta dados do Fundamentus, calcula scores e atualiza Google Sheets
"""

import os
import json
import time
import sys
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import gspread


class FundamentalistaBR:
    """Screener automatizado para análise fundamentalista do mercado brasileiro"""
    
    def __init__(self):
        self.base_url = "https://www.fundamentus.com.br/detalhes.php?papel={}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.rate_limit = float(os.getenv('FUNDAMENTUS_RATE_LIMIT', '2.5'))
        
        # Critérios de investimento (Benjamin Graham adaptado para Brasil)
        self.criterios = {
            'pl_max': 15.0,      # P/L máximo
            'pvp_max': 1.5,      # P/VP máximo
            'dy_min': 4.0,       # Dividend Yield mínimo (%)
            'roe_min': 12.0,     # ROE mínimo (%)
            'div_bruta_patrim_max': 0.8  # Dívida Bruta / Patrimônio Líquido máximo
        }
        
        # Lista atualizada de tickers líquidos da B3 (top 30 por volume)
        self.tickers = [
            'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3', 'ABEV3', 'WEGE3',
            'MGLU3', 'RAIZ4', 'TAEE11', 'BBSE3', 'HYPE3', 'RENT3', 'LREN3',
            'CIEL3', 'GGBR4', 'EMBR3', 'VIIA3', 'B3SA3', 'SULA11', 'UGPA3',
            'ENGI11', 'ENEV3', 'EQTL3', 'EGIE3', 'YDUQ3', 'NTCO3', 'PCAR3',
            'CPLE6', 'CSAN3'
        ]
    
    def buscar_dados_papel(self, ticker: str) -> Optional[Dict]:
        """
        Extrai dados fundamentalistas do Fundamentus com parsing robusto
        
        Returns:
            Dict com indicadores ou None se falhar
        """
        try:
            url = self.base_url.format(ticker)
            print(f"  📥 Buscando {ticker}...", end=' ', flush=True)
            
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # Detectar bloqueio (Fundamentus retorna 200 mesmo com bloqueio)
            if "temporariamente indisponível" in response.text.lower() or len(response.text) < 1000:
                print("❌ Bloqueado")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            dados = {'ticker': ticker}
            
            # Estratégia 1: Parsing de tabelas estruturadas
            tabelas = soup.find_all('table', class_='w728')
            if not tabelas:
                tabelas = soup.find_all('table')
            
            # Extrair dados de todas as tabelas
            for tabela in tabelas:
                linhas = tabela.find_all('tr')
                for linha in linhas:
                    cols = linha.find_all('td')
                    if len(cols) == 2:
                        chave_raw = cols[0].text.strip().replace(':', '').strip()
                        valor_raw = cols[1].text.strip()
                        
                        # Normalizar chave
                        chave = chave_raw.lower().replace(' ', '_').replace('.', '').replace('/', '_')
                        
                        # Processar valor
                        valor = self._converter_valor(valor_raw)
                        if valor is not None:
                            dados[chave] = valor
            
            # Mapeamento inteligente de campos (Fundamentus muda labels frequentemente)
            mapeamento = {
                'pl': ['p_l', 'pl', 'price_to_earnings', 'p/l'],
                'pvp': ['p_vp', 'pvp', 'price_to_book', 'p/vp'],
                'dy': ['dy', 'dividend_yield', 'div_yield', 'div._yield', 'dividendo'],
                'roe': ['roe', 'return_on_equity', 'ret_sobre_pl'],
                'roic': ['roic', 'return_on_invested_capital'],
                'div_brut_patrim': ['div_brut_patrim', 'div_bruta_patrim', 'div._brut_patrim.', 'divida_bruta_patrimonio'],
                'cresc_5_anos': ['cresc_5_anos', 'crescimento_5_anos', 'cagr_5_anos'],
                'liquidez_2_meses': ['liq_2meses', 'liquidez_2_meses', 'liq._2meses']
            }
            
            resultado = {
                'ticker': ticker,
                'pl': None,
                'pvp': None,
                'dy': None,
                'roe': None,
                'roic': None,
                'div_brut_patrim': None,
                'cresc_5_anos': None,
                'liquidez_2_meses': None
            }
            
            for campo_alvo, possibilidades in mapeamento.items():
                for chave in possibilidades:
                    if chave in dados:
                        resultado[campo_alvo] = dados[chave]
                        break
            
            # Calcular score final
            resultado['score_final'] = self._calcular_score(resultado)
            resultado['classificacao'] = self._classificar(resultado['score_final'])
            
            print(f"✅ Score: {resultado['score_final']:.0f} ({resultado['classificacao']})")
            return resultado
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de conexão: {str(e)[:40]}")
            return None
        except Exception as e:
            print(f"❌ Erro: {str(e)[:40]}")
            return None
    
    def _converter_valor(self, valor_str: str) -> Optional[float]:
        """Converte string de valor financeiro para float"""
        try:
            valor_limpo = valor_str.strip()
            
            # Caso vazio ou N/A
            if not valor_limpo or valor_limpo in ['-', 'N/A', '']:
                return None
            
            # Remover R$ e símbolos
            valor_limpo = valor_limpo.replace('R$', '').replace('.', '').replace(',', '.').replace('%', '').strip()
            
            # Converter para float
            return float(valor_limpo)
        except:
            return None
    
    def _calcular_score(self, dados: Dict) -> float:
        """Calcula score baseado nos critérios de Graham"""
        score = 0.0
        
        # P/L (20 pontos)
        if dados.get('pl') is not None and dados['pl'] <= self.criterios['pl_max']:
            score += 20 * (1 - min(dados['pl'] / self.criterios['pl_max'], 1))
        
        # P/VP (20 pontos)
        if dados.get('pvp') is not None and dados['pvp'] <= self.criterios['pvp_max']:
            score += 20 * (1 - min(dados['pvp'] / self.criterios['pvp_max'], 1))
        
        # DY (25 pontos)
        if dados.get('dy') is not None and dados['dy'] >= self.criterios['dy_min']:
            score += 25 * min(dados['dy'] / self.criterios['dy_min'], 2)  # Bônus até 2x
        
        # ROE (25 pontos)
        if dados.get('roe') is not None and dados['roe'] >= self.criterios['roe_min']:
            score += 25 * min(dados['roe'] / self.criterios['roe_min'], 2)
        
        # Dívida Bruta/PL (10 pontos)
        if dados.get('div_brut_patrim') is not None and dados['div_brut_patrim'] <= self.criterios['div_bruta_patrim_max']:
            score += 10 * (1 - min(dados['div_brut_patrim'] / self.criterios['div_bruta_patrim_max'], 1))
        
        return min(score, 100.0)
    
    def _classificar(self, score: float) -> str:
        """Classifica ação baseado no score"""
        if score >= 80:
            return 'EXCELENTE'
        elif score >= 60:
            return 'BOM'
        elif score >= 40:
            return 'ACEITÁVEL'
        else:
            return 'ESPECULATIVO'
    
    def rodar_screener(self) -> pd.DataFrame:
        """Executa screener em todos os tickers configurados"""
        print(f"\n🔍 Iniciando screener - {len(self.tickers)} tickers")
        print("=" * 70)
        
        resultados = []
        
        for i, ticker in enumerate(self.tickers, 1):
            print(f"[{i:2d}/{len(self.tickers)}] {ticker:6}", end=' ')
            dados = self.buscar_dados_papel(ticker)
            if dados:
                resultados.append(dados)
            time.sleep(self.rate_limit)  # Respeitar rate limit
        
        print("=" * 70)
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
    
    def atualizar_sheets(self, df: pd.DataFrame) -> bool:
        """Atualiza Google Sheets com resultados do screener"""
        try:
            # Verificar se credentials.json existe
            if not os.path.exists('credentials.json'):
                print("⚠️  credentials.json não encontrado. Salvando apenas localmente.")
                return False
            
            # Configurar autenticação
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(creds)
            
            # Obter spreadsheet ID
            spreadsheet_id = os.getenv('SPREADSHEET_ID')
            if not spreadsheet_id:
                print("⚠️  SPREADSHEET_ID não configurado nas variáveis de ambiente")
                return False
            
            # Acessar planilha
            sheet = client.open_by_key(spreadsheet_id).sheet1
            
            # Preparar dados
            headers = ['Data', 'Ticker', 'Score', 'Classificação', 'P/L', 'P/VP', 'DY%', 'ROE%', 'Dív/PL']
            dados_linhas = []
            
            for _, row in df.iterrows():
                dados_linhas.append([
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
            
            # Atualizar planilha
            sheet.clear()
            sheet.append_row(headers)
            sheet.append_rows(dados_linhas)
            
            print(f"✅ Google Sheets atualizada com {len(df)} ações")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao atualizar Sheets: {e}")
            return False
    
    def salvar_resultados(self, df: pd.DataFrame) -> Dict:
        """Salva resultados em JSON para histórico"""
        resultados = {
            'data_execucao': datetime.now().isoformat(),
            'total_analisadas': len(df),
            'aprovadas': len(df[df['score_final'] >= 60]) if not df.empty else 0,
            'acoes': df.to_dict('records') if not df.empty else []
        }
        
        with open('resultados.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Resultados salvos em resultados.json")
        return resultados


def main():
    """Função principal de execução"""
    print("=" * 70)
    print("🤖 SCREENER FUNDAMENTALISTA BR - MERCADO BRASILEIRO")
    print("=" * 70)
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 Fonte: Fundamentus (https://www.fundamentus.com.br)")
    print("=" * 70)
    
    # Inicializar screener
    screener = FundamentalistaBR()
    
    # Executar screener
    df_resultados = screener.rodar_screener()
    
    if df_resultados.empty:
        print("\n❌ Nenhum dado coletado. Verifique:")
        print("   • Conexão com internet")
        print("   • Bloqueio do Fundamentus (aumente FUNDAMENTUS_RATE_LIMIT)")
        print("   • Estrutura do site mudou (necessário atualizar parser)")
        sys.exit(1)
    
    # Exibir resumo
    print("\n📊 RESUMO DA EXECUÇÃO")
    print("=" * 70)
    print(f"Total analisadas: {len(df_resultados)}")
    print(f"Aprovadas (score ≥ 60): {len(df_resultados[df_resultados['score_final'] >= 60])}")
    
    # Top 10 oportunidades
    print(f"\n🏆 TOP 10 OPORTUNIDADES:")
    print("-" * 70)
    top10 = df_resultados.nlargest(10, 'score_final')
    for i, (_, row) in enumerate(top10.iterrows(), 1):
        print(f"{i:2d}. {row['ticker']:6} | "
              f"Score: {row['score_final']:5.1f} | "
              f"P/L: {row.get('pl', 'N/A'):5} | "
              f"DY: {row.get('dy', 'N/A'):4.1f}% | "
              f"ROE: {row.get('roe', 'N/A'):5.1f}% | "
              f"{row['classificacao']}")
    
    # Salvar resultados
    screener.salvar_resultados(df_resultados)
    
    # Atualizar Google Sheets (se configurado)
    if os.path.exists('credentials.json') and os.getenv('SPREADSHEET_ID'):
        print("\n☁️  Atualizando Google Sheets...")
        screener.atualizar_sheets(df_resultados)
    
    print("\n" + "=" * 70)
    print("✅ EXECUÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 70)
    print("\n💡 Dicas:")
    print("   • Score ≥ 80: Oportunidade EXCELENTE (comprar)")
    print("   • Score 60-79: BOM (monitorar)")
    print("   • Score < 60: Requer análise adicional")
    print("\n⚠️  Disclaimer: Este é um screener automatizado. Sempre faça")
    print("   sua própria análise antes de investir.")
    print("=" * 70)


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
