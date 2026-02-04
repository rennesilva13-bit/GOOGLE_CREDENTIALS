#!/usr/bin/env python3
"""
Screener de TESTE - Gera dados mockados para validar a integração com Google Sheets
"""
import os
import json
import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import gspread

def atualizar_sheets_mock():
    """Atualiza Google Sheets com dados mockados para teste"""
    try:
        # Verificar se credentials.json existe
        if not os.path.exists('credentials.json'):
            print("⚠️  credentials.json não encontrado")
            return False
        
        # Configurar autenticação
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        
        # Obter spreadsheet ID
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        if not spreadsheet_id:
            print("⚠️  SPREADSHEET_ID não configurado")
            return False
        
        # Acessar planilha
        sheet = client.open_by_key(spreadsheet_id).sheet1
        
        # Dados mockados (simulando análise real)
        headers = ['Data', 'Ticker', 'Score', 'Classificação', 'P/L', 'P/VP', 'DY%', 'ROE%', 'Dív/PL']
        dados_mock = [
            [datetime.now().strftime('%Y-%m-%d %H:%M'), 'PETR4', 85.0, 'EXCELENTE', 5.2, 0.9, 12.5, 18.2, 0.6],
            [datetime.now().strftime('%Y-%m-%d %H:%M'), 'VALE3', 78.0, 'BOM', 6.8, 1.1, 8.2, 15.6, 0.5],
            [datetime.now().strftime('%Y-%m-%d %H:%M'), 'ITUB4', 68.5, 'ACEITÁVEL', 8.1, 1.3, 6.1, 16.8, 1.2],
            [datetime.now().strftime('%Y-%m-%d %H:%M'), 'BBDC4', 65.2, 'ACEITÁVEL', 9.3, 1.2, 5.8, 14.2, 1.1],
            [datetime.now().strftime('%Y-%m-%d %H:%M'), 'TAEE11', 82.3, 'EXCELENTE', 16.2, 1.8, 7.5, 12.1, 2.8]
        ]
        
        # Atualizar planilha
        sheet.clear()
        sheet.append_row(headers)
        sheet.append_rows(dados_mock)
        
        print("="*70)
        print("✅ TESTE BEM-SUCEDIDO!")
        print("="*70)
        print("📊 Dados mockados atualizados na planilha Google Sheets:")
        for linha in dados_mock:
            print(f" • {linha[1]:6} | Score: {linha[2]:5.1f} | {linha[3]}")
        print("="*70)
        
        # Salvar resultados.json para evitar erro no upload
        resultados = {
            'data_execucao': datetime.now().isoformat(),
            'total_analisadas': 5,
            'aprovadas': 3,
            'acoes': [
                {'ticker': 'PETR4', 'score_final': 85.0, 'classificacao': 'EXCELENTE'},
                {'ticker': 'VALE3', 'score_final': 78.0, 'classificacao': 'BOM'},
                {'ticker': 'TAEE11', 'score_final': 82.3, 'classificacao': 'EXCELENTE'}
            ]
        }
        with open('resultados.json', 'w') as f:
            json.dump(resultados, f, indent=2)
        
        print("💾 Arquivo resultados.json gerado com sucesso")
        print("="*70)
        return True
        
    except Exception as e:
        print(f"❌ Erro ao atualizar Sheets: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*70)
    print("🧪 SCREENER DE TESTE (Dados Mockados)")
    print("="*70)
    print("💡 Este é um teste para validar a integração com Google Sheets")
    print("   Sem dependência do Fundamentus (evita bloqueios)")
    print("="*70)
    
    atualizar_sheets_mock()
