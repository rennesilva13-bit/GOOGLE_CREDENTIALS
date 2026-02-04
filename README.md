# 🔍 Screener Fundamentalista BR

Sistema automatizado de análise fundamentalista para o mercado brasileiro com **dados reais** coletados de múltiplas fontes.

## ✨ Funcionalidades

- 📊 Coleta automática de indicadores reais via **yfinance** (P/L, P/VP, DY, preço)
- 📈 Enriquecimento com **Status Invest** (ROE, ROIC, Dívida/EBITDA)
- ⚖️ Scoring system baseado em critérios de Benjamin Graham adaptados para Brasil
- ☁️ Execução diária automatizada via GitHub Actions (18h BRT)
- 📊 Atualização automática de Google Sheets com formatação condicional
- 🔔 Alertas para oportunidades com score ≥ 80

## 📈 Critérios de Investimento

| Métrica | Critério Ideal | Peso no Score |
|---------|----------------|---------------|
| **P/L** | ≤ 15x | 20 pontos |
| **P/VP** | ≤ 1.5x | 20 pontos |
| **DY** | ≥ 4% aa | 25 pontos |
| **ROE** | ≥ 12% | 25 pontos |
| **Dív. Líq/EBITDA** | ≤ 3x | 10 pontos |

## 🚀 Setup Rápido (5 minutos)

1. **Criar Service Account no Google Cloud**  
   [Tutorial detalhado](https://console.cloud.google.com/projectcreate)

2. **Configurar Google Sheets**  
   - Criar planilha em https://sheets.new
   - Compartilhar com email da Service Account como **Editor**

3. **Configurar Secrets no GitHub**  
   - `GOOGLE_CREDENTIALS`: Conteúdo minificado do credentials.json
   - `SPREADSHEET_ID`: ID da planilha (parte entre `/d/` e `/edit` na URL)

4. **Executar**  
   Actions → Screener Fundamentalista Diário → Run workflow

## ⚠️ Limitações Realistas

- **yfinance**: Dados atrasados ~15min após fechamento do mercado
- **Status Invest**: Pode bloquear após muitas requisições → rate limit de 3s aplicado
- **GitHub Actions**: IPs conhecidos podem ser bloqueados → fallback com yfinance

## 📜 Licença

MIT License - Uso educacional e pessoal permitido. Não constitui recomendação de investimento.
