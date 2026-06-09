# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta, time as dt_time
import re
import time
import os
import json
import base64
import traceback

def log(msg):
    agora = datetime.now().strftime('%H:%M:%S')
    print(f"[{agora}] {msg}")

# --- Configurações e Autenticação ---
def autenticar_e_criar_cliente():
    log("Iniciando autenticação no Google Cloud...")
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw:
        log("❌ ERRO: Variável 'GCP_SA_KEY_JSON' não encontrada ou vazia.")
        return None
    try:
        creds_json_str = base64.b64decode(creds_raw, validate=True).decode('utf-8')
    except:
        creds_json_str = creds_raw
    try:
        cliente = gspread.service_account_from_dict(json.loads(creds_json_str), scopes=['https://www.googleapis.com/auth/spreadsheets'])
        log("✅ Autenticação realizada com sucesso!")
        return cliente
    except Exception as e:
        log(f"❌ ERRO CRÍTICO na autenticação: {e}")
        return None

def enviar_webhook(mensagem_txt):
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    
    if not webhook_url:
        log("❌ ERRO WEBHOOK: URL não encontrada.")
        return False
    
    try:
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```\n{mensagem_txt}\n```" }
        }
        
        t0 = time.time()
        response = requests.post(webhook_url, json=payload, timeout=15)
        t1 = time.time()
        
        # O SeaTalk devolve um JSON. Se der erro de tamanho, o 'code' vem diferente de 0.
        try:
            resp_json = response.json()
            if response.status_code == 200 and resp_json.get('code', 0) == 0:
                log(f"✅ Bloco entregue no SeaTalk ({round(t1-t0, 2)}s).")
                return True
            else:
                log(f"❌ SeaTalk recusou o bloco internamente. Resposta: {resp_json}")
                return False
        except:
            if response.status_code == 200:
                return True
            return False
            
    except Exception as e:
        log(f"❌ ERRO WEBHOOK: {e}")
        return False

# --- Funções de Apoio ---
def minutos_para_hhmm(minutos):
    if minutos == -999: return "00:00"
    sinal = "-" if minutos < 0 else ""
    m = abs(minutos)
    return f"{sinal}{m // 60:02d}:{m % 60:02d}"

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', str(doca_str))
    return match.group(1) if match else "--"

def ler_aba_otimizada(planilha, nome_aba):
    log(f"⏳ Solicitando dados da aba '{nome_aba}'...")
    for tentativa in range(3):
        try:
            dados_brutos = planilha.worksheet(nome_aba).get_all_values()
            if not dados_brutos or len(dados_brutos) <= 1: return []
            
            cabecalho = dados_brutos[0]
            linhas_validas = [linha for linha in dados_brutos[1:] if len(linha) > 0 and str(linha[0]).strip() != ""]
            
            if not linhas_validas: return []
            log(f"✅ Aba '{nome_aba}': {len(linhas_validas)} linhas úteis.")
            return [cabecalho] + linhas_validas
        except Exception as e:
            time.sleep(3)
    return []

# --- Lógica Principal ---
def main():
    log("🚀 INICIANDO SCRIPT DE RELATÓRIO OPERACIONAL")
    agora_br = datetime.utcnow() - timedelta(hours=3)
    
    cliente = autenticar_e_criar_cliente()
    if not cliente: return

    SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
    try:
        planilha = cliente.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        log("❌ Não foi possível abrir a planilha.")
        return

    em_descarregando, em_doca, em_fila, em_chegada = [], [], [], []
    lts_processados_no_report = set()

    # --- ABA REPORT ---
    raw_report = ler_aba_otimizada(planilha, 'Report')
    if raw_report:
        colunas = [str(h).strip() for h in raw_report[0]]
        dados_corrigidos = [row + [None] * (len(colunas) - len(row)) for row in raw_report[1:]]
        df_rep = pd.DataFrame(dados_corrigidos, columns=colunas)
        
        C_TRIP, C_ETA, C_ORIGEM = 'LH Trip Nnumber', 'ETA Planejado', 'station_code'
        C_CHECKIN, C_ENTRADA = 'Checkin', 'Add to Queue Time'
        C_STATUS, C_DOCA, C_TO = 'Status', 'Doca', 'TO'

        for col in [C_CHECKIN, C_ENTRADA, C_ETA]:
            if col in df_rep.columns: df_rep[col] = pd.to_datetime(df_rep[col], dayfirst=True, errors='coerce')

        for _, row in df_rep.iterrows():
            status = str(row.get(C_STATUS, '')).strip().lower()
            if any(s in status for s in ['descarregando', 'doca', 'fila']) and 'finalizado' not in status:
                lt_atual = str(row.get(C_TRIP, '???')).strip()
                if lt_atual != '???': lts_processados_no_report.add(lt_atual)

                data_ref = row[C_CHECKIN] if pd.notna(row.get(C_CHECKIN)) else row.get(C_ENTRADA)
                doca = padronizar_doca(row.get(C_DOCA, '--'))
                val_to = str(row.get(C_TO, '--')).strip()
                origem = str(row.get(C_ORIGEM, '--')).strip()
                
                eta_val = row.get(C_ETA)
                eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                
                if 'fila' in status:
                    minutos = int((agora_br - row[C_CHECKIN]).total_seconds() / 60) if pd.notna(row.get(C_CHECKIN)) else -999
                else:
                    minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else 0 

                linha = f"{lt_atual:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {minutos_para_hhmm(minutos):^6} | {origem:^10}"
                if 'descarregando' in status: em_descarregando.append((minutos, linha))
                elif 'doca' in status: em_doca.append((minutos, linha))
                elif 'fila' in status: em_fila.append((minutos, linha))

    # --- ABA DEU CHEGADA ---
    raw_chegada_manual = ler_aba_otimizada(planilha, 'Deu chegada')
    if raw_chegada_manual:
        cols_manual = [str(h).strip() for h in raw_chegada_manual[0]]
        df_manual = pd.DataFrame([row + [None] * (len(cols_manual) - len(row)) for row in raw_chegada_manual[1:]], columns=cols_manual)
        
        col_lt_m = next((c for c in df_manual.columns if c.upper() == 'LT'), 'LT')
        col_origem_m = next((c for c in df_manual.columns if 'code' in c.lower()), 'code')
        col_tos_m = next((c for c in df_manual.columns if 'TOs' in c), 'TOs')
        col_eta_m = next((c for c in df_manual.columns if 'ETA' in c), 'ETA Planejado')
        col_chegada_m = next((c for c in df_manual.columns if 'Chegada' in c), 'Chegada')

        if col_chegada_m in df_manual.columns: df_manual[col_chegada_m] = pd.to_datetime(df_manual[col_chegada_m], dayfirst=True, errors='coerce')
        if col_eta_m in df_manual.columns: df_manual[col_eta_m] = pd.to_datetime(df_manual[col_eta_m], dayfirst=True, errors='coerce')

        for _, row in df_manual.iterrows():
            lt_val = str(row.get(col_lt_m, '')).strip()
            time_val = row.get(col_chegada_m)
            if lt_val and pd.notna(time_val) and (lt_val not in lts_processados_no_report):
                minutos = int((agora_br - time_val).total_seconds() / 60)
                if minutos >= 10:
                    eta_val = row.get(col_eta_m)
                    eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                    linha = f"{lt_val:^13} | --   | {str(row.get(col_tos_m, '--')).strip():^7} | {eta_s:^11} | {minutos_para_hhmm(minutos):^6} | {str(row.get(col_origem_m, '--')).strip():^10}"
                    em_chegada.append((minutos, linha))

    # --- ABA PENDENTE ---
    raw_pendente = ler_aba_otimizada(planilha, 'Pendente') 
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    
    op_date_hoje = agora_br.date() - timedelta(days=1) if agora_br.time() < dt_time(6, 0) else agora_br.date()
    op_date_amanha = op_date_hoje + timedelta(days=1)
    
    turno_atual_str = "T1" if dt_time(6, 0) <= agora_br.time() < dt_time(14, 0) else ("T2" if dt_time(14, 0) <= agora_br.time() < dt_time(22, 0) else "T3")
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}

    if raw_pendente:
        col_pen = [str(h).strip() for h in raw_pendente[0]]
        df_pen = pd.DataFrame([row + [None] * (len(col_pen) - len(row)) for row in raw_pendente[1:]], columns=col_pen)
        
        c_saida = next((c for c in df_pen.columns if 'descarregado' in c.lower()), None)
        c_pct = next((c for c in df_pen.columns if 'acote' in c.lower()), 'Pacotes')
        c_to = next((c for c in df_pen.columns if c.upper() == 'TO'), 'TO')
        c_data = next((c for c in df_pen.columns if 'cutoff' in c.lower() or 'data' in c.lower() and 'descarregado' not in c.lower()), 'Data')
        
        df_pen[c_pct] = pd.to_numeric(df_pen[c_pct], errors='coerce').fillna(0).astype(int)
        df_pen[c_to] = pd.to_numeric(df_pen[c_to], errors='coerce').fillna(0).astype(int)
        df_pen[c_data] = pd.to_datetime(df_pen[c_data], dayfirst=True, errors='coerce')
        
        for _, row in df_pen.iterrows():
            if pd.isna(row[c_data]): continue 
            if c_saida and str(row.get(c_saida, '')).strip().lower() not in ['nan', 'none', '', '-', '--']: continue 

            t = str(row.get('Turno', 'Indef')).strip().upper()
            d_alvo = (row[c_data] - timedelta(hours=6)).date()
            
            cat = None
            if d_alvo < op_date_hoje: cat = 'atrasado'
            elif d_alvo == op_date_hoje: cat = 'atrasado' if mapa_turnos.get(t, 99) < mapa_turnos.get(turno_atual_str, 0) else 'hoje'
            elif d_alvo == op_date_amanha: cat = 'amanha'
            
            if cat == 'atrasado' and row[c_pct] == 0: cat = None
            
            if cat:
                if t not in resumo[cat]: resumo[cat][t] = {'lts': 0, 'pacotes': 0, 'tos': 0}
                resumo[cat][t]['lts'] += 1
                resumo[cat][t]['pacotes'] += row[c_pct]
                resumo[cat][t]['tos'] += row[c_to]

    # --- MONTAGEM DINÂMICA (FATIADA) ---
    log("--- INICIANDO CONSTRUÇÃO E ENVIO EM BLOCOS ---")
    
    todas_linhas_patio = ["Segue as LH´s com mais tempo de Pátio:\n"]
    header = f"{'LT':^13} | {'Doca':^4} | {'TO':^7} | {'ETA':^11} | {'Tempo':^6} | {'Origem':^10}"
    
    for nome, emoji, lista in [("Descarregando", "📦", em_descarregando), ("Em Doca", "🚛", em_doca), ("Em Fila", "🔴", em_fila), ("Deu Chegada (Cobrar Monitoring)", "📢", em_chegada)]:
        if lista:
            lista.sort(key=lambda x: x[0], reverse=True)
            todas_linhas_patio.append(f"\n{emoji} {nome}: {len(lista)} LT(s)\n{header}")
            todas_linhas_patio.extend([x[1] for x in lista])

    # Fatiador Inteligente para o Pátio (Limite Seguro ~3000 chars por bloco)
    bloco_atual = ""
    for linha in todas_linhas_patio:
        if len(bloco_atual) + len(linha) > 3000:
            enviar_webhook(bloco_atual)
            time.sleep(1) # Pausa pro bot respirar e não dar spam block
            bloco_atual = linha + "\n"
        else:
            bloco_atual += linha + "\n"
    
    if bloco_atual.strip(): # Manda o que sobrou do pátio
        enviar_webhook(bloco_atual)
        time.sleep(1)

    # Monta e manda o Resumo Final separado
    bloco_resumo = "\n" + ("-" * 72) + "\n\n"
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {op_date_amanha.strftime("%d/%m")}'}
    tem_resumo = False
    
    for cat in ['atrasado', 'hoje', 'amanha']:
        if not resumo[cat]: continue
        tem_resumo = True
        total_lts = sum(d['lts'] for d in resumo[cat].values())
        total_pct = sum(d['pacotes'] for d in resumo[cat].values())
        total_tos = sum(d['tos'] for d in resumo[cat].values())
        bloco_resumo += f"{titulos[cat]}: {total_lts} LTs ({total_pct} pcts | {total_tos} TO)\n"
        for t in sorted(resumo[cat].keys()):
            r = resumo[cat][t]
            bloco_resumo += f"    - {t}: {r['lts']} LTs ({r['pacotes']} pcts | {r['tos']} TO)\n"
        bloco_resumo += "\n"

    if tem_resumo:
        enviar_webhook(bloco_resumo)
    
    log("🏁 SCRIPT FINALIZADO.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log("❌ ERRO FATAL:")
        log(traceback.format_exc())
