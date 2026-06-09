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
    """Função auxiliar para padronizar os logs com horário."""
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
    log("Preparando envio de payload para o Webhook do SeaTalk...")
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    
    if not webhook_url:
        log("❌ ERRO WEBHOOK: A URL do webhook não foi encontrada na variável 'SEATALK_WEBHOOK_URL'.")
        return False
    
    try:
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```\n{mensagem_txt}\n```" }
        }
        
        t0 = time.time()
        response = requests.post(webhook_url, json=payload, timeout=15) # Adicionado timeout para evitar travamento infinito
        t1 = time.time()
        
        log(f"Resposta do Webhook recebida em {round(t1-t0, 2)} segundos.")
        
        if response.status_code == 200:
            log("✅ Mensagem entregue no SeaTalk com sucesso!")
            return True
        else:
            log(f"❌ ERRO WEBHOOK: Falha ao enviar. Status Code: {response.status_code}")
            log(f"Detalhe do erro reportado pela API: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        log("❌ ERRO WEBHOOK: Timeout. O SeaTalk demorou mais de 15 segundos para responder.")
        return False
    except Exception as e:
        log(f"❌ ERRO WEBHOOK (Exceção de código): {e}")
        log(traceback.format_exc())
        return False

# --- Funções de Apoio ---
def minutos_para_hhmm(minutos):
    if minutos == -999:
        return "00:00"
    sinal = "-" if minutos < 0 else ""
    m = abs(minutos)
    return f"{sinal}{m // 60:02d}:{m % 60:02d}"

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', str(doca_str))
    return match.group(1) if match else "--"

def ler_aba_otimizada(planilha, nome_aba):
    """Lê a aba inteira mas filtra linhas onde a Coluna A está vazia."""
    log(f"⏳ Solicitando dados da aba '{nome_aba}' para a API do Google...")
    for tentativa in range(3):
        try:
            t0 = time.time()
            # get_all_values puxa apenas o range que realmente contém dados, evitando 413 (Response too large)
            dados_brutos = planilha.worksheet(nome_aba).get_all_values()
            t1 = time.time()
            
            if not dados_brutos or len(dados_brutos) <= 1:
                log(f"⚠️ Aba '{nome_aba}' está vazia ou contém apenas o cabeçalho. Tempo: {round(t1-t0, 2)}s.")
                return []
            
            cabecalho = dados_brutos[0]
            
            # FILTRO: Mantém apenas linhas onde a primeira coluna (index 0) não é vazia
            linhas_validas = [linha for linha in dados_brutos[1:] if len(linha) > 0 and str(linha[0]).strip() != ""]
            
            if not linhas_validas:
                log(f"⚠️ Aba '{nome_aba}' ignorada: Nenhuma linha possui dado na Coluna A. Tempo: {round(t1-t0, 2)}s.")
                return []

            log(f"✅ Aba '{nome_aba}' lida e filtrada! {len(linhas_validas)} linhas úteis encontradas. Tempo de rede: {round(t1-t0, 2)}s.")
            return [cabecalho] + linhas_validas
            
        except Exception as e:
            log(f"❌ Erro ao ler '{nome_aba}' (Tentativa {tentativa+1}/3): {e}")
            time.sleep(3)
    
    log(f"❌ Falha definitiva ao ler a aba '{nome_aba}' após 3 tentativas.")
    return []

# --- Lógica Principal ---
def main():
    log("🚀 INICIANDO SCRIPT DE RELATÓRIO OPERACIONAL")
    agora_br = datetime.utcnow() - timedelta(hours=3) # Ajuste fuso Brasília
    log(f"Data/Hora Base Operacional: {agora_br.strftime('%d/%m/%Y %H:%M:%S')}")
    
    cliente = autenticar_e_criar_cliente()
    if not cliente: 
        return

    SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
    
    try:
        log("Tentando abrir a planilha principal...")
        planilha = cliente.open_by_key(SPREADSHEET_ID)
        log(f"✅ Planilha '{planilha.title}' aberta com sucesso.")
    except Exception as e:
        log(f"❌ Não foi possível abrir a planilha. Verifique o ID ou permissões.")
        log(traceback.format_exc())
        return

    em_descarregando, em_doca, em_fila, em_chegada = [], [], [], []
    lts_processados_no_report = set()

    # --- PARTE 1: Processar o PÁTIO (Aba 'Report') ---
    log("--- INICIANDO PROCESSAMENTO: ABA REPORT ---")
    raw_report = ler_aba_otimizada(planilha, 'Report')
    if raw_report:
        log("Montando DataFrame do Pandas para 'Report'...")
        colunas = [str(h).strip() for h in raw_report[0]]
        dados_corrigidos = [row + [None] * (len(colunas) - len(row)) for row in raw_report[1:]]
        df_rep = pd.DataFrame(dados_corrigidos, columns=colunas)
        
        log(f"DataFrame 'Report' montado. Formato: {df_rep.shape[0]} linhas x {df_rep.shape[1]} colunas.")
        
        C_TRIP    = 'LH Trip Nnumber' 
        C_ETA     = 'ETA Planejado'
        C_ORIGEM  = 'station_code'
        C_CHECKIN = 'Checkin'
        C_ENTRADA = 'Add to Queue Time'
        C_STATUS  = 'Status'
        C_DOCA    = 'Doca'
        C_TO      = 'TO'

        log("Convertendo campos de data na aba 'Report'...")
        for col in [C_CHECKIN, C_ENTRADA, C_ETA]:
            if col in df_rep.columns:
                df_rep[col] = pd.to_datetime(df_rep[col], dayfirst=True, errors='coerce')

        log("Iterando sobre as linhas do 'Report'...")
        for index, row in df_rep.iterrows():
            try:
                status = str(row.get(C_STATUS, '')).strip().lower()
                termos_interesse = ['descarregando', 'doca', 'fila']
                
                if any(s in status for s in termos_interesse) and 'finalizado' not in status:
                    lt_atual = str(row.get(C_TRIP, '???')).strip()
                    if lt_atual and lt_atual != '???':
                        lts_processados_no_report.add(lt_atual)

                    data_ref = row[C_CHECKIN] if pd.notna(row.get(C_CHECKIN)) else row.get(C_ENTRADA)
                    doca = padronizar_doca(row.get(C_DOCA, '--'))
                    val_to = str(row.get(C_TO, '--')).strip()
                    origem = str(row.get(C_ORIGEM, '--')).strip()
                    
                    eta_val = row.get(C_ETA)
                    eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                    
                    if 'fila' in status:
                        if pd.isna(row.get(C_CHECKIN)):
                            minutos = -999
                        else:
                            minutos = int((agora_br - row[C_CHECKIN]).total_seconds() / 60)
                    else:
                        if pd.notna(data_ref):
                            minutos = int((agora_br - data_ref).total_seconds() / 60)
                        else:
                            minutos = 0 

                    tempo = minutos_para_hhmm(minutos)
                    linha = f"{lt_atual:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {tempo:^6} | {origem:^10}"
                    
                    if 'descarregando' in status: em_descarregando.append((minutos, linha))
                    elif 'doca' in status: em_doca.append((minutos, linha))
                    elif 'fila' in status: em_fila.append((minutos, linha))
            except Exception as e_row:
                log(f"⚠️ Erro ao processar a linha {index} da aba Report: {e_row}")
        
        log(f"Fim do processamento 'Report'. LTs encontrados: {len(em_descarregando)} descarregando, {len(em_doca)} doca, {len(em_fila)} fila.")

    # --- PARTE 2: Processar 'Deu chegada' ---
    log("--- INICIANDO PROCESSAMENTO: ABA DEU CHEGADA ---")
    raw_chegada_manual = ler_aba_otimizada(planilha, 'Deu chegada')
    if raw_chegada_manual:
        log("Montando DataFrame 'Deu Chegada'...")
        cols_manual = [str(h).strip() for h in raw_chegada_manual[0]]
        dados_corrigidos_manual = [row + [None] * (len(cols_manual) - len(row)) for row in raw_chegada_manual[1:]]
        df_manual = pd.DataFrame(dados_corrigidos_manual, columns=cols_manual)
        
        col_lt_m = next((c for c in df_manual.columns if c.upper() == 'LT'), 'LT')
        col_origem_m = next((c for c in df_manual.columns if 'code' in c.lower()), 'code')
        col_tos_m = next((c for c in df_manual.columns if 'TOs' in c), 'TOs')
        col_eta_m = next((c for c in df_manual.columns if 'ETA' in c), 'ETA Planejado')
        col_chegada_m = next((c for c in df_manual.columns if 'Chegada' in c), 'Chegada')

        if col_chegada_m in df_manual.columns:
            df_manual[col_chegada_m] = pd.to_datetime(df_manual[col_chegada_m], dayfirst=True, errors='coerce')
        if col_eta_m in df_manual.columns:
            df_manual[col_eta_m] = pd.to_datetime(df_manual[col_eta_m], dayfirst=True, errors='coerce')

        log("Avaliando os LTs que já deram chegada...")
        for index, row in df_manual.iterrows():
            lt_val = str(row.get(col_lt_m, '')).strip()
            time_val = row.get(col_chegada_m)
            
            if lt_val and pd.notna(time_val) and (lt_val not in lts_processados_no_report):
                minutos = int((agora_br - time_val).total_seconds() / 60)
                if minutos >= 10:
                    doca = "--"
                    val_to = str(row.get(col_tos_m, '--')).strip()
                    origem = str(row.get(col_origem_m, '--')).strip()
                    eta_val = row.get(col_eta_m)
                    eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                    tempo = minutos_para_hhmm(minutos)
                    linha = f"{lt_val:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {tempo:^6} | {origem:^10}"
                    em_chegada.append((minutos, linha))
        
        log(f"Fim do processamento 'Deu Chegada'. {len(em_chegada)} LTs pendentes a cobrar.")

    # --- PARTE 3: Processar o RESUMO (Aba 'Pendente') ---
    log("--- INICIANDO PROCESSAMENTO: ABA PENDENTE ---")
    raw_pendente = ler_aba_otimizada(planilha, 'Pendente') 
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    
    # Corte Operacional 06:00
    if agora_br.time() < dt_time(6, 0):
        op_date_hoje = agora_br.date() - timedelta(days=1)
    else:
        op_date_hoje = agora_br.date()
    op_date_amanha = op_date_hoje + timedelta(days=1)
    
    hora_atual = agora_br.time()
    turno_atual_str = "T3"
    if dt_time(6, 0) <= hora_atual < dt_time(14, 0): turno_atual_str = "T1"
    elif dt_time(14, 0) <= hora_atual < dt_time(22, 0): turno_atual_str = "T2"
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}

    if raw_pendente:
        log("Montando DataFrame 'Pendente'...")
        colunas_pen = [str(h).strip() for h in raw_pendente[0]]
        dados_corrigidos_pen = [row + [None] * (len(colunas_pen) - len(row)) for row in raw_pendente[1:]]
        df_pen = pd.DataFrame(dados_corrigidos_pen, columns=colunas_pen)
        
        col_saida = next((c for c in df_pen.columns if 'descarregado' in c.lower()), None)
        col_pacotes = next((c for c in df_pen.columns if 'acote' in c.lower()), 'Pacotes')
        col_to = next((c for c in df_pen.columns if c.upper() == 'TO'), 'TO')
        col_data = next((c for c in df_pen.columns if 'cutoff' in c.lower() or 'data' in c.lower() and 'descarregado' not in c.lower()), 'Data')
        
        log("Ajustando tipos numéricos e de data em 'Pendente'...")
        df_pen[col_pacotes] = pd.to_numeric(df_pen[col_pacotes], errors='coerce').fillna(0).astype(int)
        df_pen[col_to] = pd.to_numeric(df_pen[col_to], errors='coerce').fillna(0).astype(int)
        df_pen[col_data] = pd.to_datetime(df_pen[col_data], dayfirst=True, errors='coerce')
        
        log("Calculando sumarização por turno...")
        for index, row in df_pen.iterrows():
            if pd.isna(row[col_data]): continue 
            
            if col_saida:
                val_saida = str(row.get(col_saida, '')).strip()
                if val_saida and val_saida.lower() not in ['nan', 'none', '', '-', '--']: continue 

            t = str(row.get('Turno', 'Indef')).strip().upper()
            pct = row[col_pacotes]
            val_to_row = row[col_to]
            
            data_viagem = row[col_data]
            d_alvo = (data_viagem - timedelta(hours=6)).date() # Subtração para corte das 06h
            
            categoria = None
            if d_alvo < op_date_hoje: categoria = 'atrasado'
            elif d_alvo == op_date_hoje:
                eh_turno_passado = mapa_turnos.get(t, 99) < mapa_turnos.get(turno_atual_str, 0)
                categoria = 'atrasado' if eh_turno_passado else 'hoje'
            elif d_alvo == op_date_amanha: categoria = 'amanha'
            
            if categoria == 'atrasado' and pct == 0: categoria = None
            
            if categoria:
                if t not in resumo[categoria]: resumo[categoria][t] = {'lts': 0, 'pacotes': 0, 'tos': 0}
                resumo[categoria][t]['lts'] += 1
                resumo[categoria][t]['pacotes'] += pct
                resumo[categoria][t]['tos'] += val_to_row
        log("Resumo operacional calculado com sucesso.")

    # --- MONTAGEM E ENVIO ---
    log("--- INICIANDO CONSTRUÇÃO DA MENSAGEM FINAL ---")
    for lista in [em_descarregando, em_doca, em_fila, em_chegada]:
        lista.sort(key=lambda x: x[0], reverse=True)
    
    header = f"{'LT':^13} | {'Doca':^4} | {'TO':^7} | {'ETA':^11} | {'Tempo':^6} | {'Origem':^10}"
    bloco_patio = ["Segue as LH´s com mais tempo de Pátio:\n"]
    if em_descarregando:
        bloco_patio.append(f"📦 Descarregando: {len(em_descarregando)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_descarregando])
    if em_doca:
        bloco_patio.append(f"\n🚛 Em Doca: {len(em_doca)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_doca])
    if em_fila:
        bloco_patio.append(f"\n🔴 Em Fila: {len(em_fila)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_fila])
    if em_chegada:
        bloco_patio.append(f"\n📢 Deu Chegada (Cobrar Monitoring): {len(em_chegada)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_chegada])

    bloco_resumo = []
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {op_date_amanha.strftime("%d/%m")}'}
    for cat in ['atrasado', 'hoje', 'amanha']:
        if not resumo[cat]: continue
        total_lts = sum(d['lts'] for d in resumo[cat].values())
        total_pct = sum(d['pacotes'] for d in resumo[cat].values())
        total_tos = sum(d['tos'] for d in resumo[cat].values())
        bloco_resumo.append(f"{titulos[cat]}: {total_lts} LTs ({total_pct} pcts | {total_tos} TO)")
        for t in sorted(resumo[cat].keys()):
            r = resumo[cat][t]
            bloco_resumo.append(f"    - {t}: {r['lts']} LTs ({r['pacotes']} pcts | {r['tos']} TO)")
        bloco_resumo.append("") 

    txt_completo = "\n".join(bloco_patio) + "\n" + ("-" * 72) + "\n\n" + "\n".join(bloco_resumo)
    log(f"Tamanho da mensagem final montada: {len(txt_completo)} caracteres.")
    
    log("Disparando envio principal para o Webhook...")
    if not enviar_webhook(txt_completo):
        log("⚠️ Envio completo falhou. Tentando quebrar a mensagem em blocos menores (Pátio e depois Resumo)...")
        enviar_webhook("\n".join(bloco_patio))
        time.sleep(1)
        if bloco_resumo: 
            enviar_webhook("\n".join(bloco_resumo))
    
    log("🏁 SCRIPT FINALIZADO.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log("❌ ERRO FATAL NÃO TRATADO NO SCRIPT:")
        log(traceback.format_exc())
