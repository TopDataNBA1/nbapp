import os
import pandas as pd
import time
import shutil
import subprocess # <-- Necesario para ejecutar comandos de Git
import json
import requests
import pytz
import re
from datetime import datetime, date, timedelta
from PIL import Image, ImageDraw, ImageFont
from collections import defaultdict
import sys
import threading
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.live.nba.endpoints import boxscore  # NUEVO
from nba_api.stats.endpoints import leaguegamefinder
import csv  # ← AÑADIDO PARA LOG
from datetime import datetime  # ← YA ESTABA, PERO OK
# === NUEVO: IMPORT PARA API EN VIVO ===
from nba_api.live.nba.endpoints import boxscore as live_boxscore
import time
from dateutil import parser
import pytz

import atexit
# Firebase eliminado - ahora usamos solo OneSignal

# ============================================================
# CONFIGURACIÓN DE PUSH CONDICIONAL
# ============================================================
# True = Solo hace push cuando hay cambios reales (notificaciones nuevas o datos diferentes)
# False = Push cada ciclo (comportamiento anterior)
PUSH_SOLO_SI_HAY_CAMBIOS = True
# Variable para guardar el hash del último data.json enviado
_ultimo_hash_data = None
# ============================================================


# OneSignal para notificaciones web
from onesignal_notifier import OneSignalNotifier
ONESIGNAL_APP_ID = "7d1985a1-b7ee-42b0-b0c6-089032fd8124"
ONESIGNAL_API_KEY = "os_v2_app_pumylinx5zblbmggbcidf7mbeqtjlrqd5xnuehveovbccjjoekhrzd6fsria3yr6acowybb3dllebjvjwhi2nctvrieqzqjuswzxwxq"
try:
    onesignal = OneSignalNotifier(ONESIGNAL_APP_ID, ONESIGNAL_API_KEY)
    print("✅ OneSignal inicializado")
except Exception as e:
    print(f"❌ Error OneSignal: {e}")
    onesignal = None

LOCK_FILE = "script.lock"
LOCK_MAX_AGE = 300  # segundos (5 minutos)

if os.path.exists(LOCK_FILE):
    age = time.time() - os.path.getmtime(LOCK_FILE)
    if age < LOCK_MAX_AGE:
        print("⛔ Script ya en ejecución (lock activo). Abortando.")
        sys.exit(0)
    else:
        print("⚠️ Lock antiguo detectado, se ignora.")
        os.remove(LOCK_FILE)

with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))

def cleanup():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

atexit.register(cleanup)

def parse_time_et_to_iso(time_str):
    """
    Convierte un string tipo '7:00 pm ET' a un datetime ISO con tzinfo.
    """
    if not time_str:
        return None
    try:
        # Quitar ET y parsear
        dt_naive = parser.parse(time_str.replace(' ET',''))
        # Aplicar zona horaria de Eastern
        eastern = pytz.timezone('US/Eastern')
        dt_eastern = eastern.localize(dt_naive)
        return dt_eastern.isoformat()
    except Exception as e:
        print(f"Error parseando hora '{time_str}': {e}")
        return None

# === HORA PRIMER PARTIDO ===
from nba_api.stats.endpoints import scoreboardv2
import pytz
from datetime import datetime

def obtener_hora_primer_partido():
    tz = pytz.timezone('Europe/Paris')
    hoy = datetime.now(tz).strftime("%Y-%m-%d")

    try:
        sb = scoreboardv2.ScoreboardV2(game_date=hoy)
        games = sb.get_dict()['resultSets'][0]['rowSet']

        if not games:
            return None  # NO hay partidos

        horas = []
        for g in games:
            start_utc = g[4]   # Índice 4 = startTimeUTC
            if start_utc:
                start_iso = parse_time_et_to_iso(start_utc)  # ← NUEVO
                if start_iso:
                    start_dt = datetime.fromisoformat(start_iso)
                    horas.append(start_dt.astimezone(tz))
                
        if not horas:
            return None

        return min(horas)

    except Exception as e:
        print(f"Error leyendo horario partidos: {e}")
        return None

# === WEB EN VIVO ===
WEB_FOLDER = r"C:\Users\USUARIO\Desktop\con grok\web"          # ← la carpeta donde tienes el repo clonado
WEB_DATA_FILE = os.path.join(WEB_FOLDER, "data.json")
NOTIFICACIONES_HISTORICAS_FILE = os.path.join(WEB_FOLDER, "notificaciones_historicas.json")  # TODAS las notificaciones

# === CARGAR/GUARDAR NOTIFICACIONES HISTÓRICAS ===
def cargar_notificaciones_historicas():
    """Carga TODAS las notificaciones históricas del archivo dedicado"""
    try:
        if os.path.exists(NOTIFICACIONES_HISTORICAS_FILE):
            with open(NOTIFICACIONES_HISTORICAS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                notifs_global = data.get('global', [])
                notifs_franchise = data.get('franchise', [])
                print(f"✅ Cargadas {len(notifs_global)} notificaciones globales + {len(notifs_franchise)} franchise históricas")
                return notifs_global, notifs_franchise
        return [], []
    except Exception as e:
        print(f"⚠️ Error cargando notificaciones históricas: {e}")
        return [], []

def guardar_notificaciones_historicas(notifs_global, notifs_franchise):
    """Guarda TODAS las notificaciones históricas en archivo dedicado"""
    try:
        data = {
            "global": notifs_global,
            "franchise": notifs_franchise,
            "ultima_actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_global": len(notifs_global),
            "total_franchise": len(notifs_franchise)
        }
        with open(NOTIFICACIONES_HISTORICAS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Guardadas {len(notifs_global)} + {len(notifs_franchise)} notificaciones históricas")
    except Exception as e:
        print(f"❌ Error guardando notificaciones históricas: {e}")

# === CARGAR NOTIFICACIONES EXISTENTES AL INICIO ===
def cargar_notificaciones_existentes():
    """Carga las últimas 100 notificaciones del data.json o del histórico"""
    try:
        notificaciones = []
        
        # Primero intentar cargar de data.json
        if os.path.exists(WEB_DATA_FILE):
            with open(WEB_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                notificaciones = data.get('notificaciones', [])
                print(f"✅ Cargadas {len(notificaciones)} notificaciones del data.json previo")
        
        # Si está vacío o tiene pocas, cargar del histórico
        if len(notificaciones) < 50:
            historico_file = os.path.join(WEB_FOLDER, "notificaciones_historicas.json")
            if os.path.exists(historico_file):
                with open(historico_file, 'r', encoding='utf-8') as f:
                    historico = json.load(f)
                    notificaciones_hist = historico.get('global', [])
                    if notificaciones_hist:
                        # Usar las del histórico
                        notificaciones = notificaciones_hist
                        print(f"✅ Cargadas {len(notificaciones)} notificaciones del HISTÓRICO")
        
        return notificaciones[-100:] if notificaciones else []
    except Exception as e:
        print(f"⚠️ Error cargando notificaciones previas: {e}")
        return []

def cargar_notificaciones_franchise_existentes():
    """Carga las últimas 100 notificaciones franchise del data.json o del histórico"""
    try:
        notificaciones = []
        
        # Primero intentar cargar de data.json
        print(f"[DEBUG-FRANCHISE] Buscando en: {WEB_DATA_FILE}")
        if os.path.exists(WEB_DATA_FILE):
            with open(WEB_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                notificaciones = data.get('notificaciones_franchise', [])
                print(f"[DEBUG-FRANCHISE] data.json tiene {len(notificaciones)} notificaciones franchise")
        
        # Si está vacío, cargar del archivo histórico (igual que hace global)
        if not notificaciones:
            print(f"[DEBUG-FRANCHISE] data.json vacío, buscando en histórico: {NOTIFICACIONES_HISTORICAS_FILE}")
            if os.path.exists(NOTIFICACIONES_HISTORICAS_FILE):
                with open(NOTIFICACIONES_HISTORICAS_FILE, 'r', encoding='utf-8') as f:
                    historico = json.load(f)
                    notificaciones = historico.get('franchise', [])
                    print(f"[DEBUG-FRANCHISE] Histórico tiene {len(notificaciones)} notificaciones franchise")
                    if notificaciones:
                        print(f"✅ Cargadas {len(notificaciones)} notificaciones franchise del HISTÓRICO")
                        return notificaciones[-100:]
            else:
                print(f"[DEBUG-FRANCHISE] Archivo histórico NO existe")
        
        if notificaciones:
            print(f"✅ Cargadas {len(notificaciones)} notificaciones franchise del data.json previo")
        else:
            print(f"⚠️ No se encontraron notificaciones franchise en ningún sitio")
        return notificaciones[-100:] if notificaciones else []
    except Exception as e:
        print(f"⚠️ Error cargando notificaciones franchise previas: {e}")
        import traceback
        traceback.print_exc()
        return []

# Cargar notificaciones históricas al inicio
notificaciones_historicas_global, notificaciones_historicas_franchise = cargar_notificaciones_historicas()

notificaciones_del_dia = cargar_notificaciones_existentes()  # ← Cargar existentes

# ← Cargar tablas del día anterior si existen
def cargar_tablas_existentes():
    """Carga las tablas del data.json para mostrar datos anteriores hasta que haya nuevos"""
    tablas = {}
    tablas_teams = {}
    try:
        if os.path.exists(WEB_DATA_FILE):
            with open(WEB_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                tablas = data.get('tablas', {})
                tablas_teams = data.get('tablas_teams', {})
                if tablas:
                    print(f"✅ Cargadas {len(tablas)} tablas globales del data.json previo")
                if tablas_teams:
                    print(f"✅ Cargadas {len(tablas_teams)} tablas teams del data.json previo")
    except Exception as e:
        print(f"⚠️ Error cargando tablas previas: {e}")
    return tablas, tablas_teams

tablas_activos_global, tablas_teams_global = cargar_tablas_existentes()
onesignal_pendientes = []          # ← Cola de notificaciones OneSignal pendientes (se envían después del git push)

# Si ya pasaron las 10:00, ignorar partidos finalizados (día NBA cambió pero CSVs no actualizados)
# Se desactivará cuando haya partidos EN CURSO (nueva jornada)
_hora_inicio = datetime.now(pytz.timezone('Europe/Paris')).hour
ignorar_partidos_finalizados = _hora_inicio >= 10
if ignorar_partidos_finalizados:
    print("⚠️ Hora >= 10:00 - Ignorando partidos finalizados hasta nueva jornada")

# === CONFIGURACIÓN GIT ===  
# RUTA ABSOLUTA A LA CARPETA BASE DEL REPOSITORIO GIT (donde está el .git)
GIT_REPO_PATH = r"C:\Users\USUARIO\Desktop\con grok\web" # ¡IMPORTANTE! VERIFICA QUE ESTA RUTA ES CORRECTA.
GIT_BRANCH = 'main' # Si tu rama principal es 'master', cámbialo a 'master'.

# === FORZAR UTF-8 EN CONSOLA WINDOWS ===
if sys.platform.startswith('win'):
    os.system('')
    sys.stdout.reconfigure(encoding='utf-8')

# === ARCHIVO DE CONTROL ===
CONTROL_FILE = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\dias_procesados.json"
NOTIFICACIONES_FILE = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\notificaciones_enviadas.json"
LOG_CSV_FILE = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\nba_log.csv"  # ← NUEVO: LOG EN CSV

# === FUNCIÓN LOG A CSV (NUEVA) ===
def log_to_csv(message):
    """Escribe cada print en nba_log.csv con timestamp"""
    try:
        with open(LOG_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp, message.strip()])
    except Exception as e:
        pass  # No fallar por log

# === AÑADIR ENCABEZADO AL CSV (solo primera vez) ===
if not os.path.exists(LOG_CSV_FILE):
    with open(LOG_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Mensaje'])

# === SOBREESCRIBIR print() PARA QUE TAMBIÉN LOGUEE ===
original_print = print
def print(*args, **kwargs):
    message = ' '.join(map(str, args))
    original_print(message)
    log_to_csv(message)

# === ARCHIVO DE CONTROL ===
def cargar_dias_procesados():
    if os.path.exists(CONTROL_FILE):
        try:
            with open(CONTROL_FILE, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def guardar_dia_procesado(fecha_str):
    dias = cargar_dias_procesados()
    dias.add(fecha_str)
    with open(CONTROL_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(dias), f)
    print(f"Día {fecha_str} marcado como procesado")

def cargar_notificaciones_enviadas():
    enviadas = set()
    
    # 1. Cargar del archivo de notificaciones enviadas
    if os.path.exists(NOTIFICACIONES_FILE):
        try:
            with open(NOTIFICACIONES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                hoy = datetime.now(pytz.timezone('Europe/Paris')).strftime('%Y-%m-%d')
                enviadas = set(data.get(hoy, []))
                print(f"✅ Cargadas {len(enviadas)} claves de notificaciones enviadas para {hoy}")
        except Exception as e:
            print(f"Error cargando notificaciones: {e}")
    
    # 2. También extraer claves de las notificaciones ya existentes en data.json
    # Esto evita reenviar si reiniciamos el script
    if os.path.exists(WEB_DATA_FILE):
        try:
            with open(WEB_DATA_FILE, 'r', encoding='utf-8') as f:
                web_data = json.load(f)
                
                # Extraer claves de notificaciones globales
                for notif in web_data.get('notificaciones', []):
                    texto = notif.get('texto', '')
                    # Generar una clave aproximada del texto para evitar duplicados
                    clave_texto = texto.lower().replace('<b>', '').replace('</b>', '')
                    enviadas.add(f"texto_hash_{hash(clave_texto)}")
                
                # Extraer claves de notificaciones franchise
                for notif in web_data.get('notificaciones_franchise', []):
                    texto = notif.get('texto', '')
                    clave_texto = texto.lower().replace('<b>', '').replace('</b>', '')
                    enviadas.add(f"texto_hash_{hash(clave_texto)}")
                
                print(f"✅ Total claves después de incluir data.json: {len(enviadas)}")
        except Exception as e:
            print(f"⚠️ Error extrayendo claves de data.json: {e}")
    
    if not enviadas:
        print(f"⚠️ No se encontró archivo de notificaciones enviadas o está vacío")
    
    return enviadas

def guardar_notificacion(clave, texto=None):
    global mensajes_enviados
    hoy = datetime.now(pytz.timezone('Europe/Paris')).strftime('%Y-%m-%d')
    mensajes_enviados.add(clave)
    
    # También guardar el hash del texto si se proporciona
    if texto:
        clave_texto = texto.lower().replace('<b>', '').replace('</b>', '')
        mensajes_enviados.add(f"texto_hash_{hash(clave_texto)}")
    
    data = {}
    if os.path.exists(NOTIFICACIONES_FILE):
        try:
            with open(NOTIFICACIONES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except:
            pass
    
    data.setdefault(hoy, []).append(clave)
    data = {k: v for k, v in data.items() if (datetime.now(pytz.timezone('Europe/Paris')).date() - datetime.strptime(k, '%Y-%m-%d').date()).days < 7}
    
    with open(NOTIFICACIONES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def notificacion_ya_enviada(clave, texto=None):
    """Verifica si una notificación ya fue enviada, por clave o por texto"""
    if clave in mensajes_enviados:
        return True
    if texto:
        clave_texto = texto.lower().replace('<b>', '').replace('</b>', '')
        if f"texto_hash_{hash(clave_texto)}" in mensajes_enviados:
            return True
    return False

# === EMOJIS REALES ===
EMOJI_MAP = {
    "BasketballThumbsUp": "🏀👍",
    "RaisedHandsBasketball": "🙌🏀",
    "HandBasketball": "🖐️🏀",
    "BasketballBlock": "🚫🏀",
    "BasketballTarget": "🎯🏀",
    "CalendarBasketball": "📅🏀",
    "LoudspeakerTop25ALERT": "📢🔥",
    "LoudspeakerTop100ALERT": "📢",
    "LoudspeakerTrophyHOF ALERT": "🏆📢",
}

def get_prefix_emoji(key: str) -> str:
    return EMOJI_MAP.get(key, "")

# === STATS CON EMOJIS ===
STATS = {
    "pts": {"box_key": "points", "display": "points", "milestone_step": 1000, "emojis": "basketball thumbs up"},
    "trb": {"box_key": "reboundsTotal", "display": "rebounds", "milestone_step": 1000, "emojis": "raised hands basketball"},
    "ast": {"box_key": "assists", "display": "assists", "milestone_step": 1000, "emojis": "hand basketball"},
    "stl": {"box_key": "steals", "display": "steals", "milestone_step": 100, "emojis": "hand basketball"},
    "blk": {"box_key": "blocks", "display": "blocks", "milestone_step": 100, "emojis": "basketball block"},
    "fg3": {"box_key": "threePointersMade", "display": "three-pointers", "milestone_step": 100, "emojis": "basketball target"},
    "g": {"box_key": None, "display": "games", "milestone_step": 100, "emojis": "calendar basketball"},
}

# === FRANCHISE CONFIG ===
FRANCHISE_STATS = {
    "pts": {"csv_col": "PTS", "display": "points", "milestone_step": 500},
    "trb": {"csv_col": "REB", "display": "rebounds", "milestone_step": 500},
    "ast": {"csv_col": "AST", "display": "assists", "milestone_step": 500},
    "stl": {"csv_col": "STL", "display": "steals", "milestone_step": 100},
    "blk": {"csv_col": "BLK", "display": "blocks", "milestone_step": 100},
    "fg3": {"csv_col": "FG3", "display": "three-pointers", "milestone_step": 100},
    "g": {"csv_col": "G", "display": "games", "milestone_step": 100},
}

# Mapeo de abreviatura NBA API → abreviatura Basketball Reference
TEAM_ABBREV_MAP = {
    'ATL': 'ATL', 'BOS': 'BOS', 'BKN': 'NJN', 'CHA': 'CHA', 'CHI': 'CHI',
    'CLE': 'CLE', 'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GSW': 'GSW',
    'HOU': 'HOU', 'IND': 'IND', 'LAC': 'LAC', 'LAL': 'LAL', 'MEM': 'MEM',
    'MIA': 'MIA', 'MIL': 'MIL', 'MIN': 'MIN', 'NOP': 'NOH', 'NYK': 'NYK',
    'OKC': 'OKC', 'ORL': 'ORL', 'PHI': 'PHI', 'PHX': 'PHO', 'POR': 'POR',
    'SAC': 'SAC', 'SAS': 'SAS', 'TOR': 'TOR', 'UTA': 'UTA', 'WAS': 'WAS'
}

# Mapeo de abreviatura BR → nombre de franquicia (sin ciudad)
FRANCHISE_NAMES = {
    'ATL': 'Hawks', 'BOS': 'Celtics', 'NJN': 'Nets', 'CHA': 'Hornets', 'CHI': 'Bulls',
    'CLE': 'Cavaliers', 'DAL': 'Mavericks', 'DEN': 'Nuggets', 'DET': 'Pistons', 'GSW': 'Warriors',
    'HOU': 'Rockets', 'IND': 'Pacers', 'LAC': 'Clippers', 'LAL': 'Lakers', 'MEM': 'Grizzlies',
    'MIA': 'Heat', 'MIL': 'Bucks', 'MIN': 'Timberwolves', 'NOH': 'Pelicans', 'NYK': 'Knicks',
    'OKC': 'Thunder', 'ORL': 'Magic', 'PHI': '76ers', 'PHO': 'Suns', 'POR': 'Trail Blazers',
    'SAC': 'Kings', 'SAS': 'Spurs', 'TOR': 'Raptors', 'UTA': 'Jazz', 'WAS': 'Wizards'
}

# === HoF ===
def cargar_hof():
    csv_file = r'C:\Users\USUARIO\Desktop\con Grok\GLOBAL\hof_players.csv'
    if os.path.exists(csv_file):
        # Intentar varios encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                return set(df['name'].astype(str).str.lower().str.strip())
            except UnicodeDecodeError:
                continue
        print("⚠️ No se pudo leer hof_players.csv con ningún encoding")
    return set()

HOF_SET = cargar_hof()

# === CONFIG ===
TELEGRAM_TOKEN = '8443967594:AAG0-PiB2KjpMuVtyMQL3Kg5vHteI2e8xBs'
CHAT_ID = '-1003157720995'
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1436135535897874533/jqCft_wX4mKFyehmThUQSTrtkzX2W8sfQhA2znDicWj_xbPMl7MTdNsSE8dCyN1m2wAM'

PHOTOS_DIR = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\photos_merged" 
TEMP_DIR = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\temp" 
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

# === ESTADO GLOBAL ===
mensajes_enviados = cargar_notificaciones_enviadas()
overtaken_tracker = defaultdict(lambda: defaultdict(set))
recarga_hecha_hoy = False

# === CARGAR HORA PRIMER PARTIDO ===
hora_primer_partido = obtener_hora_primer_partido()
print("Primer partido:", hora_primer_partido)

# === UTILIDADES ===
def parse_minutes(minutes_str):
    if not minutes_str or minutes_str == 'PT00M00.00S':
        return 0.0
    m = re.match(r'PT(\d+)M([\d\.]+)S', minutes_str)
    if m:
        return int(m.group(1)) + float(m.group(2)) / 60
    return 0.0

import unicodedata

def normalize_name(name):
    if not name or pd.isna(name):
        return ""
    # 1. Convertir a string
    name = str(name)
    # 2. Quitar asteriscos y espacios dobles
    name = name.replace('*', '').strip()
    # 3. NORMALIZAR UNICODE (esto es la clave)
    name = unicodedata.normalize('NFD', name)
    # 4. Quitar todos los diacríticos (ć → c, š → s, ñ → n, etc.)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    # 5. Lower y limpiar espacios
    return name.lower().replace('  ', ' ')

def fix_latvian_chars(name):
    import unicodedata
    # Primero normalizar unicode (NFC -> NFD y viceversa pueden dar problemas)
    name = unicodedata.normalize('NFC', name)
    replacements = {
        'š': 's', 'Š': 'S', 'č': 'c', 'Č': 'C', 'ž': 'z', 'Ž': 'Z',
        'ā': 'a', 'Ā': 'A', 'ē': 'e', 'Ē': 'E', 'ī': 'i', 'Ī': 'I',
        'ū': 'u', 'Ū': 'U', 'ģ': 'g', 'Ģ': 'G', 'ķ': 'k', 'Ķ': 'K',
        'ļ': 'l', 'Ļ': 'L', 'ņ': 'n', 'Ņ': 'N',
        'ć': 'c', 'Ć': 'C', 'ń': 'n', 'Ń': 'N',  # Serbio/Croata
        'ić': 'ic', 'ič': 'ic',  # Sufijos comunes
        'ö': 'o', 'Ö': 'O', 'ü': 'u', 'Ü': 'U',  # Alemán/Turco
        'é': 'e', 'É': 'E', 'á': 'a', 'Á': 'A', 'í': 'i', 'Í': 'I',
        'ó': 'o', 'Ó': 'O', 'ú': 'u', 'Ú': 'U',  # Español/Portugués
        'ñ': 'n', 'Ñ': 'N',
    }
    for acc, plain in replacements.items():
        name = name.replace(acc, plain)
    # Fallback: eliminar cualquier acento restante
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return name

def get_player_headshot_url(nombre): 
    name_original = str(nombre).strip().replace('*', '')
    parts = name_original.lower().split()
    sufijo = ''
    if len(parts) >= 2 and parts[-1] in ['jr', 'sr', 'ii', 'iii', 'iv']:
        sufijo = parts[-1]
        nombre_parts = parts[:-1]
    else:
        nombre_parts = parts
    nombre_parts_fixed = [fix_latvian_chars(p) for p in nombre_parts]
    hyphen_name = f"{nombre_parts_fixed[0]}-{'-'.join(nombre_parts_fixed[1:])}{sufijo}"
    print(f"[DEBUG-FOTO] Buscando foto para: '{nombre}' -> hyphen_name: '{hyphen_name}'")
    for path in [os.path.join(PHOTOS_DIR, f"{hyphen_name}.png"), os.path.join(PHOTOS_DIR, f"{hyphen_name}.jpg")]:
        print(f"[DEBUG-FOTO]   Probando: {path} -> exists: {os.path.exists(path)}")
        if os.path.exists(path):
            return path
    candidates = [name_original.lower().replace(' ', '_'), name_original.lower().replace(' ', '-'), fix_latvian_chars(name_original.lower())]
    for cand in candidates:
        for ext in ['.jpg', '.jpeg', '.png']:
            path = os.path.join(PHOTOS_DIR, f"{cand}{ext}")
            if os.path.exists(path):
                print(f"[DEBUG-FOTO]   Encontrado alternativo: {path}")
                return path
    print(f"[DEBUG-FOTO]   ❌ No encontrada foto para: {nombre}")
    return None

# === COLLAGE ===
def resize_maximize_face(img, target_w, target_h):
    ratio_w = target_w / img.width
    ratio_h = target_h / img.height
    ratio = max(ratio_w, ratio_h)
    new_w = int(img.width * ratio)
    new_h = int(img.height * ratio)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    if target_w == 600 and target_h == 800:
        left = (new_w - target_w) // 2
        top = (new_h - target_h) // 2
        img = img.crop((left, top, left + target_w, top + target_h))
    elif target_w == 600 and target_h == 400:
        left = (new_w - target_w) // 2
        top = int((new_h - target_h) * 0.25)
        img = img.crop((left, top, left + target_w, top + target_h))
    else:
        left = (new_w - target_w) // 2
        top = (new_h - target_h) // 2
        img = img.crop((left, top, left + target_w, top + target_h))
    return img

def crear_collage(foto_urls, is_milestone=False):
    if not foto_urls:
        return None

    valid_fotos = [url for url in foto_urls if url and os.path.exists(url) and os.path.getsize(url) <= 10 * 1024 * 1024]
    if not valid_fotos:
        return None

    num_fotos = len(valid_fotos)
    
    # Canvas base 1200x800
    if num_fotos <= 4:
        canvas = Image.new('RGBA', (1200, 800), (0, 0, 0, 0))
    elif num_fotos <= 7:
        canvas = Image.new('RGBA', (1200, 800), (0, 0, 0, 0))
    else:  # 8 fotos: 2 filas de 4
        canvas = Image.new('RGBA', (1200, 800), (0, 0, 0, 0))

    if num_fotos == 1:
        # 1 foto centrada
        targets = [(1000, 800)]
        positions = [(100, 0)]
    elif num_fotos == 2:
        # 2 fotos lado a lado
        targets = [(600, 800), (600, 800)]
        positions = [(0, 0), (600, 0)]
    elif num_fotos == 3:
        # 1 grande izq, 2 pequeñas derecha
        targets = [(600, 800), (600, 400), (600, 400)]
        positions = [(0, 0), (600, 0), (600, 400)]
    elif num_fotos == 4:
        # 2x2 grid
        targets = [(600, 400), (600, 400), (600, 400), (600, 400)]
        positions = [(0, 0), (600, 0), (0, 400), (600, 400)]
    elif num_fotos == 5:
        # 2x2 pero la 4ª posición dividida en 2 verticales (300x400 cada una)
        targets = [(600, 400), (600, 400), (600, 400), (300, 400), (300, 400)]
        positions = [(0, 0), (600, 0), (0, 400), (600, 400), (900, 400)]
    elif num_fotos == 6:
        # 2x2 pero las 2 inferiores divididas en 2 cada una
        targets = [(600, 400), (600, 400), (300, 400), (300, 400), (300, 400), (300, 400)]
        positions = [(0, 0), (600, 0), (0, 400), (300, 400), (600, 400), (900, 400)]
    elif num_fotos == 7:
        # Superior: 1 grande + 2 pequeñas. Inferior: 4 pequeñas
        targets = [(600, 400), (300, 400), (300, 400), (300, 400), (300, 400), (300, 400), (300, 400)]
        positions = [(0, 0), (600, 0), (900, 0), (0, 400), (300, 400), (600, 400), (900, 400)]
    else:  # 8 fotos
        # 2 filas de 4 (cada foto 300x400)
        targets = [(300, 400)] * 8
        positions = [
            (0, 0), (300, 0), (600, 0), (900, 0),
            (0, 400), (300, 400), (600, 400), (900, 400)
        ]

    for i, url in enumerate(valid_fotos[:len(targets)]):
        try:
            img = Image.open(url).convert('RGBA')
            w, h = targets[i]
            img = resize_maximize_face(img, w, h)
            x, y = positions[i]
            canvas.paste(img, (x, y), img)
        except Exception as e:
            print(f"Error procesando imagen {i}: {e}")

    # Guardar y devolver el collage
    unique_id = f"{int(time.time() * 1000000)}"
    path = os.path.join(TEMP_DIR, f"collage_{unique_id}.png")
    canvas.save(path, 'PNG')
    return path

def publish_collage_to_web(local_path):
    """Copia un collage local a la carpeta web/collages y devuelve la URL relativa.
    Devuelve None si falla."""
    try:
        if not local_path or not os.path.exists(local_path):
            return None
        collages_dir = os.path.join(WEB_FOLDER, 'collages')
        os.makedirs(collages_dir, exist_ok=True)
        base = os.path.basename(local_path)
        # Usar microsegundos para nombre único
        unique_id = f"{int(time.time() * 1000000)}"
        name = f"{unique_id}_{base}"
        dest = os.path.join(collages_dir, name)
        shutil.copy(local_path, dest)
        try:
            os.remove(local_path)
        except:
            pass
        rel = f"collages/{name}"
        print(f"Published collage to web: {rel}")
        return rel
    except Exception as e:
        print(f"Error publishing collage to web: {e}")
        return None

# === REACH IMAGE ===
def crear_reach_image(player_photo, milestone_value, stat_display):
    if not player_photo or not os.path.exists(player_photo):
        return None
    try:
        img = Image.open(player_photo).convert('RGBA')
        img = img.resize((600, 800), Image.LANCZOS)
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("arial.ttf", 60)
        except:
            font = ImageFont.load_default()
        milestone_text = f"Reach {milestone_value:,} {stat_display}"
        text_bbox = draw.textbbox((0, 0), milestone_text, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        text_x = (600 - text_width) // 2
        text_y = 700 - text_height
        draw.rectangle(
            [(text_x - 10, text_y - 10), (text_x + text_width + 10, text_y + text_height + 10)],
            fill=(0, 0, 0, 128)
        )
        draw.text((text_x, text_y), milestone_text, fill=(255, 255, 255), font=font)
        reach_path = os.path.join(TEMP_DIR, f"reach-{milestone_value:,}.jpg".replace(',', ''))
        img.save(reach_path, 'JPEG', quality=95)
        return reach_path
    except Exception as e:
        print(f"Warning: Error creando reach image: {e}")
        return None

# === ENVÍO TELEGRAM ===
def enviar_mensaje_telegram(mensaje, foto_urls=None, is_milestone=False, milestone_value=None, player_photo=None):
    url_photo = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_text = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    if is_milestone and player_photo and os.path.exists(player_photo):
        try:
            data = {"chat_id": CHAT_ID, "caption": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True}
            with open(player_photo, 'rb') as f:
                files = {'photo': (os.path.basename(player_photo), f, 'image/jpeg')}
                response = requests.post(url_photo, data=data, files=files)
            if response.status_code == 200:
                print(f"Photo: Foto del jugador enviada a Telegram")
            stat_display = next((s['display'] for s in STATS.values() if s['display'] in mensaje.lower()), "stat")
            reach_path = crear_reach_image(player_photo, milestone_value, stat_display)
            if reach_path and os.path.exists(reach_path):
                with open(reach_path, 'rb') as f:
                    files = {'photo': (os.path.basename(reach_path), f, 'image/jpeg')}
                    response = requests.post(url_photo, data=data, files=files)
                if response.status_code == 200:
                    print(f"Photo: Reach image enviada a Telegram")
                os.remove(reach_path)
        except Exception as e:
            print(f"Warning: Error enviando milestone a Telegram: {e}")
            requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})
    elif foto_urls:
        collage_path = crear_collage(foto_urls)
        if collage_path:
            try:
                data = {"chat_id": CHAT_ID, "caption": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True}
                with open(collage_path, 'rb') as f:
                    files = {'photo': (os.path.basename(collage_path), f, 'image/png')}
                    response = requests.post(url_photo, data=data, files=files)
                if response.status_code == 200:
                    print(f"Photo: Collage Telegram enviado")
            finally:
                try: os.remove(collage_path)
                except: pass
        else:
            requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})
    else:
        requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})

# === ENVÍO DISCORD ===
def enviar_mensaje_discord(mensaje, foto_urls=None, is_milestone=False, milestone_value=None, player_photo=None):
    mensaje_limpio = mensaje.replace('<b>', '**').replace('</b>', '**')
    if len(mensaje_limpio) > 2000:
        mensaje_limpio = mensaje_limpio[:1997] + "..."
    if is_milestone and player_photo and os.path.exists(player_photo):
        try:
            with open(player_photo, 'rb') as f:
                files = {'file': ('photo.jpg', f, 'image/jpeg')}
                response = requests.post(DISCORD_WEBHOOK_URL, data={'content': mensaje_limpio}, files=files)
            print(f"Discord photo status: {response.status_code}")
            if response.status_code == 200:
                print(f"Photo: Foto del jugador enviada a Discord")
            stat_display = next((s['display'] for s in STATS.values() if s['display'] in mensaje.lower()), "stat")
            reach_path = crear_reach_image(player_photo, milestone_value, stat_display)
            if reach_path and os.path.exists(reach_path):
                with open(reach_path, 'rb') as f:
                    files = {'file': ('reach.jpg', f, 'image/jpeg')}
                    response = requests.post(DISCORD_WEBHOOK_URL, data={'content': mensaje_limpio}, files=files)
                print(f"Discord reach status: {response.status_code}")
                if response.status_code == 200:
                    print(f"Photo: Reach image enviada a Discord")
                os.remove(reach_path)
        except Exception as e:
            print(f"Warning: Error enviando milestone a Discord: {e}")
            requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})
    elif foto_urls:
        collage_path = crear_collage(foto_urls)
        if collage_path:
            try:
                with open(collage_path, 'rb') as f:
                    files = {'file': ('collage.png', f, 'image/png')}
                    response = requests.post(DISCORD_WEBHOOK_URL, data={'content': mensaje_limpio}, files=files)
                print(f"Discord collage status: {response.status_code}")
                if response.status_code == 200:
                    print(f"Photo: Collage Discord enviado")
            finally:
                try: os.remove(collage_path)
                except: pass
        else:
            requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})
    else:
        requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})

# === ENVÍO MULTIPLATAFORMA (VERSIÓN CORRECTA) ===
def enviar_multi_plataforma(mensaje, foto_urls=None, is_milestone=False, milestone_value=None, player_photo=None):
    print(f"Enviando: {mensaje[:50]}...")

    # --- 1) PUBLICAR COLLAGE EN LA WEB (si hay fotos) ---
    web_collage_url = None

    # --- 1) PUBLICAR COLLAGE EN LA WEB (si hay fotos válidas) ---
    web_collage_url = None

    valid_fotos = [f for f in (foto_urls or []) if f and os.path.exists(f)]

    if valid_fotos:
        collage_path = crear_collage(valid_fotos)
        if collage_path:
            web_collage_url = publish_collage_to_web(collage_path)
            print(f"Collage publicado en web: {web_collage_url}")

    # --- 2) ENVÍO A TELEGRAM/DISCORD (si los usas) ---
    if foto_urls:
        # Usa siempre el flujo normal de fotos → collage (1 o más)
        enviar_mensaje_telegram(mensaje, foto_urls, is_milestone, milestone_value, player_photo)
        enviar_mensaje_discord(mensaje, foto_urls, is_milestone, milestone_value, player_photo)

    elif is_milestone and player_photo and os.path.exists(player_photo):
        # Solo foto suelta si por lo que sea no hay foto_urls
        enviar_mensaje_telegram(mensaje, None, is_milestone, milestone_value, player_photo)
        enviar_mensaje_discord(mensaje, None, is_milestone, milestone_value, player_photo)

    else:
        enviar_mensaje_telegram(mensaje)
        enviar_mensaje_discord(mensaje)


    print("   TELEGRAM + DISCORD: Enviado")

    # --- 3) DEVOLVER LA URL DEL COLLAGE PARA QUE LA USE TU LÓGICA PRINCIPAL ---
    return web_collage_url

# === DETECCIÓN AUTOMÁTICA DE COLUMNAS ===
def detectar_columnas_csv(df):
    cols_lower = [col.lower().strip() for col in df.columns]
    nombre_col = None
    for candidate in ['nombre', 'player', 'name', 'jugador']:
        if candidate in cols_lower:
            nombre_col = df.columns[cols_lower.index(candidate)]
            break
    if nombre_col is None and len(df.columns) >= 2:
        nombre_col = df.columns[1]
    valor_col = None
    for candidate in ['valor', 'value', 'total', 'pts', 'trb', 'ast', 'stl', 'blk', 'fg3', 'g', 'points', 'rebounds', 'assists', 'steals', 'blocks', 'three-pointers', 'games']:
        if candidate in cols_lower:
            valor_col = df.columns[cols_lower.index(candidate)]
            break
    if valor_col is None and len(df.columns) >= 3:
        valor_col = df.columns[2]
    rank_col = None
    if 'rank' in cols_lower:
        rank_col = df.columns[cols_lower.index('rank')]
    elif len(df.columns) >= 1:
        rank_col = df.columns[0]
    return rank_col, nombre_col, valor_col

# === CARGA DE JUGADORES 250 ===
def obtener_jugadores_250_por_stat(stat):
    csv_file = os.path.join(r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL", f"{stat}_alltime.csv")
    if not os.path.exists(csv_file):
        return [(i, f"Jugador_{i}", 0) for i in range(1, 251)], []
    try:
        df = pd.read_csv(csv_file)
        if df.empty:
            return [(i, f"Jugador_{i}", 0) for i in range(1, 251)], []
        rank_col, nombre_col, valor_col = detectar_columnas_csv(df)
        if not nombre_col or not valor_col:
             return [(i, f"Jugador_{i}", 0) for i in range(1, 251)], []
        df[nombre_col] = df[nombre_col].astype(str).str.strip()
        df[valor_col] = df[valor_col].astype(str).str.replace(',', '').str.strip()
        df[valor_col] = pd.to_numeric(df[valor_col], errors='coerce').fillna(0).astype(int)
        if rank_col in df.columns:
            df[rank_col] = pd.to_numeric(df[rank_col], errors='coerce').fillna(0).astype(int)
        else:
            df['rank_temp'] = range(1, len(df) + 1)
            rank_col = 'rank_temp'
        jugadores = list(zip(df[rank_col], df[nombre_col], df[valor_col]))
        if len(jugadores) < 250:
            for i in range(len(jugadores) + 1, 251):
                jugadores.append((i, f"Jugador_{i}", 0))
        return jugadores, []
    except Exception as e:
        print(f"Error cargando {csv_file}: {e}")
        return [(i, f"Jugador_{i}", 0) for i in range(1, 251)], []

# === RECARGA DIARIA DE CSV A LAS 14:00 CEST ===
def recargar_alltime_data():
    print("\nRECARGANDO CSV ALL-TIME A LAS 14:00 CEST...")
    new_data = {}
    for stat in STATS:
        data, _ = obtener_jugadores_250_por_stat(stat)
        new_data[stat] = data
    print("   7 CSV recargados correctamente.")
    return new_data

# === GUARDAR CSVs ACTUALIZADOS ===
def guardar_csv_actualizado(stat_key, datos_actualizados):
    """Guarda el CSV con los totales actualizados después de cada partido"""
    csv_file = os.path.join(r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL", f"{stat_key}_alltime.csv")
    stat_col = STATS[stat_key]['display'].lower()  # pts, reb, ast, etc.
    
    try:
        # Crear DataFrame con los datos actualizados
        rows = []
        for jugador in datos_actualizados:
            rows.append({
                'rank': jugador['rank_actual'],
                'nombre': jugador['nombre'],
                'valor': jugador['total']
            })
        
        df = pd.DataFrame(rows)
        df = df.sort_values('rank').reset_index(drop=True)
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"   💾 CSV {stat_key}_alltime.csv guardado ({len(df)} filas)")
        return True
    except Exception as e:
        print(f"   ❌ Error guardando {stat_key}_alltime.csv: {e}")
        return False

def guardar_todos_los_csv(actuales_por_stat):
    """Guarda todos los CSVs después de procesar los partidos"""
    print("\n💾 GUARDANDO CSVs ACTUALIZADOS...")
    for stat_key, datos in actuales_por_stat.items():
        guardar_csv_actualizado(stat_key, datos)
    print("   ✅ Todos los CSVs guardados")

# === FRANCHISE: CARGA Y GUARDADO DE CSVs ===
def obtener_franchise_data(stat_key):
    """Carga el CSV de franchise para una estadística"""
    csv_file = os.path.join(r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL", f"franchise_{stat_key}.csv")
    if not os.path.exists(csv_file):
        print(f"   ⚠️ No existe franchise_{stat_key}.csv")
        return {}
    
    try:
        df = pd.read_csv(csv_file)
        stat_col = FRANCHISE_STATS[stat_key]['csv_col']
        
        # Organizar por equipo: {team: [(rank, nombre, total), ...]}
        franchise_data = {}
        for _, row in df.iterrows():
            team = row['Team']
            if team not in franchise_data:
                franchise_data[team] = []
            franchise_data[team].append({
                'rank': int(row['Rank']),
                'nombre': str(row['Player']).replace('*', '').strip(),
                'total': int(row[stat_col]),
                'norm_name': normalize_name(row['Player'])
            })
        
        return franchise_data
    except Exception as e:
        print(f"   ❌ Error cargando franchise_{stat_key}.csv: {e}")
        return {}

def cargar_todos_franchise_data():
    """Carga todos los CSVs de franchise"""
    print("\n📂 CARGANDO CSVs DE FRANCHISE...")
    franchise_data = {}
    for stat_key in FRANCHISE_STATS:
        data = obtener_franchise_data(stat_key)
        franchise_data[stat_key] = data
        teams_count = len(data)
        total_players = sum(len(players) for players in data.values())
        print(f"   ✅ franchise_{stat_key}.csv: {teams_count} equipos, {total_players} jugadores")
    return franchise_data

def guardar_franchise_csv(stat_key, franchise_data_stat):
    """Guarda el CSV de franchise actualizado"""
    csv_file = os.path.join(r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL", f"franchise_{stat_key}.csv")
    stat_col = FRANCHISE_STATS[stat_key]['csv_col']
    
    try:
        rows = []
        for team, players in franchise_data_stat.items():
            for player in players:
                rows.append({
                    'Rank': player['rank'],
                    'Player': player['nombre'],
                    stat_col: player['total'],
                    'Team': team
                })
        
        df = pd.DataFrame(rows)
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        print(f"   ❌ Error guardando franchise_{stat_key}.csv: {e}")
        return False

def guardar_todos_franchise_csv(franchise_data):
    """Guarda todos los CSVs de franchise"""
    for stat_key, data in franchise_data.items():
        if data:
            guardar_franchise_csv(stat_key, data)

def cargar_jugadores_250_por_stat(stat):
    if stat == 'stl':
        print(f"Cargado {stat.upper()} desde CSV. Filas: 251") 
    else:
        print(f"Cargado {stat.upper()} desde CSV. Filas: 250")
    return obtener_jugadores_250_por_stat(stat)

# === LÓGICA DE PARTIDOS Y STATS ===
def recuperar_dias_perdidos():
    print("\nVERIFICANDO DÍAS PERDIDOS...")
    time.sleep(1)
    print("No hay días pendientes")

# === ESTADO EXACTO DE CADA PARTIDO EN VIVO ===
game_status_cache = {}      # ← Cache para no hacer 50 llamadas cada vez
game_status_timestamp = 0   # ← Para refrescar cada 60 segundos

def actualizar_estado_partidos_en_vivo():
    global game_status_cache, game_status_timestamp
    now = time.time()
    if now - game_status_timestamp < 60:
        return
    
    print("   Actualizando estado real de partidos en vivo...")
    game_status_cache = {}
    
    try:
        board = scoreboardv2.ScoreboardV2(game_date=datetime.now().strftime('%m/%d/%Y'))
        games = board.get_data_frames()[0]
        
        for _, game in games.iterrows():
            game_id = str(game['GAME_ID'])
            status = int(game['GAME_STATUS_ID'])
            status_text = game['GAME_STATUS_TEXT'].strip()
            
            # ✅ MEJORA: Priorizar status_text sobre status_id
            if "Final" in status_text or "End" in status_text:
                game_status_cache[game_id] = "finished"
            elif "Q" in status_text or "Half" in status_text or status == 2:
                game_status_cache[game_id] = "playing"
            elif status == 1:
                game_status_cache[game_id] = "not_started"
            else:
                game_status_cache[game_id] = "inactive"  # Default a playing si hay duda
            
            # ✅ DEBUG
            print(f"      Game {game_id}: {status_text} -> {game_status_cache[game_id]}")
                
    except Exception as e:
        print(f"   Error actualizando estado en vivo: {e}")
    
    game_status_timestamp = now

# === OBTENER PARTIDOS Y STATS EN VIVO ===
def obtener_stats_partido():
    global ignorar_partidos_finalizados
    
    cet_tz = pytz.timezone('Europe/Paris')
    now_madrid = datetime.now(cet_tz)
    game_date_cet = (now_madrid - timedelta(days=1)).date() if now_madrid.hour < 10 else now_madrid.date()
    game_date_str = game_date_cet.strftime('%Y-%m-%d')
    print(f"   Dia NBA detectado (CEST): {game_date_str}")
    
    game_ids = []
    partidos_ignorados = 0
    partidos_en_curso = 0
    
    # METODO 1: cdn.nba.com (mas fiable)
    try:
        url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        games = data.get("scoreboard", {}).get("games", [])
        for g in games:
            gid = g.get("gameId")
            status = g.get("gameStatus", 1)
            if gid:
                # Contar partidos en curso
                if status == 2:
                    partidos_en_curso += 1
                # Si debemos ignorar finalizados y este está finalizado, saltar
                if ignorar_partidos_finalizados and status == 3:
                    partidos_ignorados += 1
                    continue
                game_ids.append(gid)
        
        # Si hay partidos EN CURSO, desactivar ignorar_partidos_finalizados
        # Esto significa que la nueva jornada ha comenzado
        if partidos_en_curso > 0 and ignorar_partidos_finalizados:
            ignorar_partidos_finalizados = False
            print(f"   ✅ {partidos_en_curso} partidos EN CURSO - Procesando todos los partidos")
        
        if game_ids or partidos_ignorados:
            print(f"   Partidos cdn.nba.com: {len(game_ids)} activos" + (f", {partidos_ignorados} finalizados ignorados" if partidos_ignorados else ""))
    except Exception as e:
        print(f"   cdn.nba.com fallo: {e}")
    
    # METODO 2: stats.nba.com (backup)
    if not game_ids:
        try:
            game_date_for_api = game_date_cet.strftime('%m/%d/%Y')
            board = scoreboardv2.ScoreboardV2(game_date=game_date_for_api, timeout=10)
            games_df = board.get_data_frames()[0]
            game_ids = games_df['GAME_ID'].unique().tolist()
            print(f"   Partidos desde ScoreboardV2: {len(game_ids)}")
        except Exception as e:
            print(f"   ScoreboardV2 fallo: {e}")
    
    game_ids = list(set(game_ids))[:15]
    if game_ids:
        print(f"   Procesando {len(game_ids)} partidos: {game_ids}")
    stats_partido = {}
    for game_id in game_ids:
        try:
            # Descargar JSON oficial NBA
            url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            resp = requests.get(url, headers=headers)
            data_json = resp.json()
            game_data = data_json["game"]

            # Determinar estado general del partido
            if game_data.get("gameStatus") == 2:
                game_status_cache[game_id] = "playing"
            elif game_data.get("gameStatus") == 3:
                game_status_cache[game_id] = "finished"
            else:
                game_status_cache[game_id] = "not_started"

            # Crear mapa de estado de jugadores
            player_status_map = {}
            for side in ["homeTeam", "awayTeam"]:
                for p in game_data.get(side, {}).get("players", []):
                    norm = normalize_name(p.get("name", ""))
                    if norm:
                        # ACTIVE o INACTIVE
                        player_status_map[norm] = p.get("status", "ACTIVE")

            # Procesar cada jugador
            for side in ["homeTeam", "awayTeam"]:
                team_data = game_data.get(side, {})
                team_abbr = team_data.get("teamTricode", "")
                if not team_abbr:
                    continue

                for player in team_data.get("players", []):
                    name = player.get("name", "")
                    if not name:
                        continue
                    norm = normalize_name(name)
                    if not norm:
                        continue

                    # Inicializar stats si no existen
                    stats_partido.setdefault(norm, {
                        "player_id": player.get("personId"),
                        "game_id": game_id,
                        "game_status": game_status_cache.get(game_id, "not_started"),
                        "team": team_abbr,
                        **{k: 0 for k in STATS}
                    })

                    # 🔴 REGLA DEFINITIVA: SI EL JUGADOR ES INACTIVO, SIEMPRE INACTIVE/ROJO
                    if player_status_map.get(norm, "ACTIVE") == "INACTIVE":
                        stats_partido[norm]['game_status'] = "inactive"
                        continue  # No sumar estadísticas ni cambiar estado

                    # Jugador activo → estado según partido
                    stats_partido[norm]['game_status'] = game_status_cache.get(game_id, "not_started")

                    # Extraer estadísticas de jugadores activos
                    s = player.get("statistics", {})
                    mins = parse_minutes(player.get("minutesPlayedActual", "PT00M00.00S"))
                    has_stats = any(s.get(k, 0) > 0 for k in ['points','reboundsTotal','assists','steals','blocks'])
                    played = (mins > 0 or has_stats) and stats_partido[norm]['game_status'] in ["playing", "finished"]

                    # DEBUG: Si tiene minutos pero no cuenta como played
                    if mins > 0 and not played:
                        print(f"[DEBUG-GAMES] {norm}: mins={mins}, has_stats={has_stats}, game_status={stats_partido[norm]['game_status']}, played={played}")

                    if played:
                        stats_partido[norm]['g'] += 1

                    # Sumar estadísticas
                    for k, info in STATS.items():
                        if k == 'g': continue
                        val = s.get(info['box_key'], 0)
                        if isinstance(val, (int, float)):
                            stats_partido[norm][k] += max(0, int(val))

        except Exception as e:
            print(f"   Error en game {game_id}: {e}")

    return stats_partido
    
# === COMBINAR DATOS ===
def combinar_datos(jugadores_250, stats_partido, stat_key):
    ranking_completo = []
    for rank, nombre, valor_anterior in jugadores_250:
        norm_name = normalize_name(nombre)
        incremento = stats_partido.get(norm_name, {}).get(stat_key, 0)
        if stat_key == 'g' and norm_name in stats_partido:
            incremento = stats_partido[norm_name].get('g', 0)
        elif stat_key == 'g':
            incremento = 0
        nuevo_valor = valor_anterior + incremento
        ranking_completo.append({
             'posicion_inicial': rank, 
             'nombre': nombre,
             f'{stat_key}_250': valor_anterior,
             f'{stat_key}_partido': incremento,
             'total': nuevo_valor,
             'norm_name': norm_name
        })
    ranking_completo.sort(key=lambda x: (-x['total'], x['nombre']))
    for i, item in enumerate(ranking_completo):
        item['rank_actual'] = i + 1
    return ranking_completo

SINGULAR_PLURAL = {
    "points": "point",
    "rebounds": "rebound",
    "assists": "assist",
    "steals": "steal",
    "blocks": "block",
    "three-pointers": "three-pointer",
    "games": "game"
}

def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n}{suffix}"

def generar_badges(posicion, nombre_jugador=None, rivales=None):
    """Genera emojis según posición, HOF status de rivales, etc."""
    badges = ""
    if posicion <= 25:
        badges += "📢🔝2️⃣5️⃣ "      # Top 25 all-time
    elif posicion <= 100:
        badges += "📢🔝💯 "          # Top 100 all-time
    
    # Comprobar si algún rival adelantado es Hall of Famer
    if rivales:
        hof_rivals = [r for r in rivales if r.lower().strip() in HOF_SET]
        if hof_rivals:
            badges += "📢HOF🏛️ "    # Hall of Famer adelantado
    
    return badges.strip()

def generar_badges_franchise(posicion, rivales=None):
    """Genera emojis para franchise rankings (top 3 y top 10)"""
    badges = ""
    if posicion <= 3:
        badges += "📢🔝3️⃣ "       # Top 3 franchise
    elif posicion <= 10:
        badges += "📢🔝🔟 "        # Top 10 franchise
    
    # HOF también aplica a franchise
    if rivales:
        hof_rivals = [r for r in rivales if r.lower().strip() in HOF_SET]
        if hof_rivals:
            badges += "📢HOF🏛️ "
    
    return badges.strip()

# === FRANCHISE: DETECCIÓN DE ADELANTAMIENTOS ===
franchise_overtaken_tracker = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))  # stat -> team -> player -> set(overtaken)
notificaciones_franchise_del_dia = cargar_notificaciones_franchise_existentes()  # Cargar existentes
franchise_valores_iniciales = {}  # Guarda los valores al inicio del día: {stat: {team: {norm_name: total_inicial}}}

def inicializar_franchise_valores_iniciales(franchise_data):
    """Guarda los valores iniciales del día para no acumular stats incorrectamente"""
    global franchise_valores_iniciales
    franchise_valores_iniciales = {}
    for stat_key, teams_data in franchise_data.items():
        franchise_valores_iniciales[stat_key] = {}
        for team, players in teams_data.items():
            franchise_valores_iniciales[stat_key][team] = {}
            for p in players:
                franchise_valores_iniciales[stat_key][team][p['norm_name']] = p['total']
    print("   ✅ Valores iniciales de franchise guardados")

def detectar_adelantamientos_franchise(franchise_data, stats_partido, stat_key):
    """Detecta adelantamientos y milestones en rankings de franchise"""
    global notificaciones_franchise_del_dia
    
    stat_display = FRANCHISE_STATS[stat_key]['display']
    step = FRANCHISE_STATS[stat_key]['milestone_step']
    
    cambios_por_equipo = {}
    
    for norm_name, player_stats in stats_partido.items():
        team_api = player_stats.get('team', '')
        if not team_api:
            continue
        
        # Convertir abreviatura NBA API → Basketball Reference
        team_br = TEAM_ABBREV_MAP.get(team_api, team_api)
        
        if team_br not in franchise_data.get(stat_key, {}):
            continue
        
        team_ranking = franchise_data[stat_key][team_br]
        
        hoy = player_stats.get(stat_key, 0)
        if hoy <= 0:
            continue
        
        # Buscar al jugador en el ranking del equipo
        jugador_en_ranking = None
        jugador_idx = -1
        for idx, p in enumerate(team_ranking):
            if p['norm_name'] == norm_name:
                jugador_en_ranking = p
                jugador_idx = idx
                break
        
        if not jugador_en_ranking:
            # Jugador no está en top 25 de su equipo
            continue
        
        # CORRECCIÓN: Usar valor inicial del día, no el acumulado
        valor_inicial = franchise_valores_iniciales.get(stat_key, {}).get(team_br, {}).get(norm_name, jugador_en_ranking['total'])
        antes = valor_inicial  # Valor al inicio del día
        nuevo = valor_inicial + hoy  # Valor inicial + stats de hoy
        jugador_en_ranking['total'] = nuevo  # Actualizar con el cálculo correcto
        
        # Detectar adelantamientos
        adelantados = []
        for idx2, rival in enumerate(team_ranking):
            if rival['norm_name'] == norm_name:
                continue
            
            # Obtener el total actual del rival (también basado en valor inicial + hoy del rival)
            rival_norm = rival['norm_name']
            rival_inicial = franchise_valores_iniciales.get(stat_key, {}).get(team_br, {}).get(rival_norm, rival['total'])
            rival_hoy = stats_partido.get(rival_norm, {}).get(stat_key, 0) if rival_norm in stats_partido else 0
            rival_total = rival_inicial + rival_hoy
            rival['total'] = rival_total  # Actualizar rival también
            
            # Si antes estaba por debajo o igual y ahora ESTRICTAMENTE por encima
            # (igual que en global: valor_antes_rival >= valor_antes_actual and valor_ahora_actual > valor_ahora_rival)
            if antes <= rival_inicial and nuevo > rival_total:
                # Verificar que no lo hemos notificado ya
                if rival['nombre'] not in franchise_overtaken_tracker[stat_key][team_br][norm_name]:
                    adelantados.append(rival['nombre'])
                    franchise_overtaken_tracker[stat_key][team_br][norm_name].add(rival['nombre'])
        
        # Reordenar ranking del equipo
        team_ranking.sort(key=lambda x: x['total'], reverse=True)
        for idx, p in enumerate(team_ranking):
            p['rank'] = idx + 1
        
        # Nueva posición del jugador
        nueva_pos = next((p['rank'] for p in team_ranking if p['norm_name'] == norm_name), 0)
        
        # Generar notificación si hay adelantamiento
        if adelantados and nueva_pos <= 25:
            clave = f"franchise_{norm_name}_{stat_key}_{team_br}_passed_{'_'.join(sorted([normalize_name(r) for r in adelantados]))}"
            
            # Construir el mensaje primero para verificar duplicados por texto
            badges = generar_badges_franchise(nueva_pos, adelantados)
            badge_prefix = f"{badges} " if badges else ""
            
            nombre = jugador_en_ranking['nombre']
            franchise_name = FRANCHISE_NAMES.get(team_br, team_br)
            
            if len(adelantados) == 1:
                rivales_texto = f"<b>{adelantados[0]}</b>"
            else:
                rivales_texto = ", ".join([f"<b>{r}</b>" for r in adelantados[:-1]]) + f" and <b>{adelantados[-1]}</b>"
            
            # Mensaje diferente para games vs otras stats
            if stat_key == 'g':
                msg = f"{badge_prefix}<b>{nombre}</b> today surpasses {rivales_texto} and is now ranked {ordinal(nueva_pos)} in {franchise_name} history with {nuevo:,} games."
            else:
                txt_hoy = SINGULAR_PLURAL.get(stat_display, stat_display) if hoy == 1 else stat_display
                msg = f"{badge_prefix}<b>{nombre}</b> ({hoy} {txt_hoy} today) has passed {rivales_texto} and is now ranked {ordinal(nueva_pos)} in {franchise_name} history with {nuevo:,}."
            
            if not notificacion_ya_enviada(clave, msg):
                guardar_notificacion(clave, msg)
                
                msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()
                
                notificaciones_franchise_del_dia.append({
                    "texto": msg_limpio,
                    "foto": [],  # Sin foto para franchise por ahora
                    "timestamp": int(time.time()),
                    "team": team_br,
                    "type": "franchise"
                })
                print(f"[FRANCHISE] Adelantamiento: {nombre} pasa a {adelantados} en {franchise_name}")
                
                # 🔔 ENVIAR NOTIFICACIÓN ONESIGNAL PARA FRANCHISE
                if onesignal:
                    onesignal_pendientes.append({
                        "titulo": f"🏀 {franchise_name} Ranking",
                        "mensaje": msg_limpio,
                        "tipo": "overtake",
                        "scope": "teams",
                        "stat": stat_key,
                        "ranking": nueva_pos,
                        "team": team_br,
                        "imagen_url": None
                    })
                    print(f"[ONESIGNAL] Encolada notificación franchise: {nombre}")
        
        # Detectar milestones
        next_milestone = ((nuevo // step) + 1) * step
        prev_milestone = (antes // step) * step
        current_milestone = (nuevo // step) * step
        
        if current_milestone > prev_milestone and current_milestone > 0:
            clave_mile = f"franchise_milestone_{norm_name}_{stat_key}_{team_br}_{current_milestone}"
            
            badges = generar_badges_franchise(nueva_pos)
            badge_prefix = f"{badges} " if badges else ""
            
            nombre = jugador_en_ranking['nombre']
            franchise_name = FRANCHISE_NAMES.get(team_br, team_br)
            
            # Mensaje con formato igual que global
            # Para games: "Player today becomes Xth player to reach Y games with Franchise"
            # Para otras stats: "(X stat today) becomes Xth player to reach Y stat with Franchise"
            if stat_key == 'g':
                msg = f"{badge_prefix}<b>{nombre}</b> today becomes the {ordinal(nueva_pos)} player to reach {current_milestone:,} {stat_display} with {franchise_name}."
            else:
                txt_hoy = SINGULAR_PLURAL.get(stat_display, stat_display) if hoy == 1 else stat_display
                msg = f"{badge_prefix}<b>{nombre}</b> ({hoy} {txt_hoy} today) becomes the {ordinal(nueva_pos)} player to reach {current_milestone:,} {stat_display} with {franchise_name}."
            
            if not notificacion_ya_enviada(clave_mile, msg):
                guardar_notificacion(clave_mile, msg)
                
                msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()
                
                notificaciones_franchise_del_dia.append({
                    "texto": msg_limpio,
                    "foto": [],
                    "timestamp": int(time.time()),
                    "team": team_br,
                    "type": "franchise_milestone"
                })
                print(f"[FRANCHISE] Milestone: {nombre} alcanza {current_milestone} {stat_display} con {team_br}")
                
                # 🔔 ENVIAR NOTIFICACIÓN ONESIGNAL PARA MILESTONE FRANCHISE
                if onesignal:
                    onesignal_pendientes.append({
                        "titulo": f"🎯 {franchise_name} Milestone",
                        "mensaje": msg_limpio,
                        "tipo": "milestone",
                        "scope": "teams",
                        "stat": stat_key,
                        "ranking": nueva_pos,
                        "team": team_br,
                        "imagen_url": None
                    })
                    print(f"[ONESIGNAL] Encolada notificación milestone franchise: {nombre}")
    
    return franchise_data

def detectar_adelantamientos_o_milestones(anterior, actual, stat_key):
    print(f"[NOTIF-1] Entrando en detectar_adelantamientos_o_milestones | stat={stat_key}")

    # ✅ DEBUG ESPECÍFICO PARA GAMES
    if stat_key == 'g':
        print(f"[DEBUG-GAMES] Jugadores activos hoy:")
        for act in actual:
            hoy = act[f'{stat_key}_partido']
            if hoy > 0:
                print(f"   - {act['nombre']}: +{hoy} games (total={act['total']})")
    
    prev = {j['norm_name']: j for j in anterior}
    step = STATS[stat_key]['milestone_step']
    stat_display_plural = STATS[stat_key]['display']
    
    cambios = {}
    for act in actual:
        norm_name = act['norm_name']
        cambios.setdefault(norm_name, {'diff': '', 'antece': '', 'nota': ''})

    for i, act in enumerate(actual):
        norm_name = act['norm_name']
        nombre = act['nombre']
        total = act['total']
        antes = act[f'{stat_key}_250']
        hoy = act[f'{stat_key}_partido']
        p = act['rank_actual']
        nuevo = total

        next_milestone = ((total // step) + 1) * step
        dist_milestone = next_milestone - total

        # LÓGICA DE CAMBIOS
        if p == 1:
            if dist_milestone > 0:
                cambios[norm_name] = {'diff': dist_milestone, 'antece': str(next_milestone), 'nota': ''}
            else:
                cambios[norm_name] = {'diff': '', 'antece': '', 'nota': ''}
        elif i > 0:
            nombre_superior = actual[i-1]['nombre']
            total_superior = actual[i-1]['total']
            diferencia = total_superior - total
            nota_rival = ""
            if diferencia == 0: nota_rival = "Mismo total"
            elif diferencia == 1: nota_rival = "A tiro de 1"
            elif diferencia == 2 and stat_key == 'pts': nota_rival = "A tiro de 2"
            elif diferencia == 3 and stat_key == 'pts': nota_rival = "A tiro de 3"
            if dist_milestone > 0 and dist_milestone < diferencia:
                cambios[norm_name] = {'diff': dist_milestone, 'antece': str(next_milestone), 'nota': nota_rival}
            else:
                cambios[norm_name] = {'diff': diferencia, 'antece': nombre_superior[:30], 'nota': nota_rival}

        if hoy <= 0:
            continue

        # ADELANTAMIENTOS
        # ✅ DEBUG 1: Ver si entra aquí
        if stat_key == 'g':
            print(f"\n[DEBUG-G-1] Procesando adelantamientos para {nombre}")
            print(f"   hoy: {hoy}, antes: {antes}, nuevo: {nuevo}")

        if hoy <= 0:
            if stat_key == 'g':
                print(f"   [DEBUG-G-2] ⏭️ SALTADO por hoy <= 0")
            continue

        # ✅ DEBUG 2: Ver si pasa la validación de hoy
        if stat_key == 'g':
            print(f"   [DEBUG-G-3] ✅ Pasó validación hoy > 0, calculando adelantamientos...")

        jugadores_antes = {j['norm_name']: j[f'{stat_key}_250'] for j in actual}
        jugadores_ahora = {j['norm_name']: j['total'] for j in actual}

        adelantados = []
        for norm_rival, valor_antes_rival in jugadores_antes.items():
            if norm_rival == norm_name:
                continue
            valor_antes_actual = antes
            valor_ahora_rival = jugadores_ahora.get(norm_rival, 0)
            valor_ahora_actual = nuevo
            
            # ✅ DEBUG 3: Ver CADA comparación para games
            if stat_key == 'g' and 'conley' in norm_name:
                if valor_antes_rival == valor_antes_actual - 1:  # Rivales cercanos
                    try:
                        rival_nombre = next((j['nombre'] for j in actual if j['norm_name'] == norm_rival), norm_rival)
                        print(f"   [DEBUG-G-4] Comparando con {rival_nombre}:")
                        print(f"      Antes: rival={valor_antes_rival}, {nombre}={valor_antes_actual}")
                        print(f"      Ahora: rival={valor_ahora_rival}, {nombre}={valor_ahora_actual}")
                        print(f"      ¿Rival estaba delante? {valor_antes_rival > valor_antes_actual}")
                        print(f"      ¿Ahora estoy delante? {valor_ahora_actual > valor_ahora_rival}")
                    except:
                        pass
            
            if (valor_antes_rival >= valor_antes_actual and valor_ahora_actual > valor_ahora_rival):
                try:
                    idx = [j['norm_name'] for j in actual].index(norm_rival)
                    adelantados.append(actual[idx]['nombre'])
                    if stat_key == 'g':
                        print(f"   [DEBUG-G-5] ✅ ADELANTADO: {actual[idx]['nombre']}")
                except Exception as e:
                    if stat_key == 'g':
                        print(f"   [DEBUG-G-5-ERROR] Error añadiendo rival: {e}")

        # ✅ DEBUG 4: Ver resultado
        if stat_key == 'g':
            print(f"   [DEBUG-G-6] adelantados totales: {adelantados}")
            print(f"   [DEBUG-G-7] tracker actual: {list(overtaken_tracker[stat_key][norm_name])}")

        nuevos = set(adelantados) - overtaken_tracker[stat_key][norm_name]

        # ✅ DEBUG 5: Ver el set final
        if stat_key == 'g':
            print(f"   [DEBUG-G-8] nuevos (diferencia): {nuevos}")
            print(f"   [DEBUG-G-9] ¿Entrará en if nuevos? {bool(nuevos)}\n")

        # MILESTONE (SIN EMOJIS)
        clave_milestone = f"{norm_name}_{stat_key}_milestone_{nuevo}"
        txt_hoy = SINGULAR_PLURAL[stat_display_plural] if hoy == 1 else stat_display_plural
        badges = generar_badges(p)
        badge_prefix = f"{badges} " if badges else ""
        msg = f"{badge_prefix}<b>{nombre}</b> ({hoy} {txt_hoy} today) becomes the {ordinal(p)} player to reach {nuevo:,}"
        
        if nuevo % step == 0 and nuevo > antes and not notificacion_ya_enviada(clave_milestone, msg):
            print(f"[NOTIF-2] MILESTONE detectado → {nombre} | {nuevo} {stat_display_plural}")
            msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()
            foto = get_player_headshot_url(nombre)
            # Enviar a Telegram/Discord como antes
            enviar_multi_plataforma(msg, [foto], True, nuevo, foto)
            guardar_notificacion(clave_milestone, msg)

            # Usar crear_collage igual que adelantamientos (1 foto)
            try:
                web_url = None

                if foto and os.path.exists(foto):
                    print(f"[MILESTONE] Foto encontrada: {foto}")
                    collage_path = crear_collage([foto])  # Igual que adelantamientos
                    if collage_path:
                        print(f"[MILESTONE] Collage creado: {collage_path}")
                        web_url = publish_collage_to_web(collage_path)
                    else:
                        print(f"[MILESTONE] ❌ crear_collage devolvió None")
                else:
                    print(f"[MILESTONE] ❌ No se encontró foto para {nombre}")

                if web_url:
                    notificaciones_del_dia.append({
                        "texto": msg_limpio,
                        "foto": [web_url],
                        "timestamp": int(time.time())
                    })
                    print(f"[NOTIF-3] Añadido MILESTONE a notificaciones_del_dia CON FOTO")

                    # Encolar notificación OneSignal para enviar después del git push
                    if onesignal:
                        onesignal_pendientes.append({
                            "titulo": "🎯 Milestone",
                            "mensaje": msg_limpio,
                            "tipo": "milestone",
                            "scope": "global",
                            "stat": stat_key,
                            "ranking": p,
                            "imagen_url": web_url
                        })
                        print(f"[ONESIGNAL] Encolada notificación milestone con imagen")

                else:
                    notificaciones_del_dia.append({
                        "texto": msg_limpio,
                        "foto": [],
                        "timestamp": int(time.time())
                    })
                    print(f"[NOTIF-3B] Añadido MILESTONE a notificaciones_del_dia SOLO TEXTO")
                    
                    # Encolar notificación OneSignal para enviar después del git push
                    if onesignal:
                        onesignal_pendientes.append({
                            "titulo": "🎯 Milestone",
                            "mensaje": msg_limpio,
                            "tipo": "milestone",
                            "scope": "global",
                            "stat": stat_key,
                            "ranking": p,
                            "imagen_url": None
                        })
                        print(f"[ONESIGNAL] Encolada notificación milestone sin imagen")
            except Exception as e:
                print(f"Warning: error publicando milestone image: {e}")
                notificaciones_del_dia.append({
                    "texto": msg_limpio,
                    "foto": [],
                    "timestamp": int(time.time())
                })

        # ADELANTAMIENTO (SIN EMOJIS)
        if nuevos:
            if stat_key == 'g':
                print(f"[DEBUG-G-10] ✅✅✅ ENTRANDO EN BLOQUE DE ENVÍO")
            
            print(f"[NOTIF-4] ADELANTAMIENTO detectado → {nombre} pasa a {nuevos}")
            rivales_key = "_".join(sorted([normalize_name(r) for r in nuevos]))
            clave_grupal = f"{norm_name}_{stat_key}_passed_{antes}_{rivales_key}"
            
            # Construir mensaje para verificar también por texto
            nuevos_lista = sorted(nuevos)
            txt_hoy = SINGULAR_PLURAL[stat_display_plural] if hoy == 1 else stat_display_plural
            badges = generar_badges(p, nombre, nuevos_lista)
            badge_prefix = f"{badges} " if badges else ""
            
            if len(nuevos_lista) == 1:
                rivales_texto = f"<b>{nuevos_lista[0]}</b>"
            else:
                rivales_texto = ", ".join([f"<b>{r}</b>" for r in nuevos_lista[:-1]]) + f" and <b>{nuevos_lista[-1]}</b>"
            
            if stat_key == 'g':
                msg = f"{badge_prefix}<b>{nombre}</b> today surpasses {rivales_texto} and now ranked {ordinal(p)} all time with {nuevo:,} games"
            else:
                msg = f"{badge_prefix}<b>{nombre}</b> ({hoy} {txt_hoy} today) has passed {rivales_texto} and is now ranked {ordinal(p)} all-time with {nuevo:,}."
            
            if stat_key == 'g':
                print(f"[DEBUG-G-11] Clave: {clave_grupal}")
                print(f"[DEBUG-G-12] ¿Ya enviado? {notificacion_ya_enviada(clave_grupal, msg)}")
            
            if notificacion_ya_enviada(clave_grupal, msg):
                if stat_key == 'g':
                    print(f"[DEBUG-G-13] ⏭️ SALTADO: ya enviado\n")
                continue
            
            if stat_key == 'g':
                print(f"[DEBUG-G-14] 🚀 GUARDANDO NOTIFICACIÓN...")
            
            guardar_notificacion(clave_grupal, msg)

            for rival in nuevos:
                overtaken_tracker[stat_key][norm_name].add(rival)

            msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()

            # Intentamos crear un collage local y publicarlo en la web. Si no se puede, guardamos sólo el texto.
            try:
                foto_principal = get_player_headshot_url(nombre)
                fotos = [foto_principal] if foto_principal else []
                print(f"[COLLAGE] Foto principal de {nombre}: {foto_principal}")
                
                for rival in nuevos_lista:
                    foto_rival = get_player_headshot_url(rival)
                    if foto_rival and len(fotos) < 8:  # Ahora soportamos hasta 8
                        fotos.append(foto_rival)
                
                print(f"[COLLAGE] Total fotos válidas: {len(fotos)}")
                
                if fotos:
                    collage_path = crear_collage(fotos)
                    web_url = publish_collage_to_web(collage_path) if collage_path else None
                else:
                    print(f"[COLLAGE] ❌ No hay fotos válidas para el collage")
                    web_url = None

                if web_url:
                    notificaciones_del_dia.append({
                        "texto": msg_limpio,
                        "foto": [web_url],
                        "timestamp": int(time.time())
                    })
                    print(f"[NOTIF-5] Añadido ADELANTAMIENTO a notificaciones_del_dia CON FOTO")

                    # Encolar notificación OneSignal para enviar después del git push
                    if onesignal:
                        onesignal_pendientes.append({
                            "titulo": "🔥 Ranking Update",
                            "mensaje": msg_limpio,
                            "tipo": "overtake",
                            "scope": "global",
                            "stat": stat_key,
                            "ranking": p,
                            "imagen_url": web_url
                        })
                        print(f"[ONESIGNAL] Encolada notificación overtake con imagen")

                else:
                    notificaciones_del_dia.append({
                        "texto": msg_limpio,
                        "foto": [],
                        "timestamp": int(time.time())
                    })
                    print(f"[NOTIF-5B] Añadido ADELANTAMIENTO a notificaciones_del_dia SOLO TEXTO")

                    # Encolar notificación OneSignal para enviar después del git push
                    if onesignal:
                        onesignal_pendientes.append({
                            "titulo": "🔥 Ranking Update",
                            "mensaje": msg_limpio,
                            "tipo": "overtake",
                            "scope": "global",
                            "stat": stat_key,
                            "ranking": p,
                            "imagen_url": None
                        })
                        print(f"[ONESIGNAL] Encolada notificación overtake sin imagen")
            except Exception as e:
                print(f"Warning: error generando/publicando collage: {e}")
                notificaciones_del_dia.append({
                    "texto": msg_limpio,
                    "foto": [],
                    "timestamp": int(time.time())
                })

    return cambios

def mostrar_ranking_optimizado(actual, stat_key, stats_partido, cambios):
    stat_display = STATS.get(stat_key, {}).get('display', stat_key).upper()
    print(f"\n{stat_display} - ACTIVOS:")
    jugadores_relevantes = []
    jugadores_en_juego = set(stats_partido.keys())

    for r in actual:
        norm_name = r['norm_name']
        nombre = r['nombre']
        equipo = stats_partido.get(norm_name, {}).get('team', '---')  # <- NUEVO
        if norm_name not in jugadores_en_juego:
            continue

        cambio = cambios.get(norm_name, {})
        diff = cambio.get('diff', '')
        antecede = str(cambio.get('antece', ''))[:25]
        nota = cambio.get('nota', '')
        antes = r[f'{stat_key}_250']
        hoy = r[f'{stat_key}_partido']
        total = r['total']
        rank = r['rank_actual']

        jugadores_relevantes.append({
            'rank': f"{rank:>3}",
            'nombre': f"{nombre:<25}",
            'team': f"{equipo:<5}",       # <- NUEVO
            'antes': f"{antes:>6}",
            'hoy': f"{hoy:>4}",
            'total': f"{total:>6}",
            'diff': f"{diff:>4}" if diff != '' else '    ',
            'antece': f"{antecede:<25}",
            'nota': f"{nota:<15}"
        })

    jugadores_relevantes.sort(key=lambda x: int(x['rank']))

    # Cabecera actualizada
    header = f"{'Rank':>3} {'Nombre':<25} {'Team':<5} {'Antes':>6} {'Hoy':>4} {'Total':>6} {'Diff':>4} {'Antece':<25} {'Nota':<15}"
    print(header)
    print("-" * 85)
    for j in jugadores_relevantes:
        print(f"{j['rank']} {j['nombre']} {j['team']} {j['antes']} {j['hoy']} {j['total']} {j['diff']} {j['antece']} {j['nota']}")

    return jugadores_relevantes

def main():
    # NO llamar generar_web_en_vivo() al arrancar - preserva las notificaciones existentes
    # generar_web_en_vivo() # Web vacía al arrancar
    print("Cargando datos all-time...")
    alltime_data = {}
    alltime_data_actualizado = {}  # Para guardar los CSVs actualizados
    for stat in STATS:
        data, _ = obtener_jugadores_250_por_stat(stat) 
        alltime_data[stat] = data
        alltime_data_actualizado[stat] = []  # Se llenará con los datos actualizados
    
    # Cargar datos de franchise
    franchise_data = cargar_todos_franchise_data()
    inicializar_franchise_valores_iniciales(franchise_data)  # Guardar valores del día
    
    recuperar_dias_perdidos()
    
    cet_tz = pytz.timezone('Europe/Paris')
    now_init = datetime.now(cet_tz)
    last_day = now_init.date() if now_init.hour >= 10 else (now_init - timedelta(days=1)).date()
    global recarga_hecha_hoy
    recarga_hecha_hoy = False
    
    # Inicializar anteriores con las stats actuales del partido para evitar falsos adelantamientos
    print("📊 Obteniendo stats actuales para inicializar...")
    stats_partido_inicial = obtener_stats_partido()
    anteriores = {stat: combinar_datos(alltime_data[stat], stats_partido_inicial, stat) for stat in STATS}
    print(f"   ✅ Anteriores inicializados con {len(stats_partido_inicial)} jugadores activos")
    
    # NO ejecutar scrapers al iniciar - solo se ejecutan a las 14:00 exactas
    # El script principal carga los CSVs que ya existen y trabaja con ellos

    while True:
        try:
            now = datetime.now(cet_tz)
            current_day = now.date() if now.hour >= 10 else (now - timedelta(days=1)).date()
            current_day_str = current_day.strftime('%Y-%m-%d')
            if last_day != current_day:
                print(f"\nNUEVO DÍA: {current_day_str}")
                # NO reiniciar anteriores aquí - los CSVs aún no tienen los datos de anoche
                # anteriores se reiniciará a las 14:00 después de actualizar los CSVs
                last_day = current_day
                recarga_hecha_hoy = False
                global mensajes_enviados, ignorar_partidos_finalizados, franchise_overtaken_tracker, notificaciones_franchise_del_dia
                mensajes_enviados = cargar_notificaciones_enviadas()
                # CRÍTICO: Ignorar partidos finalizados hasta que se actualicen los CSVs a las 14:00
                # Los partidos de anoche ya acabaron pero los CSVs aún no tienen sus datos
                ignorar_partidos_finalizados = True
                print(f"   ⚠️ Partidos finalizados ignorados hasta actualización de CSVs (14:00)")
                print(f"   Notificaciones del día actual cargadas del archivo.")
                # NO reinicializar valores de franchise aquí - esperar a las 14:00
                # Limpiar tracker de adelantamientos de franchise
                franchise_overtaken_tracker = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
                # Cargar notificaciones franchise del histórico (no resetear a vacío)
                notificaciones_franchise_del_dia = cargar_notificaciones_franchise_existentes()
                print(f"   Cargadas {len(notificaciones_franchise_del_dia)} notificaciones franchise del histórico")
            if now.hour >= 14 and not recarga_hecha_hoy:
                # 🔄 EJECUTAR SCRAPERS DE BASKETBALL REFERENCE
                # IMPORTANTE: Solo actualizan los CSVs, NO tocan la web ni las notificaciones
                print("\n" + "=" * 60)
                print("🔄 EJECUTANDO SCRAPERS DE BASKETBALL REFERENCE (14:00)")
                print("   Solo actualizan CSVs - NO tocan web ni notificaciones")
                print("=" * 60)
                
                import subprocess
                from datetime import datetime as dt
                
                log_file = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\scraper_log.txt"
                
                scrapers = [
                    (r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\scraper_br_7_csvs_selenium.py", "7 CSVs Globales"),
                    (r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\scrape_franchise_initial.py", "210 CSVs Franchise"),
                ]
                
                for scraper_path, nombre in scrapers:
                    print(f"\n📊 Ejecutando: {nombre}...")
                    try:
                        # Guardar log a archivo para debug
                        with open(log_file, 'a', encoding='utf-8') as log:
                            log.write(f"\n{'='*60}\n")
                            log.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S')} - Iniciando {nombre}\n")
                            log.write(f"{'='*60}\n")
                        
                        # Ejecutar sin capturar output para evitar bloqueos
                        with open(log_file, 'a', encoding='utf-8') as log:
                            result = subprocess.run(
                                ["python", scraper_path], 
                                stdout=log, 
                                stderr=log, 
                                timeout=2400,  # 40 min timeout
                                cwd=r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL"
                            )
                        
                        if result.returncode == 0:
                            print(f"✅ {nombre} completado")
                        else:
                            print(f"⚠️ {nombre} terminó con código {result.returncode} - ver scraper_log.txt")
                            
                    except subprocess.TimeoutExpired:
                        print(f"⚠️ {nombre} timeout (40 min)")
                        with open(log_file, 'a', encoding='utf-8') as log:
                            log.write(f"\n❌ TIMEOUT después de 40 minutos\n")
                    except Exception as e:
                        print(f"⚠️ Error en {nombre}: {e}")
                        with open(log_file, 'a', encoding='utf-8') as log:
                            log.write(f"\n❌ ERROR: {e}\n")
                
                # SOLO marcar que ya se ejecutaron los scrapers hoy
                recarga_hecha_hoy = True
                print("   ✅ Scrapers completados. CSVs actualizados.")
                
                # RECARGAR los CSVs en memoria para tenerlos listos para la siguiente jornada
                print("   🔄 Recargando CSVs en memoria...")
                alltime_data = recargar_alltime_data()
                franchise_data = cargar_todos_franchise_data()
                
                # CRÍTICO: Actualizar anteriores con los CSVs nuevos (sin stats de partidos)
                # Esto evita falsas notificaciones al comparar datos viejos con nuevos
                anteriores = {stat: combinar_datos(alltime_data[stat], {}, stat) for stat in STATS}
                
                # También reinicializar valores de franchise
                inicializar_franchise_valores_iniciales(franchise_data)
                
                # Marcar que debemos ignorar partidos finalizados hasta la siguiente jornada
                ignorar_partidos_finalizados = True
                
                print("   ✅ CSVs recargados. Partidos finalizados serán ignorados hasta nueva jornada.")

            print(f"\n{'='*60}")
            print(f"Refresh: Actualizando... {now.strftime('%H:%M:%S')}")
            print(f"{'='*60}")

            stats_partido = obtener_stats_partido()
            actualizar_estado_partidos_en_vivo()  # ← NUEVA LÍNEA
            print(f"   {len(stats_partido)} jugadores con stats hoy → Procesando rankings...")

            for stat_key in STATS:
                jugadores_250 = alltime_data[stat_key]
                actual = combinar_datos(jugadores_250, stats_partido, stat_key)
                cambios = detectar_adelantamientos_o_milestones(anteriores.get(stat_key, []), actual, stat_key)
                mostrar_ranking_optimizado(actual, stat_key, stats_partido, cambios)

                tabla_web = []
                for r in actual:
                    norm_name = r['norm_name']
                    if norm_name not in stats_partido:
                        continue
                    cambio = cambios.get(norm_name, {})
                    diff = cambio.get('diff', '')
                    diff_str = str(diff) if diff not in ('', 0, None) else ''
                    tabla_web.append({
                        "Rank": r['rank_actual'],
                        "Nombre": r['nombre'],
                        "Team": stats_partido.get(r['norm_name'], {}).get('team', ''),
                        "Hoy": r[f'{stat_key}_partido'],
                        "Total": r['total'],
                        "Diff": diff_str,
                        "Antece": str(cambio.get('antece', ''))[:30],
                        "Nota": cambio.get('nota', ''),
                        "game_status": stats_partido.get(r['norm_name'], {}).get('game_status', 'not_started')
                    })
                if tabla_web:
                    tablas_activos_global[stat_key] = tabla_web

                anteriores[stat_key] = actual
                
                # Actualizar alltime_data con los datos actuales para guardar
                alltime_data_actualizado[stat_key] = actual

            # === PROCESAR FRANCHISE RANKINGS ===
            for stat_key in FRANCHISE_STATS:
                franchise_data = detectar_adelantamientos_franchise(franchise_data, stats_partido, stat_key)

            # === GENERAR TABLAS DE TEAMS PARA LA WEB ===
            global tablas_teams_global
            tablas_teams_nuevas = {}  # Usar variable temporal
            stat_abbrev = {'pts': 'PTS', 'trb': 'REB', 'ast': 'AST', 'stl': 'STL', 'blk': 'BLK', 'fg3': '3PM', 'g': 'G'}
            
            for stat_key in FRANCHISE_STATS:
                if stat_key not in franchise_data:
                    continue
                
                step = FRANCHISE_STATS[stat_key]['milestone_step']
                
                for team_br, players in franchise_data[stat_key].items():
                    for player in players:
                        norm_name = player['norm_name']
                        if norm_name not in stats_partido:
                            continue  # Solo jugadores activos hoy
                        
                        player_stats = stats_partido[norm_name]
                        
                        # IMPORTANTE: Verificar que el equipo actual del jugador coincide
                        team_api = player_stats.get('team', '')
                        team_actual_br = TEAM_ABBREV_MAP.get(team_api, team_api)
                        if team_actual_br != team_br:
                            continue  # Este jugador ya no juega para este equipo
                        
                        # Obtener stats de hoy (puede ser 0)
                        hoy = player_stats.get(stat_key, 0)
                        
                        # Inicializar equipo si no existe
                        if team_br not in tablas_teams_nuevas:
                            tablas_teams_nuevas[team_br] = []
                        
                        # Calcular diff y antece (igual que en global: milestone vs rival)
                        rank = player['rank']
                        total = player['total']
                        
                        # Calcular próximo milestone
                        next_milestone = ((total // step) + 1) * step
                        dist_milestone = next_milestone - total
                        
                        # Calcular distancia al rival superior
                        diff_rival = None
                        antece_rival = ''
                        if rank > 1:
                            for p2 in players:
                                if p2['rank'] == rank - 1:
                                    diff_rival = p2['total'] - total
                                    antece_rival = p2['nombre'][:30]
                                    break
                        
                        # Elegir el menor: milestone o rival
                        if rank == 1:
                            diff = dist_milestone
                            antece = str(next_milestone)
                        elif diff_rival is not None and dist_milestone < diff_rival:
                            diff = dist_milestone
                            antece = str(next_milestone)
                        else:
                            diff = diff_rival if diff_rival is not None else ''
                            antece = antece_rival
                        
                        tablas_teams_nuevas[team_br].append({
                            "Rank": rank,
                            "Nombre": player['nombre'],
                            "Stat": stat_abbrev.get(stat_key, stat_key.upper()),
                            "Hoy": hoy,
                            "Total": total,
                            "Diff": str(diff) if diff not in ('', None) else '',
                            "Antece": antece,
                            "game_status": player_stats.get('game_status', 'not_started')
                        })
            
            # Solo actualizar tablas_teams_global si hay datos nuevos
            # Esto preserva los datos de la última jornada cuando no hay partidos
            if tablas_teams_nuevas:
                tablas_teams_global = tablas_teams_nuevas

            # ❌ YA NO GUARDAMOS CSVs AQUÍ - Los CSVs solo se actualizan via scraper a las 14:00
            # Los datos en memoria incluyen las stats del día, pero los CSVs mantienen el valor de inicio de jornada
            # if stats_partido:
            #     guardar_todos_los_csv(alltime_data_actualizado)
            #     guardar_todos_franchise_csv(franchise_data)

            generar_web_en_vivo()
            print(f"WEB 100% ACTUALIZADA → {len(tablas_activos_global)} tablas | {len(notificaciones_del_dia)} notifs global | {len(notificaciones_franchise_del_dia)} notifs franchise")

            time.sleep(90)
        except Exception as e:
            print(f"Error CRÍTICO en bucle de monitoreo: {e}")
            time.sleep(60)

# === MANEJO DE GIT ===
def git_commit_and_push():
    original_cwd = os.getcwd()
    try:
        # 1. Nos movemos a la carpeta del repositorio Git
        os.chdir(GIT_REPO_PATH)
        print(f"\n⚙️ Ejecutando Git en: {GIT_REPO_PATH}")

        # 2. Add (Añadir solo el archivo data.json)
        print("   > Git Add data.json...")
        subprocess.run(["git", "add", "."], check=True, capture_output=True)

        # 3. Checkear si hay cambios para commitear (evita commits vacíos)
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not result.stdout.strip():
            print("   > No hay cambios nuevos que commitear. Saltando Commit/Push.")
            return

        # 4. Commit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Actualización automatizada de datos en vivo - {timestamp}"
        print(f"   > Git Commit: {commit_message}")
        subprocess.run(["git", "commit", "-m", commit_message], check=True, capture_output=True)

        # 5. PULL: Integrar cambios remotos antes de hacer push (ESTO SOLUCIONA EL ERROR 'rejected')
        print(f"   > Git Pull (fetch and merge)...")
        subprocess.run(["git", "pull", "origin", GIT_BRANCH], check=True, capture_output=True)
        print("   ✅ Git Pull exitoso (o no había nada que descargar)")
        
        # 6. Push
        print(f"   > Git Push a rama {GIT_BRANCH}...")
        subprocess.run(["git", "push", "origin", GIT_BRANCH], check=True, capture_output=True)
        print("   ✅ Git Push exitoso")

    except subprocess.CalledProcessError as e:
        print("   ❌ ERROR CRÍTICO en Git Commit/Pull/Push")
        # Muestra el error exacto de Git
        print(f"   Stdsalida: {e.stdout.decode().strip()}")
        print(f"   Stderror: {e.stderr.decode().strip()}")
        
    except Exception as e:
        print(f"   ❌ Error general en git: {e}")
    finally:
        # Siempre volvemos al directorio original
        os.chdir(original_cwd)

# === GENERAR WEB EN VIVO ===
def generar_web_en_vivo():
    global tablas_activos_global, notificaciones_del_dia, notificaciones_franchise_del_dia
    global notificaciones_historicas_global, notificaciones_historicas_franchise
    global _ultimo_hash_data

    print(f"[WEB-1] generar_web_en_vivo() llamada | notificaciones_del_dia = {len(notificaciones_del_dia)} | franchise = {len(notificaciones_franchise_del_dia)}")
  
    if not os.path.exists(WEB_FOLDER):
        os.makedirs(WEB_FOLDER)
    
    # 1. FIX: Comprobación de datos antes de generar el JSON.
    if not tablas_activos_global:
        # Aquí se "chiva" de que no hay datos.
        print("⚠️ ADVERTENCIA: 'tablas_activos_global' está vacío. Esto podría indicar un problema en la API de la NBA o que no hay partidos activos.")
    
    # 1.5 GUARDAR NOTIFICACIONES HISTÓRICAS (TODAS, no solo 100)
    # Añadir las nuevas notificaciones al historial si no están ya
    for notif in notificaciones_del_dia:
        # Usar timestamp como identificador único
        if not any(h.get('timestamp') == notif.get('timestamp') and h.get('texto') == notif.get('texto') 
                   for h in notificaciones_historicas_global):
            notificaciones_historicas_global.append(notif)
    
    for notif in notificaciones_franchise_del_dia:
        if not any(h.get('timestamp') == notif.get('timestamp') and h.get('texto') == notif.get('texto') 
                   for h in notificaciones_historicas_franchise):
            notificaciones_historicas_franchise.append(notif)
    
    # Guardar históricas en archivo separado
    guardar_notificaciones_historicas(notificaciones_historicas_global, notificaciones_historicas_franchise)
    
    # 2. Preparar los datos (SIN cache_buster para comparación)
    data_para_comparar = {
        "tablas": tablas_activos_global,
        "tablas_teams": tablas_teams_global,
        "notificaciones": notificaciones_del_dia[-100:],
        "notificaciones_franchise": notificaciones_franchise_del_dia[-100:],
    }
    
    # Calcular hash de los datos relevantes
    import hashlib
    hash_actual = hashlib.md5(json.dumps(data_para_comparar, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    
    # Datos completos para guardar (con timestamp y cache_buster)
    data = {
        "ultima_actualizacion": datetime.now().strftime("%H:%M:%S"),
        "tablas": tablas_activos_global,
        "tablas_teams": tablas_teams_global,
        "notificaciones": notificaciones_del_dia[-100:],
        "notificaciones_franchise": notificaciones_franchise_del_dia[-100:],
        "total_notificaciones_historicas": len(notificaciones_historicas_global),
        "total_franchise_historicas": len(notificaciones_historicas_franchise),
        "cache_buster": int(time.time())
    }
    
    print(f"[WEB-1] generar_web_en_vivo() llamada | notificaciones_del_dia = {len(notificaciones_del_dia)}")
    
    # 3. Escribir el archivo data.json
    try:
        with open(WEB_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"✅ Archivo {WEB_DATA_FILE} generado/actualizado correctamente.")
    except Exception as e:
        print(f"❌ ERROR al escribir data.json: {e}")
        return
    
    # 4. PUSH CONDICIONAL: Solo si hay cambios reales
    if PUSH_SOLO_SI_HAY_CAMBIOS:
        if _ultimo_hash_data == hash_actual:
            print("   ⏸️ Sin cambios en datos - Saltando push a GitHub")
        else:
            _ultimo_hash_data = hash_actual
            print("   📤 Cambios detectados - Ejecutando push a GitHub")
            git_commit_and_push()
            print("[WEB-3] Git commit & push ejecutado tras actualizar data.json")
    else:
        # Comportamiento anterior: push siempre
        git_commit_and_push()
        print("[WEB-3] Git commit & push ejecutado tras actualizar data.json")
    
    # === ENVIAR NOTIFICACIONES ONESIGNAL PENDIENTES (después del git push) ===
    global onesignal_pendientes
    if onesignal and onesignal_pendientes:
        print(f"[ONESIGNAL] Enviando {len(onesignal_pendientes)} notificaciones pendientes...")
        for notif in onesignal_pendientes:
            try:
                scope = notif.get("scope", "global")
                
                if scope == "global":
                    onesignal.enviar_notificacion_global(
                        titulo=notif["titulo"],
                        mensaje=notif["mensaje"],
                        stat=notif.get("stat", "pts"),
                        alert_type=notif.get("tipo", "overtake"),
                        imagen_url=notif.get("imagen_url"),
                        ranking=notif.get("ranking")
                    )
                else:  # teams/franchise
                    onesignal.enviar_notificacion_teams(
                        titulo=notif["titulo"],
                        mensaje=notif["mensaje"],
                        stat=notif.get("stat", "pts"),
                        alert_type=notif.get("tipo", "overtake"),
                        team=notif.get("team"),
                        imagen_url=notif.get("imagen_url"),
                        ranking=notif.get("ranking")
                    )
            except Exception as e:
                print(f"[ONESIGNAL] Error enviando notificación: {e}")
        onesignal_pendientes = []  # Limpiar cola
        print("[ONESIGNAL] Cola de notificaciones vaciada")

def cleanup_old_collages(days=2):
    """Elimina collages en WEB_FOLDER/collages más antiguos que `days` días."""
    try:
        collages_dir = os.path.join(WEB_FOLDER, 'collages')
        if not os.path.exists(collages_dir):
            return
        cutoff = time.time() - days * 86400
        for fn in os.listdir(collages_dir):
            path = os.path.join(collages_dir, fn)
            try:
                if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    print(f"Removed old collage: {path}")
            except Exception:
                pass
    except Exception as e:
        print(f"Error cleaning collages: {e}")

if __name__ == "__main__":
    for stat in STATS:
        cargar_jugadores_250_por_stat(stat)
    main()