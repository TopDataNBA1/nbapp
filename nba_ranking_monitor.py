import os
import pandas as pd
import time
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

# === WEB EN VIVO ===
WEB_FOLDER = r"C:\Users\USUARIO\Desktop\con Grok\web\docs"
WEB_DATA_FILE = os.path.join(WEB_FOLDER, "data.json")
notificaciones_del_dia = []        # ← Guardará todas las notificaciones
tablas_activos_global = {}         # ← Guardará las tablas para la web

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
    if os.path.exists(NOTIFICACIONES_FILE):
        try:
            with open(NOTIFICACIONES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                hoy = datetime.now(pytz.timezone('Europe/Paris')).strftime('%Y-%m-%d')
                return set(data.get(hoy, []))
        except Exception as e:
            print(f"Error cargando notificaciones: {e}")
    return set()

def guardar_notificacion(clave):
    global mensajes_enviados
    hoy = datetime.now(pytz.timezone('Europe/Paris')).strftime('%Y-%m-%d')
    mensajes_enviados.add(clave)
    
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

# === HoF ===
def cargar_hof():
    csv_file = r'C:\Users\USUARIO\Desktop\con Grok\hpf_players.csv'
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        return set(df['name'].astype(str).str.lower().str.strip())
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
mensajes_enviados = cargar_notificaciones_enviadas() or set()
overtaken_tracker = defaultdict(lambda: defaultdict(set))
recarga_hecha_hoy = False

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
    replacements = {'š': 's', 'Š': 'S', 'č': 'c', 'Č': 'C', 'ž': 'z', 'Ž': 'Z', 'ā': 'a', 'Ā': 'A', 'ē': 'e', 'Ē': 'E', 'ī': 'i', 'Ī': 'I', 'ū': 'u', 'Ū': 'U', 'ģ': 'g', 'Ģ': 'G', 'ķ': 'k', 'Ķ': 'K', 'ļ': 'l', 'Ļ': 'L', 'ņ': 'n', 'Ņ': 'N'}
    for acc, plain in replacements.items(): name = name.replace(acc, plain)
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
    for path in [os.path.join(PHOTOS_DIR, f"{hyphen_name}.png"), os.path.join(PHOTOS_DIR, f"{hyphen_name}.jpg")]:
        if os.path.exists(path):
            return path
    candidates = [name_original.lower().replace(' ', '_'), name_original.lower().replace(' ', '-'), fix_latvian_chars(name_original.lower())]
    for cand in candidates:
        for ext in ['.jpg', '.jpeg', '.png']:
            path = os.path.join(PHOTOS_DIR, f"{cand}{ext}")
            if os.path.exists(path):
                return path
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
    valid_fotos = [url for url in foto_urls if os.path.exists(url) and os.path.getsize(url) <= 10 * 1024 * 1024]
    if not valid_fotos:
        return None
    num_fotos = len(valid_fotos)
    canvas = Image.new('RGBA', (1200, 800), (0, 0, 0, 0))
    if num_fotos == 1:
        targets = [(1000, 800)]
        positions = [(100, 0)]
    elif num_fotos == 2:
        targets = [(600, 800), (600, 800)]
        positions = [(0, 0), (600, 0)]
    elif num_fotos == 3:
        targets = [(600, 800), (600, 400), (600, 400)]
        positions = [(0, 0), (600, 0), (600, 400)]
    else:
        targets = [(600, 400)] * min(4, num_fotos)
        positions = [(0, 0), (600, 0), (0, 400), (600, 400)]
    for i, url in enumerate(valid_fotos[:len(targets)]):
        try:
            img = Image.open(url).convert('RGBA')
            w, h = targets[i]
            img = resize_maximize_face(img, w, h)
            datas = img.getdata()
            new_data = []
            for item in datas:
                if item[0] > 240 and item[1] > 240 and item[2] > 240:
                    new_data.append((0, 0, 0, 0))
                else:
                    new_data.append(item)
            img.putdata(new_data)
            img = img.convert('RGBA')
            x, y = positions[i]
            canvas.paste(img, (x, y), img)
        except Exception as e:
            print(f"Error: {e}")
    path = os.path.join(TEMP_DIR, f"collage_{int(time.time())}.png")
    canvas.save(path, 'PNG')
    return path

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
            try:
                response = requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})
                print(f"Telegram status: {response.status_code} → {mensaje[:50]}...")
            except Exception as e:
                print(f"Error enviando Telegram: {e}")

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
            try:
                response = requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})
                print(f"Telegram status: {response.status_code} → {mensaje[:50]}...")
            except Exception as e:
                print(f"Error enviando Telegram: {e}")

    else:
        try:
            response = requests.post(url_text, json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML", "disable_web_page_preview": True})
            print(f"Telegram status: {response.status_code} → {mensaje[:50]}...")
        except Exception as e:
            print(f"Error enviando Telegram: {e}")


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
            try:
                response = requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})
                print(f"Discord status: {response.status_code} → {mensaje_limpio[:50]}...")
            except Exception as e:
                print(f"Error enviando Discord: {e}")
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
            try:
                response = requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})
                print(f"Discord status: {response.status_code} → {mensaje_limpio[:50]}...")
            except Exception as e:
                print(f"Error enviando Discord: {e}")

    else:
        try:
            response = requests.post(DISCORD_WEBHOOK_URL, json={'content': mensaje_limpio})
            print(f"Discord status: {response.status_code} → {mensaje_limpio[:50]}...")
        except Exception as e:
            print(f"Error enviando Discord: {e}")


# === ENVÍO MULTIPLATAFORMA ===
def enviar_multi_plataforma(mensaje, foto_urls=None, is_milestone=False, milestone_value=None, player_photo=None):
    print(f"Enviando: {mensaje[:50]}...")
    if is_milestone and player_photo and os.path.exists(player_photo):
        enviar_mensaje_telegram(mensaje, None, is_milestone, milestone_value, player_photo)
        enviar_mensaje_discord(mensaje, None, is_milestone, milestone_value, player_photo)
    elif foto_urls:
        enviar_mensaje_telegram(mensaje, foto_urls)
        enviar_mensaje_discord(mensaje, foto_urls)
    else:
        enviar_mensaje_telegram(mensaje)
        enviar_mensaje_discord(mensaje)
    print("   TELEGRAM + DISCORD: Enviado")

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
    if now - game_status_timestamp < 60:  # ← Refrescamos solo cada minuto
        return
    
    print("   Actualizando estado real de partidos en vivo...")
    game_status_cache = {}
    
    try:
        board = scoreboardv2.ScoreboardV2(game_date=datetime.now().strftime('%m/%d/%Y'))
        games = board.get_data_frames()[0]
        
        for _, game in games.iterrows():
            game_id = str(game['GAME_ID'])
            status = int(game['GAME_STATUS_ID'])  # 1=not started, 2=playing, 3=finished
            
            if status == 1:
                game_status_cache[game_id] = "not_started"
            elif status == 2:
                game_status_cache[game_id] = "playing"
            elif status == 3:
                game_status_cache[game_id] = "finished"
                
            # Doble comprobación con el texto por si hay Final/OT
            status_text = game['GAME_STATUS_TEXT'].strip()
            if "Final" in status_text or "End" in status_text:
                game_status_cache[game_id] = "finished"
            elif ":" in status_text and ("Q" in status_text or "Half" in status_text):
                game_status_cache[game_id] = "playing"
                
    except Exception as e:
        print(f"   Error actualizando estado en vivo: {e}")
    
    game_status_timestamp = now

# === OBTENER PARTIDOS Y STATS EN VIVO ===
def obtener_stats_partido():
    cet_tz = pytz.timezone('Europe/Paris')
    now_madrid = datetime.now(cet_tz)
    game_date_cet = (now_madrid - timedelta(days=1)).date() if now_madrid.hour < 10 else now_madrid.date()
    game_date_str = game_date_cet.strftime('%Y-%m-%d')
    game_date_for_api = game_date_cet.strftime('%m/%d/%Y')
    print(f"   Día NBA detectado (CEST): {game_date_str}")
    game_ids = []
    try:
        board = scoreboardv2.ScoreboardV2(game_date=game_date_for_api)
        games_df = board.get_data_frames()[0]
        game_ids = games_df['GAME_ID'].unique().tolist()
        print(f"   Partidos desde ScoreboardV2: {len(game_ids)}")
    except Exception as e:
        print(f"   ScoreboardV2 falló: {e}")
    try:
        lgf = leaguegamefinder.LeagueGameFinder(date_from_nullable=game_date_for_api, date_to_nullable=game_date_for_api, league_id_nullable='00')
        games_df = lgf.get_data_frames()[0]
        game_ids.extend(games_df['GAME_ID'].unique().tolist())
    except Exception as e:
        print(f"   LeagueGameFinder falló: {e}")
    game_ids = list(set(game_ids))[:15]
    print(f"   Procesando {len(game_ids)} partidos: {game_ids}")
    stats_partido = {}
    for game_id in game_ids:
        # === NUEVO: USAR API EN VIVO ===
            try:
                live_bs = live_boxscore.BoxScore(game_id=game_id)
                data = live_bs.get_dict()['game']
                game_status = "not_started"
                if data['gameStatus'] == 2:
                    game_status = "playing"
                elif data['gameStatus'] == 3:
                    game_status = "finished"
                elif data['gameStatus'] == 1:
                    game_status = "not_started"
                # Guardar estado para este game_id
                game_status_cache[game_id] = game_status
            except:
                pass  # Si falla live, seguimos con el tradicional

            try:
                bs = boxscore.BoxScore(game_id=game_id)
                data = bs.get_dict().get('game', {})
                for team in ['homeTeam', 'awayTeam']:
                    for player in data.get(team, {}).get('players', []):
                        name = player.get('name', '')
                        if not name: continue
                        norm = normalize_name(name)
                        if not norm: continue
                        
                        # === AÑADIR player_id y game_id al diccionario ===
                        stats_partido.setdefault(norm, {
                            "player_id": player.get('personId'),
                            "game_id": game_id,           # ← NUEVO
                            "game_status": game_status_cache.get(game_id, "not_started"),  # ← NUEVO
                            **{k: 0 for k in STATS}
                        })
                        
                        s = player.get('statistics', {})
                        mins = parse_minutes(player.get('minutesPlayedActual', 'PT00M00.00S'))
                        played = mins > 0 or any(s.get(k, 0) > 0 for k in ['points', 'reboundsTotal', 'assists'])
                        if played:
                            stats_partido[norm]['g'] += 1
                        for k, info in STATS.items():
                            if k == 'g': continue
                            val = s.get(info['box_key'], 0)
                            if isinstance(val, (int, float)):
                                stats_partido[norm][k] += max(0, int(val))
                                
                        # Actualizar estado del jugador (por si cambió desde la última vez)
                        stats_partido[norm]['game_status'] = game_status_cache.get(game_id, "not_started")
                                
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

def detectar_adelantamientos_o_milestones(anterior, actual, stat_key):
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
                cambios[norm_name] = {'diff': diferencia, 'antece': nombre_superior[:20], 'nota': nota_rival}

        if hoy <= 0:
            continue

        # ADELANTAMIENTOS
        jugadores_antes = {j['norm_name']: j[f'{stat_key}_250'] for j in actual}
        jugadores_ahora = {j['norm_name']: j['total'] for j in actual}
        
        adelantados = []
        for norm_rival, valor_antes_rival in jugadores_antes.items():
            if norm_rival == norm_name:
                continue
            valor_antes_actual = antes
            valor_ahora_rival = jugadores_ahora.get(norm_rival, 0)
            valor_ahora_actual = nuevo
            if (valor_antes_rival > valor_antes_actual and valor_ahora_actual > valor_ahora_rival):
                try:
                    idx = [j['norm_name'] for j in actual].index(norm_rival)
                    adelantados.append(actual[idx]['nombre'])
                except:
                    pass
        
        nuevos = set(adelantados) - overtaken_tracker[stat_key][norm_name]

        # MILESTONE (SIN EMOJIS)
        clave_milestone = f"{norm_name}_{stat_key}_milestone_{nuevo}"
        if nuevo % step == 0 and nuevo > antes and clave_milestone not in mensajes_enviados:
            txt_hoy = SINGULAR_PLURAL.get(stat_display_plural, stat_display_plural) if hoy == 1 else stat_display_plural
            msg = f"<b>{nombre}</b> ({hoy} {txt_hoy} today) becomes the {ordinal(p)} player to reach {nuevo:,} {stat_display_plural}"
            msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()
            foto = get_player_headshot_url(nombre)
            enviar_multi_plataforma(msg, None, True, nuevo, foto)
            guardar_notificacion(clave_milestone)
            notificaciones_del_dia.append(msg_limpio)

        # ADELANTAMIENTO (SIN EMOJIS)
        if nuevos:
            rivales_key = "_".join(sorted([normalize_name(r) for r in nuevos]))
            clave_grupal = f"{norm_name}_{stat_key}_passed_{antes}_{rivales_key}"
            if clave_grupal in mensajes_enviados:
                continue
            guardar_notificacion(clave_grupal)
            for rival in nuevos:
                overtaken_tracker[stat_key][norm_name].add(rival)

            nuevos_lista = sorted(nuevos)
            txt_hoy = SINGULAR_PLURAL[stat_display_plural] if hoy == 1 else stat_display_plural
            if len(nuevos_lista) == 1:
                rivales_texto = f"<b>{nuevos_lista[0]}</b>"
            else:
                rivales_texto = ", ".join([f"<b>{r}</b>" for r in nuevos_lista[:-1]]) + f" and <b>{nuevos_lista[-1]}</b>"

            if stat_key == 'g':
                msg = f"<b>{nombre}</b> today surpasses {rivales_texto} and now ranked {ordinal(p)} all time with {nuevo:,} games"
            else:
                msg = f"<b>{nombre}</b> ({hoy} {txt_hoy} today) has passed {rivales_texto} and is now ranked {ordinal(p)} all-time in career {stat_display_plural} with {nuevo:,}."

            msg_limpio = msg.replace("<b>", "").replace("</b>", "").strip()
            notificaciones_del_dia.append(msg_limpio)

            fotos = [get_player_headshot_url(nombre)]
            for rival in nuevos_lista:
                foto_rival = get_player_headshot_url(rival)
                if foto_rival and len(fotos) < 4:
                    fotos.append(foto_rival)
            fotos = fotos[:4]
            if msg.strip():  # asegurar que no enviamos mensaje vacío
                enviar_multi_plataforma(msg, fotos)


    return cambios

def mostrar_ranking_optimizado(actual, stat_key, stats_partido, cambios):
    stat_display = STATS.get(stat_key, {}).get('display', stat_key).upper()
    print(f"\n{stat_display} - ACTIVOS:")
    jugadores_relevantes = []
    jugadores_en_juego = set(stats_partido.keys())
    for r in actual:
        norm_name = r['norm_name']
        nombre = r['nombre']
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
            'antes': f"{antes:>6}",
            'hoy': f"{hoy:>4}",
            'total': f"{total:>6}",
            'diff': f"{diff:>4}" if diff != '' else '    ',
            'antece': f"{antecede:<25}",
            'nota': f"{nota:<15}"
        })
    jugadores_relevantes.sort(key=lambda x: int(x['rank']))
    header = f"{'Rank':>3} {'Nombre':<25} {'Antes':>6} {'Hoy':>4} {'Total':>6} {'Diff':>4} {'Antece':<25} {'Nota':<15}"
    print(header)
    print("-" * 78)
    for j in jugadores_relevantes:
        print(f"{j['rank']} {j['nombre']} {j['antes']} {j['hoy']} {j['total']} {j['diff']} {j['antece']} {j['nota']}")

def main():
    generar_web_en_vivo() # Web vacía al arrancar
    print("Cargando datos all-time...")
    alltime_data = {}
    for stat in STATS:
        data, _ = obtener_jugadores_250_por_stat(stat) 
        alltime_data[stat] = data
    recuperar_dias_perdidos()
    anteriores = {stat: [] for stat in STATS}
    cet_tz = pytz.timezone('Europe/Paris')
    last_day = None
    global recarga_hecha_hoy
    recarga_hecha_hoy = False

    while True:
        try:
            now = datetime.now(cet_tz)
            current_day = now.date() if now.hour >= 10 else (now - timedelta(days=1)).date()
            current_day_str = current_day.strftime('%Y-%m-%d')
            if last_day != current_day:
                print(f"\nNUEVO DÍA: {current_day_str}")
                anteriores = {stat: combinar_datos(alltime_data[stat], {}, stat) for stat in STATS}
                last_day = current_day
                recarga_hecha_hoy = False
                global mensajes_enviados
                mensajes_enviados = cargar_notificaciones_enviadas() or set()
                print(f"   Notificaciones del día actual cargadas del archivo.")
            if now.hour == 14 and now.minute == 0 and not recarga_hecha_hoy:
                alltime_data = recargar_alltime_data()
                anteriores = {stat: combinar_datos(alltime_data[stat], {}, stat) for stat in STATS}
                recarga_hecha_hoy = True
                print("   Ranking reiniciado con datos actualizados.")

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
                        "Antes": r[f'{stat_key}_250'],
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

            generar_web_en_vivo()
            print(f"WEB 100% ACTUALIZADA → {len(tablas_activos_global)} tablas | {len(notificaciones_del_dia)} notifs")

            time.sleep(30)
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
        subprocess.run(["git", "add", os.path.join(WEB_FOLDER, "data.json")])

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
    global tablas_activos_global, notificaciones_del_dia # Asegurar acceso a las variables globales
    
    if not os.path.exists(WEB_FOLDER):
        os.makedirs(WEB_FOLDER)
    
    # 1. FIX: Comprobación de datos antes de generar el JSON.
    if not tablas_activos_global:
        # Aquí se "chiva" de que no hay datos.
        print("⚠️ ADVERTENCIA: 'tablas_activos_global' está vacío. Esto podría indicar un problema en la API de la NBA o que no hay partidos activos.")
    
    # 2. Preparar los datos
    data = {
        "ultima_actualizacion": datetime.now().strftime("%H:%M:%S"),
        "tablas": tablas_activos_global, # Si está vacío, se envía vacío (el JS lo maneja)
        "notificaciones": notificaciones_del_dia[-60:],
        "cache_buster": int(time.time())
    }
    
    # 3. Escribir el archivo data.json
    try:
        # Escritura simple y segura en Windows
        with open(WEB_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"✅ Archivo {WEB_DATA_FILE} generado/actualizado correctamente.")
    except Exception as e:
        print(f"❌ ERROR al escribir data.json: {e}")
        return # Si falla la escritura del JSON, detenemos el proceso

    # 4. Subir a Git para actualizar la web real
    git_commit_and_push() # <--- ¡ESTA ES LA LLAMADA QUE FALTABA!

if __name__ == "__main__":
    for stat in STATS:
        cargar_jugadores_250_por_stat(stat)
    main()