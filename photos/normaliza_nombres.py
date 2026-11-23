import os
from typing import Optional, Tuple

# === CONFIGURACIÓN ===
CARPETA_BUSCADAS = r"C:\Users\USUARIO\Desktop\con Grok\GLOBAL\photos_merged\16"
SIMULACION = False # ¡IMPORTANTE! Cambiar a False para aplicar cambios reales

# === LÓGICA DE NORMALIZACIÓN (para minúsculas y guiones) ===
def normalizar_formato_simple(nombre_archivo: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Normaliza el nombre del archivo según las reglas:
    1. Convierte todo a minúsculas.
    2. Reemplaza los espacios (' ') por guiones medios ('-').
    Ejemplo: Lee Mayberry.jpg -> lee-mayberry.jpg
    """
    nombre_base, extension = os.path.splitext(nombre_archivo)
    
    # 1. Convertir todo a minúsculas
    nombre_base_minusculas = nombre_base.lower()
    
    # 2. Reemplazar espacios por guiones medios
    nuevo_nombre_base = nombre_base_minusculas.replace(' ', '-')
    
    # 3. Se asegura de no renombrar si el nombre ya está normalizado
    if nombre_base + extension == nuevo_nombre_base + extension:
        return None, "El nombre ya está normalizado (minúsculas y guiones)."
    
    return nuevo_nombre_base + extension, None

# === FUNCIÓN PRINCIPAL ===
def simular_normalizacion():
    print(f"--- INICIO DE LA NORMALIZACIÓN (MINÚSCULAS Y GUIONES) ---")
    print(f"Carpeta de trabajo: {CARPETA_BUSCADAS}\n")

    # Nota: Se ha cambiado el nombre de la función aquí para reflejar la nueva lógica.
    funcion_normalizacion = normalizar_formato_simple 

    if not os.path.isdir(CARPETA_BUSCADAS):
        print(f"Error: La carpeta no existe o la ruta es incorrecta: {CARPETA_BUSCADAS}")
        return

    archivos_renombrados = 0
    archivos_ignorados = 0
    archivos_ya_normalizados = 0

    for nombre_archivo in os.listdir(CARPETA_BUSCADAS):
        ruta_antigua = os.path.join(CARPETA_BUSCADAS, nombre_archivo)
        
        # Ignorar directorios
        if not os.path.isfile(ruta_antigua):
            continue

        nuevo_nombre_full, error_msg = funcion_normalizacion(nombre_archivo)
        
        if error_msg:
            if "El nombre ya está normalizado" in error_msg:
                archivos_ya_normalizados += 1
            else:
                archivos_ignorados += 1
            continue
        
        # --- SIMULACIÓN (Log) ---
        archivos_renombrados += 1

        # --- ACCIÓN REAL (solo si SIMULACION = False) ---
        if SIMULACION:
            print(f"  [SIMULADO] '{nombre_archivo}' -> '{nuevo_nombre_full}'")
        else:
            ruta_nueva = os.path.join(CARPETA_BUSCADAS, nuevo_nombre_full)
            
            # Prevenir renombrar si ya existe el archivo de destino (por si tiene duplicados)
            if os.path.exists(ruta_nueva):
                print(f"  [ERROR REAL] Ya existe '{nuevo_nombre_full}'. No se renombró '{nombre_archivo}'.")
                continue
            
            try:
                os.rename(ruta_antigua, ruta_nueva)
                print(f"  [RENOMBRADO] '{nombre_archivo}' -> '{nuevo_nombre_full}'")
            except Exception as e:
                print(f"  [ERROR REAL] No se pudo renombrar '{nombre_archivo}': {e}")


    print("\n--- RESUMEN DE LA OPERACIÓN ---")
    if SIMULACION:
        print(f"SIMULACIÓN COMPLETA. Se renombrarían {archivos_renombrados} archivos.")
        print(f"Archivos ya normalizados (no requieren cambio): {archivos_ya_normalizados}")
        print("Para aplicar los cambios reales, cambia 'SIMULACION = True' a 'False' y ejecuta de nuevo.")
    else:
        print(f"PROCESO COMPLETO. {archivos_renombrados} archivos renombrados.")
        print(f"Archivos ya normalizados: {archivos_ya_normalizados}")

# === EJECUCIÓN ===
if __name__ == "__main__":
    simular_normalizacion()