import json, time
from flask import Flask, jsonify
from your_data_module import tablas_activos_global, notificaciones_del_dia  # tu script que genera los datos

app = Flask(__name__, static_folder="docs", template_folder="docs")

@app.route("/data.json")
def data():
    return jsonify({
        "ultima_actualizacion": time.strftime("%H:%M:%S"),
        "tablas": tablas_activos_global,
        "notificaciones": notificaciones_del_dia
    })

def update_data_loop():
    while True:
        # aquí actualizas tus datos, por ejemplo llamando a tu función principal
        generate_global_data()  # debes tener esta función que llene tablas_activos_global
        time.sleep(25)  # refresca cada 25 segundos

if __name__ == "__main__":
    import threading
    threading.Thread(target=update_data_loop, daemon=True).start()
    app.run(debug=True, port=8000)
