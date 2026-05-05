"""
Algoritmo de Berkeley - Nodo Maestro
=====================================
El maestro solicita la hora a todos los esclavos,
calcula el promedio y envía el ajuste a cada nodo.
Si el maestro cae, uno de los esclavos toma el control.
"""

import socket
import threading
import time
import datetime
import random
import logging
import json
import sys
import os

# ─── Configuración de logs ───────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[MAESTRO] %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/master.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("master")

# ─── Configuración de red ────────────────────────────────────────────────────
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000

SLAVES = [
    {"id": 1, "host": "127.0.0.1", "port": 5001},
    {"id": 2, "host": "127.0.0.1", "port": 5002},
]

SYNC_INTERVAL = 10   # segundos entre sincronizaciones
MAX_DIFF      = 5    # diferencia máxima aceptable en segundos (umbral de Berkeley)


def get_local_time() -> float:
    """Devuelve la hora local del maestro con una pequeña deriva aleatoria simulada."""
    return time.time() + random.uniform(-2, 2)


def request_time(slave: dict) -> float | None:
    """
    Solicita la hora actual al esclavo.
    Devuelve el timestamp del esclavo o None si no responde.
    """
    try:
        with socket.create_connection((slave["host"], slave["port"]), timeout=3) as s:
            msg = json.dumps({"cmd": "GET_TIME"})
            s.sendall(msg.encode())
            data = s.recv(1024).decode()
            resp = json.loads(data)
            slave_time = resp["time"]
            log.info(f"Hora recibida del Esclavo-{slave['id']}: "
                     f"{datetime.datetime.fromtimestamp(slave_time).strftime('%H:%M:%S.%f')}")
            return slave_time
    except Exception as e:
        log.warning(f"No se pudo contactar con Esclavo-{slave['id']}: {e}")
        return None


def send_adjustment(slave: dict, delta: float) -> bool:
    """
    Envía el ajuste de tiempo al esclavo.
    delta > 0 → el esclavo debe adelantar su reloj
    delta < 0 → el esclavo debe atrasar su reloj
    """
    try:
        with socket.create_connection((slave["host"], slave["port"]), timeout=3) as s:
            msg = json.dumps({"cmd": "ADJUST", "delta": delta})
            s.sendall(msg.encode())
            data = s.recv(1024).decode()
            resp = json.loads(data)
            if resp.get("status") == "OK":
                log.info(f"Esclavo-{slave['id']} ajustado en {delta:+.3f}s")
                return True
    except Exception as e:
        log.warning(f"Error enviando ajuste a Esclavo-{slave['id']}: {e}")
    return False


def berkeley_sync():
    """Ejecuta una ronda completa del algoritmo de Berkeley."""
    log.info("=" * 50)
    log.info("Iniciando ronda de sincronización Berkeley")

    master_time = get_local_time()
    log.info(f"Hora del Maestro: {datetime.datetime.fromtimestamp(master_time).strftime('%H:%M:%S.%f')}")

    # 1. Recolectar horas de todos los esclavos
    times = {"master": master_time}
    for slave in SLAVES:
        t = request_time(slave)
        if t is not None:
            times[f"slave_{slave['id']}"] = t

    if len(times) < 2:
        log.warning("Menos de 2 nodos respondieron. Sincronización cancelada.")
        return

    # 2. Calcular promedio (algoritmo de Berkeley)
    avg = sum(times.values()) / len(times)
    log.info(f"Hora promedio calculada: {datetime.datetime.fromtimestamp(avg).strftime('%H:%M:%S.%f')}")
    log.info(f"Nodos en el cálculo: {list(times.keys())}")

    # 3. Calcular y enviar ajustes
    master_delta = avg - master_time
    log.info(f"Ajuste propio del Maestro: {master_delta:+.3f}s")

    for slave in SLAVES:
        key = f"slave_{slave['id']}"
        if key in times:
            delta = avg - times[key]
            send_adjustment(slave, delta)

    log.info("Ronda de sincronización completada")
    log.info("=" * 50)


def handle_election(conn, addr):
    """
    Responde a mensajes de elección de nuevo maestro.
    Si un esclavo detecta que el maestro cayó, inicia elección.
    """
    try:
        data = conn.recv(1024).decode()
        msg = json.loads(data)
        if msg.get("cmd") == "ELECTION":
            log.info(f"Mensaje de elección recibido de {addr}")
            resp = json.dumps({"status": "ALIVE", "id": 0})  # id 0 = maestro
            conn.sendall(resp.encode())
    except Exception:
        pass
    finally:
        conn.close()


def listen_for_elections():
    """Servidor que escucha mensajes de elección de los esclavos."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(5)
    log.info(f"Maestro escuchando en {MASTER_HOST}:{MASTER_PORT}")
    while True:
        try:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_election, args=(conn, addr), daemon=True)
            t.start()
        except Exception as e:
            log.error(f"Error en servidor de elecciones: {e}")
            break


def main():
    log.info("★ Nodo MAESTRO arrancado ★")

    # Hilo para escuchar mensajes de elección
    t = threading.Thread(target=listen_for_elections, daemon=True)
    t.start()

    # Espera inicial para que los esclavos arranquen
    log.info("Esperando 3 segundos a que los esclavos estén listos...")
    time.sleep(3)

    try:
        while True:
            berkeley_sync()
            log.info(f"Próxima sincronización en {SYNC_INTERVAL}s...")
            time.sleep(SYNC_INTERVAL)
    except KeyboardInterrupt:
        log.info("Maestro detenido manualmente.")


if __name__ == "__main__":
    main()