"""
Algoritmo de Berkeley - Nodo Esclavo
======================================
Responde con su hora local al maestro y aplica
el ajuste recibido. Si el maestro cae, inicia
una elección para elegir un nuevo coordinador.
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

# ─── Argumentos ──────────────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print("Uso: python slave.py <SLAVE_ID>")
    print("     SLAVE_ID: 1 ó 2")
    sys.exit(1)

SLAVE_ID   = int(sys.argv[1])
SLAVE_PORT = 5000 + SLAVE_ID          # Puerto: 5001 ó 5002
SLAVE_HOST = "127.0.0.1"

# ─── Configuración de logs ───────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format=f"[ESCLAVO-{SLAVE_ID}] %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"logs/slave_{SLAVE_ID}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(f"slave_{SLAVE_ID}")

# ─── Configuración general ───────────────────────────────────────────────────
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000

# Todos los nodos conocidos (para elección)
ALL_NODES = [
    {"id": 0, "host": "127.0.0.1", "port": 5000, "role": "master"},
    {"id": 1, "host": "127.0.0.1", "port": 5001, "role": "slave"},
    {"id": 2, "host": "127.0.0.1", "port": 5002, "role": "slave"},
]

HEARTBEAT_INTERVAL = 6   # segundos entre comprobaciones del maestro
MASTER_TIMEOUT     = 4   # segundos de espera para considerar maestro caído

# ─── Estado del esclavo ──────────────────────────────────────────────────────
state = {
    "clock_offset": random.uniform(-10, 10),   # Deriva simulada en segundos
    "is_master": False,
    "current_master_id": 0,
}
state_lock = threading.Lock()


def local_time() -> float:
    """Hora local del esclavo (con la deriva aplicada)."""
    with state_lock:
        return time.time() + state["clock_offset"]


def adjust_clock(delta: float):
    """Ajusta el reloj sumando delta al offset actual."""
    with state_lock:
        old = state["clock_offset"]
        state["clock_offset"] += delta
        new = state["clock_offset"]
    log.info(f"Reloj ajustado: offset {old:+.3f}s → {new:+.3f}s (delta={delta:+.3f}s)")


# ─── Servidor del esclavo ────────────────────────────────────────────────────

def handle_connection(conn, addr):
    """Atiende una conexión entrante (del maestro u otro esclavo)."""
    try:
        data = conn.recv(1024).decode()
        msg = json.loads(data)
        cmd = msg.get("cmd")

        if cmd == "GET_TIME":
            t = local_time()
            log.info(f"Hora solicitada → {datetime.datetime.fromtimestamp(t).strftime('%H:%M:%S.%f')}")
            resp = json.dumps({"time": t})
            conn.sendall(resp.encode())

        elif cmd == "ADJUST":
            delta = msg.get("delta", 0.0)
            adjust_clock(delta)
            conn.sendall(json.dumps({"status": "OK"}).encode())

        elif cmd == "ELECTION":
            # Responde si está vivo (protocolo bully)
            log.info(f"Mensaje de elección recibido de {addr}")
            conn.sendall(json.dumps({"status": "ALIVE", "id": SLAVE_ID}).encode())

        elif cmd == "NEW_MASTER":
            new_id = msg.get("master_id")
            with state_lock:
                state["current_master_id"] = new_id
                state["is_master"] = (new_id == SLAVE_ID)
            log.info(f"Nuevo maestro elegido: Nodo-{new_id}")
            conn.sendall(json.dumps({"status": "OK"}).encode())

        elif cmd == "GET_TIME_FOR_SYNC":
            # Cuando este esclavo es maestro y sincroniza a los demás
            t = local_time()
            conn.sendall(json.dumps({"time": t}).encode())

    except Exception as e:
        log.error(f"Error manejando conexión de {addr}: {e}")
    finally:
        conn.close()


def run_server():
    """Arranca el servidor TCP del esclavo."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SLAVE_HOST, SLAVE_PORT))
    server.listen(5)
    log.info(f"Esclavo-{SLAVE_ID} escuchando en {SLAVE_HOST}:{SLAVE_PORT}")
    while True:
        try:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
            t.start()
        except Exception as e:
            log.error(f"Error en servidor: {e}")
            break


# ─── Detección de caída del maestro y elección ───────────────────────────────

def is_master_alive() -> bool:
    """Comprueba si el maestro actual responde."""
    with state_lock:
        master_id   = state["current_master_id"]
        i_am_master = state["is_master"]

    if i_am_master:
        return True   # Soy el maestro, no me compruebo a mí mismo

    master_node = next((n for n in ALL_NODES if n["id"] == master_id), None)
    if master_node is None:
        return False

    try:
        with socket.create_connection(
            (master_node["host"], master_node["port"]), timeout=MASTER_TIMEOUT
        ) as s:
            s.sendall(json.dumps({"cmd": "ELECTION"}).encode())
            data = s.recv(1024).decode()
            resp = json.loads(data)
            return resp.get("status") == "ALIVE"
    except Exception:
        return False


def send_to_node(node: dict, msg: dict) -> dict | None:
    """Envía un mensaje a un nodo y devuelve la respuesta."""
    try:
        with socket.create_connection((node["host"], node["port"]), timeout=2) as s:
            s.sendall(json.dumps(msg).encode())
            data = s.recv(1024).decode()
            return json.loads(data)
    except Exception:
        return None


def start_election():
    """
    Algoritmo Bully para elección de nuevo maestro.
    El nodo con mayor ID que responda gana la elección.
    """
    log.warning("¡Maestro caído! Iniciando proceso de elección...")

    higher_nodes = [n for n in ALL_NODES if n["id"] > SLAVE_ID]
    someone_higher_alive = False

    for node in higher_nodes:
        resp = send_to_node(node, {"cmd": "ELECTION"})
        if resp and resp.get("status") == "ALIVE":
            log.info(f"Nodo-{node['id']} responde con mayor prioridad, él tomará el control.")
            someone_higher_alive = True
            break

    if not someone_higher_alive:
        # Soy el nodo de mayor ID disponible → me convierto en maestro
        log.info(f"★ Esclavo-{SLAVE_ID} se convierte en el nuevo MAESTRO ★")
        with state_lock:
            state["is_master"]         = True
            state["current_master_id"] = SLAVE_ID

        # Notificar a todos los demás nodos
        other_nodes = [n for n in ALL_NODES if n["id"] != SLAVE_ID]
        for node in other_nodes:
            send_to_node(node, {"cmd": "NEW_MASTER", "master_id": SLAVE_ID})

        # Arrancar sincronización como nuevo maestro
        t = threading.Thread(target=run_as_master, daemon=True)
        t.start()


def run_as_master():
    """
    Lógica de sincronización que ejecuta el esclavo
    cuando asume el rol de maestro (elección Bully).
    """
    SYNC_INTERVAL = 10
    log.info("Asumiendo tareas de sincronización como nuevo maestro.")

    while True:
        with state_lock:
            if not state["is_master"]:
                break

        log.info("─" * 40)
        log.info("[NUEVO MAESTRO] Iniciando ronda de sincronización")

        my_time = local_time()
        times   = {SLAVE_ID: my_time}

        other_slaves = [n for n in ALL_NODES if n["id"] != SLAVE_ID and n["role"] == "slave"]
        for node in other_slaves:
            resp = send_to_node(node, {"cmd": "GET_TIME_FOR_SYNC"})
            if resp and "time" in resp:
                times[node["id"]] = resp["time"]
                log.info(f"Hora de Esclavo-{node['id']}: "
                         f"{datetime.datetime.fromtimestamp(resp['time']).strftime('%H:%M:%S.%f')}")

        if len(times) >= 1:
            avg = sum(times.values()) / len(times)
            log.info(f"Hora promedio: {datetime.datetime.fromtimestamp(avg).strftime('%H:%M:%S.%f')}")

            # Ajuste propio
            adjust_clock(avg - my_time)

            # Ajuste a los demás
            for node in other_slaves:
                if node["id"] in times:
                    delta = avg - times[node["id"]]
                    send_to_node(node, {"cmd": "ADJUST", "delta": delta})

        log.info("[NUEVO MAESTRO] Ronda completada")
        time.sleep(SYNC_INTERVAL)


def heartbeat_loop():
    """Comprueba periódicamente si el maestro sigue vivo."""
    time.sleep(5)   # Espera inicial antes de monitorizar
    while True:
        with state_lock:
            i_am_master = state["is_master"]

        if not i_am_master:
            if not is_master_alive():
                log.warning("El maestro no responde.")
                start_election()

        time.sleep(HEARTBEAT_INTERVAL)


# ─── Punto de entrada ────────────────────────────────────────────────────────

def main():
    log.info(f"★ Esclavo-{SLAVE_ID} arrancado ★")
    log.info(f"Deriva inicial del reloj: {state['clock_offset']:+.3f}s")

    # Servidor TCP en hilo aparte
    threading.Thread(target=run_server, daemon=True).start()

    # Monitor del maestro
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info(f"Esclavo-{SLAVE_ID} detenido manualmente.")


if __name__ == "__main__":
    main()

