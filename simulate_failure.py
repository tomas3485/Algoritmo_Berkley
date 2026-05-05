"""
Simulador de caída del maestro
================================
Lanza todos los nodos y tras N segundos "mata"
al maestro para ver cómo los esclavos eligen
un nuevo coordinador mediante el algoritmo Bully.
"""

import subprocess
import sys
import time
import os
import signal

MASTER_FALL_AFTER = 25   # segundos antes de matar al maestro

def main():
    print("=" * 55)
    print("  SIMULACIÓN ALGORITMO DE BERKELEY + ELECCIÓN BULLY")
    print("=" * 55)
    print()

    os.makedirs("logs", exist_ok=True)

    python = sys.executable
    procs = []

    # ── Arrancar esclavos ──────────────────────────────────────
    print("[SIM] Arrancando Esclavo-1...")
    p1 = subprocess.Popen([python, "slave.py", "1"])
    procs.append(("Esclavo-1", p1))

    print("[SIM] Arrancando Esclavo-2...")
    p2 = subprocess.Popen([python, "slave.py", "2"])
    procs.append(("Esclavo-2", p2))

    time.sleep(2)

    # ── Arrancar maestro ───────────────────────────────────────
    print("[SIM] Arrancando Maestro...")
    pm = subprocess.Popen([python, "master.py"])
    procs.append(("Maestro", pm))

    print()
    print(f"[SIM] Sistema en marcha. El maestro caerá en {MASTER_FALL_AFTER}s.")
    print("[SIM] Pulsa Ctrl+C para detener la simulación.")
    print()

    try:
        time.sleep(MASTER_FALL_AFTER)

        # ── Simular caída del maestro ──────────────────────────
        print()
        print("=" * 55)
        print("  *** SIMULANDO CAÍDA DEL MAESTRO ***")
        print("=" * 55)
        pm.terminate()
        print("[SIM] Maestro terminado. Observa cómo los esclavos")
        print("      detectan la caída y eligen un nuevo maestro.")
        print()

        # Dejar que la elección suceda y observar
        time.sleep(30)

        print("[SIM] Simulación completada. Revisa los logs en ./logs/")

    except KeyboardInterrupt:
        print("\n[SIM] Deteniendo simulación...")
    finally:
        for name, p in procs:
            try:
                p.terminate()
                print(f"[SIM] {name} detenido.")
            except Exception:
                pass

    print("[SIM] ¡Listo! Revisa los archivos en ./logs/ para ver la traza completa.")


if __name__ == "__main__":
    main()