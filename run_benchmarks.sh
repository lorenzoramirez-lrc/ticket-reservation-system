#!/usr/bin/env bash
# ==============================================================================
# Archivo: run_benchmarks.sh
# Objetivo: Automatizar la preparación de base de datos y ejecución de los
#           3 escenarios de prueba de rendimiento (x3 repeticiones de 180s cada una)
#           utilizando Locust sobre src/clients/client.py (PerformanceUser).
# ==============================================================================

set -e

# Configuración por defecto
MANAGER_ADDR="${MANAGER_ADDR:-tcp://10.43.100.20:5555}"
DURATION="${DURATION:-180}"
REPETITIONS="${REPETITIONS:-3}"
RESULTS_DIR="${RESULTS_DIR:-tests/performance/results}"
SKIP_DB_RESET=false

# Función de ayuda
usage() {
    echo "Uso: $0 [opciones]"
    echo ""
    echo "Opciones:"
    echo "  -m <addr>       Dirección del Gestor de Transacciones (defecto: $MANAGER_ADDR)"
    echo "  -d <segundos>   Duración de cada corrida en segundos (defecto: $DURATION)"
    echo "  -r <num>        Número de repeticiones por escenario (defecto: $REPETITIONS)"
    echo "  -s              Omitir reinicio de base de datos (si la BD está en otra máquina inaccesible)"
    echo "  -h              Mostrar esta ayuda"
    echo ""
    echo "Ejemplo:"
    echo "  $0 -m tcp://10.43.100.20:5555 -d 180 -r 3"
    exit 1
}

# Procesar argumentos de línea de comandos
while getopts "m:d:r:b:sh" opt; do
    case "$opt" in
        m) MANAGER_ADDR="$OPTARG" ;;
        d) DURATION="$OPTARG" ;;
        r) REPETITIONS="$OPTARG" ;;
        s) SKIP_DB_RESET=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

echo "================================================================================"
echo " INICIANDO SUITE DE PRUEBAS DE RENDIMIENTO - SISTEMA DE RESERVAS"
echo "================================================================================"
echo " Gestor de Transacciones : $MANAGER_ADDR"
echo " Duración por corrida    : ${DURATION}s"
echo " Repeticiones por corrida: $REPETITIONS"
echo " Directorio de resultados: $RESULTS_DIR"
echo "================================================================================"

# Array con configuración de los 3 escenarios: "id clientes consultas reservas nombre"
SCENARIOS=(
    "1 3 2 1 'Escenario 1 (3 Clientes - 2Q, 1R)'"
    "2 5 3 2 'Escenario 2 (5 Clientes - 3Q, 2R)'"
    "3 8 4 4 'Escenario 3 (8 Clientes - 4Q, 4R)'"
)

TOTAL_RUNS=$((${#SCENARIOS[@]} * REPETITIONS))
CURRENT_RUN=0

for scen_info in "${SCENARIOS[@]}"; do
    eval set -- "$scen_info"
    SCEN_ID="$1"
    USERS="$2"
    QUERIES="$3"
    RESERVES="$4"
    SCEN_DESC="$5"

    for ((REP=1; REP<=REPETITIONS; REP++)); do
        CURRENT_RUN=$((CURRENT_RUN + 1))
        RUN_ID="scenario_${SCEN_ID}_rep_${REP}"

        echo ""
        echo "--------------------------------------------------------------------------------"
        echo " [$CURRENT_RUN/$TOTAL_RUNS] $SCEN_DESC | Repetición $REP de $REPETITIONS"
        echo " Run ID: $RUN_ID | Usuarios concurrentes: $USERS | Duración: ${DURATION}s"
        echo " Hora de inicio: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "--------------------------------------------------------------------------------"

        # 1. Reinicio de la Base de Datos antes de cada repetición
        if [ "$SKIP_DB_RESET" = false ]; then
            echo "[BD] Restableciendo base de datos con database/fill_database.py..."
                python3 database/fill_database.py; then
                echo "[BD] Base de datos restablecida correctamente (cupos al 100%)."
            else
                echo "[ADVERTENCIA] No se pudo restablecer la BD automáticamente."
                echo "              Si PostgreSQL está en otra máquina, asegúrate de reiniciarla manualmente."
            fi
        else
            echo "[BD] Omitiendo reinicio automático de base de datos (-s activado)."

        # 2. Ejecutar prueba con Locust usando src/clients/client.py PerformanceUser
        echo "[LOCUST] Lanzando clientes concurrentes..."
        SCENARIO="$SCEN_ID" \
        MANAGER_ADDR="$MANAGER_ADDR" \
        RUN_ID="$RUN_ID" \
        RESULTS_DIR="$RESULTS_DIR" \
        python3 -m locust -f src/clients/client.py \
            --headless \
            -u "$USERS" \
            -r "$USERS" \
            -t "${DURATION}s" \
            PerformanceUser

        echo "[OK] Corrida $RUN_ID finalizada. Esperando 5 segundos para cerrar conexiones..."
        sleep 5
    done
done

echo ""
echo "================================================================================"
echo " ¡TODAS LAS CORRIDAS HAN FINALIZADO EXITOSAMENTE!"
echo "================================================================================"
echo " Archivos de clientes guardados en: $RESULTS_DIR"
echo ""
echo " Pasos siguientes para consolidar y analizar métricas:"
echo " 1. Si el Gestor (GT) corrió en otra máquina, copia su archivo gt_transactions.csv:"
echo "    scp usuario@<ip-gestor>:~/ruta/gt_transactions.csv $RESULTS_DIR/"
echo ""
echo " 2. Ejecuta el script de análisis con pandas:"
echo "    python3 tests/performance/analyze_metrics.py \\"
echo "      --results-dir $RESULTS_DIR \\"
echo "      --gt-csv $RESULTS_DIR/gt_transactions.csv"
echo "================================================================================"
