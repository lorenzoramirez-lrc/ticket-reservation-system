#!/usr/bin/env python3
"""
Archivo: analyze_metrics.py
Objetivo: Procesamiento estadístico de métricas de rendimiento utilizando pandas.
          Calcula latencias promedio, desviación estándar muestral std(ddof=1),
          TPM en el GT dentro de la ventana de 180s (totales y exitosas),
          tasas de error y consolidación comparativa entre escenarios.
"""

import argparse
import glob
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np


def load_client_records(run_dir: str) -> pd.DataFrame:
    """Carga y concatena todos los archivos CSV de clientes para un run específico."""
    client_files = glob.glob(os.path.join(run_dir, "client_*.csv"))
    if not client_files:
        return pd.DataFrame()

    dfs = []
    for f in client_files:
        try:
            df = pd.read_csv(f)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"[WARN] No se pudo leer {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    full_df = pd.concat(dfs, ignore_index=True)
    full_df["response_time_ms"] = pd.to_numeric(full_df["response_time_ms"], errors="coerce")
    full_df["operation_type"] = full_df["operation_type"].astype(str).str.upper().str.strip()
    full_df["status"] = full_df["status"].astype(str).str.upper().str.strip()
    return full_df


def load_gt_records(run_id: str, run_dir: str, global_gt_path: Optional[str] = None) -> pd.DataFrame:
    """
    Carga registros del GT correspondientes a un run_id.
    Busca en el directorio de la corrida o en el archivo global de métricas del GT.
    Consolida instancias y descarta duplicados por request_id.
    """
    candidates = []
    if global_gt_path and os.path.exists(global_gt_path):
        candidates.append(global_gt_path)

    # Buscar en el directorio del run
    candidates.extend(glob.glob(os.path.join(run_dir, "gt_*.csv")))
    # Buscar en directorio padre results/
    candidates.extend(glob.glob(os.path.join(os.path.dirname(run_dir), "gt_*.csv")))
    # Buscar en raíz del proyecto
    candidates.extend(glob.glob("gt_*.csv"))

    unique_files = list(set(candidates))
    dfs = []
    for f in unique_files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                if "run_id" in df.columns:
                    match = df[df["run_id"] == run_id]
                    if not match.empty:
                        dfs.append(match)
            except Exception as e:
                print(f"[WARN] Error leyendo GT CSV {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    full_gt = pd.concat(dfs, ignore_index=True)
    # Evitar duplicados por request_id si existen múltiples instancias
    if "request_id" in full_gt.columns:
        full_gt = full_gt.drop_duplicates(subset=["request_id"])

    full_gt["operation_type"] = full_gt["operation_type"].astype(str).str.upper().str.strip()
    full_gt["status"] = full_gt["status"].astype(str).str.upper().str.strip()
    full_gt["completion_timestamp"] = pd.to_datetime(full_gt["completion_timestamp"], errors="coerce", format="ISO8601", utc=True)
    return full_gt


def calculate_tpm_from_gt(
    gt_df: pd.DataFrame,
    window_seconds: float = 180.0,
    metadata_start: Optional[str] = None
) -> Dict[str, Dict[str, float]]:
    """
    Calcula Transacciones Por Minuto (TPM) utilizando exclusivamente los registros del GT
    dentro de la ventana de medición de 180 segundos.
    Fórmula: TPM = operaciones en ventana / (window_seconds / 60)
    """
    if gt_df.empty or "completion_timestamp" not in gt_df.columns:
        return {}

    # Determinar inicio de la ventana
    valid_ts = gt_df["completion_timestamp"].dropna()
    if valid_ts.empty:
        return {}

    if metadata_start:
        w_start = pd.to_datetime(metadata_start, format="ISO8601", utc=True)
    else:
        w_start = valid_ts.min()

    w_end = w_start + pd.Timedelta(seconds=window_seconds)
    window_minutes = window_seconds / 60.0

    # Filtrar ventana exacta
    in_window = gt_df[(gt_df["completion_timestamp"] >= w_start) & (gt_df["completion_timestamp"] <= w_end)]

    results = {}
    for op in ["CONSULTA", "RESERVA", "TOTAL"]:
        if op == "TOTAL":
            op_df = in_window
        else:
            op_df = in_window[in_window["operation_type"] == op]

        total_ops = len(op_df)
        success_ops = len(op_df[op_df["status"] == "SUCCESS"])

        results[op] = {
            "tpm_total": total_ops / window_minutes,
            "tpm_success": success_ops / window_minutes,
            "gt_ops_in_window": total_ops,
            "gt_success_in_window": success_ops
        }

    return results


def process_single_run(
    run_dir: str,
    window_seconds: float = 180.0,
    global_gt_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Procesa una única ejecución generando métricas para CONSULTA, RESERVA y TOTAL."""
    meta_path = os.path.join(run_dir, "metadata.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

    run_id = meta.get("run_id", os.path.basename(run_dir))
    scenario = meta.get("scenario", "unknown")
    clients_count = meta.get("clients", None)
    meta_start = meta.get("timestamp_start", None)
    dur_s = float(meta.get("duration_seconds", window_seconds))

    client_df = load_client_records(run_dir)
    if client_df.empty:
        print(f"[WARN] No hay registros de clientes en {run_dir}")
        return []

    # Inferir número de clientes si no está en metadata
    if clients_count is None:
        clients_count = client_df["client_id"].nunique()

    # Cargar y calcular TPM desde el GT
    gt_df = load_gt_records(run_id, run_dir, global_gt_path)
    gt_tpm = calculate_tpm_from_gt(gt_df, window_seconds=dur_s, metadata_start=meta_start)

    run_rows = []
    window_minutes = dur_s / 60.0

    for op in ["CONSULTA", "RESERVA", "TOTAL"]:
        if op == "TOTAL":
            sub_df = client_df
        else:
            sub_df = client_df[client_df["operation_type"] == op]

        total_ops = len(sub_df)
        success_df = sub_df[sub_df["status"] == "SUCCESS"]
        success_ops = len(success_df)
        biz_errors = len(sub_df[sub_df["status"] == "BUSINESS_ERROR"])
        timeouts = len(sub_df[sub_df["status"] == "TIMEOUT"])
        sys_errors = len(sub_df[sub_df["status"] == "SYSTEM_ERROR"])
        failed_ops = total_ops - success_ops
        error_pct = (failed_ops / total_ops * 100.0) if total_ops > 0 else 0.0

        # Latencias: calculadas EXCLUSIVAMENTE para operaciones exitosas
        if success_ops > 0:
            avg_lat_ms = float(success_df["response_time_ms"].mean())
            # std muestral con ddof=1 sobre todas las observaciones del escenario
            std_lat_ms = float(success_df["response_time_ms"].std(ddof=1)) if success_ops >= 2 else 0.0
        else:
            avg_lat_ms = 0.0
            std_lat_ms = 0.0

        # TPM: Preferir registros de GT; de no haber GT, fallback a cliente anotado
        if op in gt_tpm:
            tpm_tot = gt_tpm[op]["tpm_total"]
            tpm_suc = gt_tpm[op]["tpm_success"]
            tpm_src = "GT"
        else:
            tpm_tot = total_ops / window_minutes
            tpm_suc = success_ops / window_minutes
            tpm_src = "CLIENT_FALLBACK"

        run_rows.append({
            "scenario": scenario,
            "run_id": run_id,
            "clients": clients_count,
            "operation_type": op,
            "avg_response_time_ms": round(avg_lat_ms, 3),
            "std_response_time_ms": round(std_lat_ms, 3),
            "tpm_total": round(tpm_tot, 2),
            "tpm_success": round(tpm_suc, 2),
            "total_operations": total_ops,
            "success_operations": success_ops,
            "failed_operations": failed_ops,
            "business_errors": biz_errors,
            "timeouts": timeouts,
            "system_errors": sys_errors,
            "error_percentage": round(error_pct, 2),
            "tpm_source": tpm_src
        })

    return run_rows


def build_consolidated_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla comparativa consolidada entre los tres escenarios,
    agregando las múltiples repeticiones mediante promedios y desviaciones estándar.
    """
    if summary_df.empty:
        return pd.DataFrame()

    grouped = summary_df.groupby(["scenario", "operation_type"], as_index=False).agg(
        clients=("clients", "first"),
        repetitions=("run_id", "nunique"),
        avg_response_time_ms_mean=("avg_response_time_ms", "mean"),
        avg_response_time_ms_std=("avg_response_time_ms", lambda x: x.std(ddof=1) if len(x) >= 2 else 0.0),
        tpm_total_mean=("tpm_total", "mean"),
        tpm_total_std=("tpm_total", lambda x: x.std(ddof=1) if len(x) >= 2 else 0.0),
        tpm_success_mean=("tpm_success", "mean"),
        tpm_success_std=("tpm_success", lambda x: x.std(ddof=1) if len(x) >= 2 else 0.0),
        error_percentage_mean=("error_percentage", "mean"),
        total_operations_sum=("total_operations", "sum"),
        success_operations_sum=("success_operations", "sum"),
        failed_operations_sum=("failed_operations", "sum")
    )

    # Redondeo para presentación limpia
    num_cols = [
        "avg_response_time_ms_mean", "avg_response_time_ms_std",
        "tpm_total_mean", "tpm_total_std", "tpm_success_mean",
        "tpm_success_std", "error_percentage_mean"
    ]
    for col in num_cols:
        grouped[col] = grouped[col].round(3)

    return grouped


def analyze_all_results(
    results_dir: str = "tests/performance/results",
    output_summary: str = "tests/performance/results/summary_metrics.csv",
    output_comparison: str = "tests/performance/results/consolidated_comparison.csv",
    global_gt_path: Optional[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Escanea todos los directorios de corridas dentro de results_dir,
    genera el CSV resumen y la tabla consolidada comparativa.
    """
    run_dirs = [
        os.path.join(results_dir, d) for d in os.listdir(results_dir)
        if os.path.isdir(os.path.join(results_dir, d)) and not d.startswith(".")
    ]

    all_rows = []
    for r_dir in sorted(run_dirs):
        rows = process_single_run(r_dir, global_gt_path=global_gt_path)
        all_rows.extend(rows)

    if not all_rows:
        print("[INFO] No se encontraron ejecuciones para analizar.")
        return pd.DataFrame(), pd.DataFrame()

    summary_df = pd.DataFrame(all_rows)
    os.makedirs(os.path.dirname(output_summary), exist_ok=True)
    summary_df.to_csv(output_summary, index=False)
    print(f"[OK] Archivo resumen generado exitosamente: {output_summary}")

    comparison_df = build_consolidated_comparison(summary_df)
    os.makedirs(os.path.dirname(output_comparison), exist_ok=True)
    comparison_df.to_csv(output_comparison, index=False)
    print(f"[OK] Tabla consolidada comparativa generada exitosamente: {output_comparison}")

    return summary_df, comparison_df


def print_text_reports(summary_df: pd.DataFrame, comparison_df: pd.DataFrame):
    """Imprime en consola las tablas principales formateadas limpiamente."""
    if not summary_df.empty:
        print("\n========================= RESUMEN POR EJECUCIÓN =========================")
        display_cols = [
            "scenario", "run_id", "operation_type", "avg_response_time_ms",
            "std_response_time_ms", "tpm_total", "tpm_success", "error_percentage", "tpm_source"
        ]
        print(summary_df[display_cols].to_string(index=False))

    if not comparison_df.empty:
        print("\n================== TABLA CONSOLIDADA COMPARATIVA ENTRE ESCENARIOS ==================")
        comp_display = [
            "scenario", "clients", "operation_type", "repetitions",
            "avg_response_time_ms_mean", "avg_response_time_ms_std",
            "tpm_total_mean", "tpm_success_mean", "error_percentage_mean"
        ]
        print(comparison_df[comp_display].to_string(index=False))
        print("===================================================================================\n")


def main():
    parser = argparse.ArgumentParser(description="Procesador de métricas de rendimiento con pandas.")
    parser.add_argument("--results-dir", default="tests/performance/results",
                        help="Directorio base de resultados")
    parser.add_argument("--summary-out", default="tests/performance/results/summary_metrics.csv",
                        help="Ruta destino para summary_metrics.csv")
    parser.add_argument("--comparison-out", default="tests/performance/results/consolidated_comparison.csv",
                        help="Ruta destino para consolidated_comparison.csv")
    parser.add_argument("--gt-csv", default=None,
                        help="Ruta al archivo global de transacciones del GT si aplica")

    args = parser.parse_args()

    sum_df, comp_df = analyze_all_results(
        results_dir=args.results_dir,
        output_summary=args.summary_out,
        output_comparison=args.comparison_out,
        global_gt_path=args.gt_csv
    )

    print_text_reports(sum_df, comp_df)


if __name__ == "__main__":
    main()
