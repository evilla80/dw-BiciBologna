import subprocess
import time
import sys
import os

def run_extractor_loader():
    print("Eseguo l'estrazione dei dati")
    subprocess.run(
        [sys.executable, "extractor.py"],
        check=True,
        cwd="01_extraction"
    )

    print("Eseguo il caricamento dei dati")
    subprocess.run([sys.executable, "02_load/loader.py"])


def run_dbt():
    print("Eseguo le trasformazioni DBT...")
    python_dir = os.path.dirname(sys.executable)
    dbt_exe = os.path.join(python_dir, "dbt.exe")

    subprocess.run(
        [dbt_exe, "run", "--full-refresh"],
        check=True,
        cwd="bike_sharing_dbt"
    )

def run_dbt_test():
    print("Controllo dei dati")
    subprocess.run([sys.executable, "test_finale.py"])

    def run_export_for_tableau():
        print("Esportazione dati per Tableau in corso...")
        # Questo script interroga DuckDB e salva le tabelle dm_ in formato CSV
        subprocess.run([sys.executable, "04_export/exporter.py"], check=True)

if __name__ == "__main__":
    run_extractor_loader()
    run_dbt()
    run_dbt_test()