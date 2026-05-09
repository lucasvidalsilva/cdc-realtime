import subprocess
import time
import sys
import requests
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.table import Table

console = Console()

def run_command(command, description):
    console.print(f"[bold blue]>[/bold blue] {description}...")
    return subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def wait_for_debezium():
    with console.status("[bold yellow]Aguardando Debezium ficar online...", spinner="dots"):
        while True:
            try:
                response = requests.get("http://localhost:8083/connectors")
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(2)
    console.print("[bold green]✓[/bold green] Debezium pronto!")

def register_connector():
    config = {
        "name": "nyc-trips-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "user",
            "database.password": "password",
            "database.dbname": "nyctaxi",
            "database.server.name": "pg-server",
            "plugin.name": "pgoutput",
            "table.include.list": "public.trips",
            "topic.prefix": "cdc"
        }
    }
    response = requests.post("http://localhost:8083/connectors", json=config)
    if response.status_code in [201, 409]: # 409 significa que já existe
        console.print("[bold green]✓[/bold green] Conector configurado!")
    else:
        console.print(f"[bold red]✗ Erro ao configurar conector: {response.text}")

def main():
    console.print(Panel.fit("[bold]CDC REAL-TIME PIPELINE[/bold]\n[dim]Postgres + Debezium + Kafka + DuckDB[/dim]", border_style="magenta"))

    # 1. Docker Compose
    run_command("docker compose -f docker/docker-compose.yml up -d", "Subindo infraestrutura Docker")
    
    # 2. Aguardar e Configurar
    wait_for_debezium()
    register_connector()

    # 3. Rodar os scripts
    console.print("\n[bold cyan] Iniciando Fluxo de Dados[/bold cyan]")
    
    # Iniciamos o Consumer e o Producer
    consumer = subprocess.Popen([sys.executable, "src/consumer/cdc_consumer.py"])
    time.sleep(5) # Tempo para o Kafka estabilizar
    
    if consumer.poll() is not None:
        console.print("[bold red]✗ O Consumer morreu ao iniciar. Verifique se o DuckDB está aberto em outro lugar![/bold red]")
        sys.exit(1)

    producer = subprocess.Popen([sys.executable, "src/producer/load_data.py"])

    try:
        console.print(Panel("O pipeline está rodando!\n[yellow]Pressione Ctrl+C para encerrar.[/yellow]", title="Status", border_style="green"))
        producer.wait() # Espera o produtor terminar de enviar as linhas
        console.print("\n[bold green]Producer finalizou o envio dos dados![/bold green]")
    except KeyboardInterrupt:
        console.print("\n[bold red]Encerrando processos...[/bold red]")
    finally:
        consumer.terminate()
        console.print("[bold blue]Pipeline finalizado com sucesso.[/bold blue]")

if __name__ == "__main__":
    main()