import psycopg2
from psycopg2.extras import execute_batch
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from typing import List, Tuple, Optional
import os
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sql_executor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# configuração do banco de dados
@dataclass
class DatabaseConfig:
    host: str = "seu_host"
    port: int = 5432
    database: str = "seu_banco"
    user: str = "postgres"
    password: str = "sua_senha"
    
    @classmethod
    def from_env(cls):
        """Carrega configuração a partir de variáveis de ambiente"""
        return cls(
            host=os.getenv('DB_HOST', 'seu_host'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'seu_banco'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'sua_senha')
        )

class SQLExecutor:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.batch_size = 1000
        self.max_workers = 4
        self.error_log_file = "comandos_erro.sql"
        self.failed_commands = []
        self.lock = threading.Lock()  # Para thread safety ao escrever erros
        
    # caso um comando falhe, salva o comando e o motivo do erro
    def save_failed_command(self, command: str, error_reason: str = ""):

        with self.lock:
            self.failed_commands.append({
                'command': command,
                'error_reason': error_reason,
                'timestamp': datetime.now().isoformat()
            })
    
    # escreve os comandos que falharam em um arquivo de log
    def write_error_log(self):
        
        if not self.failed_commands:
            return
            
        try:
            with open(self.error_log_file, 'w', encoding='utf-8') as f:
                f.write("-- COMANDOS QUE FALHARAM NA EXECUÇÃO\n")
                f.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- Total de comandos com falha: {len(self.failed_commands)}\n\n")
                
                for i, failed_cmd in enumerate(self.failed_commands, 1):
                    f.write(f"-- ERRO {i}: {failed_cmd['error_reason']}\n")
                    f.write(f"-- Timestamp: {failed_cmd['timestamp']}\n")
                    f.write(f"{failed_cmd['command']}\n")
                    if not failed_cmd['command'].endswith(';'):
                        f.write(";\n")
                    f.write("\n")
                    
            logger.info(f"Arquivo de erro salvo: {self.error_log_file} ({len(self.failed_commands)} comandos)")
            
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo de erro: {e}")
    
    # limpa o log, removendo o arquivo caso ele já exista
    def clear_error_log(self):
       
        self.failed_commands = []
        if os.path.exists(self.error_log_file):
            try:
                os.remove(self.error_log_file)
                logger.info(f"Arquivo de erro anterior removido: {self.error_log_file}")
            except Exception as e:
                logger.warning(f"Não foi possível remover arquivo de erro anterior: {e}")
    
    #cria uma nova conexão com o banco de dados
    def create_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password
        )
    
    #testa a cnexão com o banco de dados
    def test_connection(self) -> bool:
        
        try:
            conn = self.create_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
            conn.close()
            logger.info("Conexão com banco estabelecida com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar com banco: {e}")
            return False
    
    # executa comandos em lotes, individualmente ou em blocos
    def execute_batch_direct(self, commands: List[str]) -> Tuple[int, int]:
       
        conn = self.create_connection()
        successful = 0
        failed = 0
        
        try:
            with conn.cursor() as cursor:
                for cmd in commands:
                    try:
                        # executa o comando exatamente como está no arquivo
                        cursor.execute(cmd)
                        successful += 1
                    except Exception as e:
                        logger.warning(f"Erro em comando individual: {e}")
                        logger.warning(f"Comando que falhou: {cmd[:100]}...")
                        self.save_failed_command(cmd, f"Erro na execução: {str(e)}")
                        failed += 1
                        # daz rollback apenas para este comando e continua
                        conn.rollback()
                        continue
                
                # Commit das transações que deram certo
                conn.commit()
                
        except Exception as e:
            logger.error(f"Erro geral no lote: {e}")
            conn.rollback()
            
            # Salva todos os comandos restantes como falhados
            for cmd in commands:
                self.save_failed_command(cmd, f"Erro de conexão/transação: {str(e)}")
                
            failed = len(commands)
        finally:
            conn.close()
            
        return successful, failed
    
    # opção mais rápida, executa todos os comandos em uma única transação
    def execute_batch_transaction(self, commands: List[str]) -> Tuple[int, int]:
        
        conn = self.create_connection()
        successful = 0
        failed = 0
        
        try:
            with conn.cursor() as cursor:
                # Tenta executar todos os comandos em uma única transação
                for cmd in commands:
                    cursor.execute(cmd)
                
                # se nenhum falhar chega aqui
                conn.commit()
                successful = len(commands)
                
        except Exception as e:
            logger.error(f"Erro no lote (transação): {e}")
            conn.rollback()
            
            # Salva todos os comandos como falhados
            for cmd in commands:
                self.save_failed_command(cmd, f"Erro na transação do lote: {str(e)}")
                
            failed = len(commands)
        finally:
            conn.close()
            
        return successful, failed
    
    #faz a leitura do arquivo SQL, removendo comentários e separando os comandos
    def read_sql_file(self, file_path: str) -> List[str]:
       
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Remove comentários de linha (--) 
            lines = content.split('\n')
            cleaned_lines = []
            for line in lines:
                # Remove comentários que começam com --
                if '--' in line:
                    line = line.split('--')[0]
                cleaned_lines.append(line)
            
            content = '\n'.join(cleaned_lines)
            
            # Split por ponto e vírgula
            commands = []
            current_command = ""
            in_string = False
            
            i = 0
            while i < len(content):
                char = content[i]
                
                # Detecta início/fim de string
                if char == "'" and (i == 0 or content[i-1] != '\\'):
                    in_string = not in_string
                
                # Se encontrar ; fora de string, finaliza comando
                if char == ';' and not in_string:
                    if current_command.strip():
                        commands.append(current_command.strip())
                    current_command = ""
                else:
                    current_command += char
                
                i += 1
            
            # Adiciona último comando se não terminou com ;
            if current_command.strip():
                commands.append(current_command.strip())
            
            # Filtra apenas comandos UPDATE (ignora comentários e linhas vazias)
            update_commands = []
            for cmd in commands:
                cmd = cmd.strip()
                if cmd and cmd.upper().startswith('UPDATE'):
                    update_commands.append(cmd)
            
            logger.info(f"Lidos {len(update_commands)} comandos UPDATE do arquivo {file_path}")
            
            # Mostra uma amostra dos comandos encontrados
            if update_commands:
                logger.info("Primeiros comandos encontrados:")
                for i, cmd in enumerate(update_commands[:3]):
                    logger.info(f"  {i+1}: {cmd[:80]}...")
            
            return update_commands
            
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {file_path}: {e}")
            return []
    
    # executa todos os comandos do arquivo SQL
    def execute_file(self, file_path: str, use_transaction: bool = False) -> dict:
       
        if not Path(file_path).exists():
            logger.error(f"Arquivo não encontrado: {file_path}")
            return {"error": "Arquivo não encontrado"}
        
        # Limpa log de erros anterior
        self.clear_error_log()
        
        start_time = time.time()
        commands = self.read_sql_file(file_path)
        
        if not commands:
            return {"error": "Nenhum comando UPDATE encontrado no arquivo"}
        
        total_commands = len(commands)
        total_successful = 0
        total_failed = 0
        
        logger.info(f"Iniciando execução de {total_commands} comandos UPDATE...")
        logger.info(f"Modo de execução: {'Transação por lote' if use_transaction else 'Comando por comando'}")
        
        # Divide em lotes
        batches = [commands[i:i + self.batch_size] for i in range(0, len(commands), self.batch_size)]
        
        # Execução com threads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            if use_transaction:
                future_to_batch = {
                    executor.submit(self.execute_batch_transaction, batch): batch 
                    for batch in batches
                }
            else:
                future_to_batch = {
                    executor.submit(self.execute_batch_direct, batch): batch 
                    for batch in batches
                }
            
            for i, future in enumerate(as_completed(future_to_batch)):
                try:
                    successful, failed = future.result()
                    total_successful += successful
                    total_failed += failed
                    
                    progress = ((i + 1) / len(batches)) * 100
                    logger.info(f"Progresso: {progress:.1f}% - Lote {i+1}/{len(batches)} - "
                              f"Sucesso: {successful}, Falhas: {failed}")
                    
                except Exception as e:
                    logger.error(f"Erro no lote: {e}")
                    batch_commands = future_to_batch[future]
                    for cmd in batch_commands:
                        self.save_failed_command(cmd, f"Erro na execução do lote: {str(e)}")
                    total_failed += len(batch_commands)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Salva arquivo de comandos com erro
        self.write_error_log()
        
        result = {
            "total_commands": total_commands,
            "successful": total_successful,
            "failed": total_failed,
            "execution_time": execution_time,
            "commands_per_second": total_commands / execution_time if execution_time > 0 else 0,
            "error_file": self.error_log_file if self.failed_commands else None
        }
        
        logger.info(f"Execução concluída em {execution_time:.2f} segundos")
        logger.info(f"Comandos por segundo: {result['commands_per_second']:.2f}")
        logger.info(f"Sucessos: {total_successful}, Falhas: {total_failed}")
        
        if self.failed_commands:
            logger.info(f"Comandos com falha salvos em: {self.error_log_file}")
        
        return result

#Início do programa principal 
def main():
    
    config = DatabaseConfig.from_env()
    
    executor = SQLExecutor(config)
    
    # testa conexão
    if not executor.test_connection():
        logger.error("Não foi possível conectar ao banco. Verifique as configurações.")
        return
    
    # caminho do arquivo SQL
    file_path = input("Digite o caminho do arquivo SQL: ").strip()
    
    # configurações opcionais
    batch_size = input("Tamanho do lote (padrão 1000): ").strip()
    if batch_size.isdigit():
        executor.batch_size = int(batch_size)
    
    max_workers = input("Número de threads (padrão 4): ").strip()
    if max_workers.isdigit():
        executor.max_workers = int(max_workers)
    
    # modo de execução
    print("\nModos de execução disponíveis:")
    print("1. Comando por comando (mais tolerante a erros, mais lento)")
    print("2. Transação por lote (mais rápido, mas falha todo o lote se um comando falhar)")
    
    mode = input("Escolha o modo (1 ou 2, padrão 1): ").strip()
    use_transaction = mode == '2'
    
    if use_transaction:
        print("⚠️  Modo transação selecionado: se um comando falhar, todo o lote falhará")
    else:
        print("✅ Modo comando por comando selecionado: comandos são executados individualmente")
    
    # executa o arquivo
    print(f"\nIniciando execução do arquivo: {file_path}")
    result = executor.execute_file(file_path, use_transaction)
    
    print("\n" + "="*50)
    print("RESULTADO DA EXECUÇÃO")
    print("="*50)
    for key, value in result.items():
        if key == "error_file" and value:
            print(f"{key}: {value}")
        elif key != "error_file":
            print(f"{key}: {value}")
    
    if result.get("error_file"):
        print(f"\n⚠️  Arquivo com comandos que falharam: {result['error_file']}")
        print("   Você pode analisar e tentar executar esses comandos manualmente.")

if __name__ == "__main__":
    main()