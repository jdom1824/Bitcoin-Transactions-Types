import os
import csv
import time
from tqdm import tqdm
from bitcoin.core import CBlock
from bitcoin.core import b2lx
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import struct

# Directorio de archivos blk*
blk_dir = "/to/direct/node/Bitcoin/blocks"
csv_file_path = "/to/direct/blkexc/block_data.csv"

# Configuración de RPC
rpc_user = "****"
rpc_password = "****"
rpc_port = 8332
rpc_host = "127.0.0.1"
rpc_url = f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}/"

# Crear conexión RPC
rpc_connection = AuthServiceProxy(rpc_url)

# Función para obtener la altura del bloque usando RPC
def get_block_height(block_hash):
    try:
        block_header = rpc_connection.getblockheader(block_hash)
        return block_header['height']
    except JSONRPCException as e:
        print(f"Error en la consulta RPC: {str(e)}")
        return None

# Función para contar bloques en el archivo blk
def count_blocks_in_file(file_path):
    block_count = 0
    with open(file_path, "rb") as f:
        while True:
            magic = f.read(4)
            if len(magic) < 4:
                break

            block_size_bytes = f.read(4)
            if len(block_size_bytes) < 4:
                break

            block_size = struct.unpack("<I", block_size_bytes)[0]

            if block_size <= 0 or block_size > 134217728:  # 128 MB límite para el tamaño de bloque
                print(f"Tamaño de bloque no razonable: {block_size} bytes en el bloque {block_count + 1}")
                break

            f.seek(block_size, os.SEEK_CUR)  # Saltar al siguiente bloque
            block_count += 1

    return block_count

# Función para determinar el tipo de transacción
def classify_tx_output(output):
    script = output.scriptPubKey

    # Identificar P2PK (ScriptPubKey contiene solo una clave pública y OP_CHECKSIG)
    if len(script) == 35 and script[-1] == 0xac:
        return 'p2pk'
    # Identificar P2PKH (ScriptPubKey sigue el patrón OP_DUP OP_HASH160 <PubKeyHash> OP_EQUALVERIFY OP_CHECKSIG)
    elif len(script) == 25 and script[0] == 0x76 and script[1] == 0xa9 and script[-2] == 0x88 and script[-1] == 0xac:
        return 'p2pkh'
    # Identificar P2SH (ScriptPubKey sigue el patrón OP_HASH160 <ScriptHash> OP_EQUAL)
    elif len(script) == 23 and script[0] == 0xa9 and script[-1] == 0x87:
        return 'p2sh'
    # Identificar SegWit (P2WPKH o P2WSH, comienza con OP_0 y 20 o 32 bytes de datos)
    elif (len(script) == 22 and script[0] == 0x00 and script[1] == 0x14) or (len(script) == 34 and script[0] == 0x00 and script[1] == 0x20):
        return 'segwit'
    return None

# Función para procesar todos los archivos blk secuencialmente y guardar en CSV
def process_all_blk_files_and_save_csv(blk_dir, csv_file_path):
    blk_files = sorted([f for f in os.listdir(blk_dir) if f.startswith("blk") and f.endswith(".dat")])

    if not blk_files:
        print("No se encontraron archivos blk en el directorio especificado.")
        return

    # Abrir el archivo CSV para escritura
    with open(csv_file_path, mode='w', newline='') as csvfile:
        fieldnames = ['height', 'timestamp', 'tx_count', 'p2pk', 'p2pkh', 'p2sh', 'segwit', 'block_size_bytes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for blk_file in blk_files:
            file_path = os.path.join(blk_dir, blk_file)
            
            # Contar bloques en el archivo
            total_blocks = count_blocks_in_file(file_path)
            print(f"Archivo {blk_file} contiene {total_blocks} bloques.")

            with open(file_path, "rb") as f, tqdm(total=total_blocks, desc=f"Procesando {blk_file}", unit="bloques") as pbar:
                while True:
                    try:
                        magic = f.read(4)
                        if len(magic) < 4:
                            break

                        block_size_bytes = f.read(4)
                        if len(block_size_bytes) < 4:
                            break
                        block_size = struct.unpack("<I", block_size_bytes)[0]

                        block_data = f.read(block_size)
                        if len(block_data) != block_size:
                            print(f"Error leyendo el bloque: tamaño esperado {block_size}, tamaño leído {len(block_data)}.")
                            break

                        block = CBlock.deserialize(block_data)
                        block_hash = b2lx(block.GetHash())

                        # Consultar la altura del bloque usando RPC
                        height = get_block_height(block_hash)
                        if height is not None:
                            # Extraer timestamp y número de transacciones
                            timestamp = block.nTime
                            tx_count = len(block.vtx)
                            
                            # Inicializar contadores para los tipos de transacciones
                            p2pk_count = 0
                            p2pkh_count = 0
                            p2sh_count = 0
                            segwit_count = 0

                            for tx in block.vtx:
                                if tx.is_coinbase():
                                    continue  # Ignorar transacciones coinbase

                                # Flags para contar solo una vez por transacción
                                p2pk_included = False
                                p2pkh_included = False
                                p2sh_included = False
                                segwit_included = False

                                for output in tx.vout:
                                    tx_type = classify_tx_output(output)
                                    if tx_type == 'p2pk' and not p2pk_included:
                                        p2pk_count += 1
                                        p2pk_included = True
                                    elif tx_type == 'p2pkh' and not p2pkh_included:
                                        p2pkh_count += 1
                                        p2pkh_included = True
                                    elif tx_type == 'p2sh' and not p2sh_included:
                                        p2sh_count += 1
                                        p2sh_included = True
                                    elif tx_type == 'segwit' and not segwit_included:
                                        segwit_count += 1
                                        segwit_included = True

                            # Escribir la información en el archivo CSV
                            writer.writerow({
                                'height': height,
                                'timestamp': timestamp,
                                'tx_count': tx_count,
                                'p2pk': p2pk_count,
                                'p2pkh': p2pkh_count,
                                'p2sh': p2sh_count,
                                'segwit': segwit_count,
                                'block_size_bytes': block_size
                            })

                    except Exception as e:
                        print(f"Error procesando un bloque: {str(e)}")
                        break

                    pbar.update(1)

            # Pausa de 1 segundos entre archivos blk 
            time.sleep(1)

    print(f"Información de todos los bloques guardada en: {csv_file_path}")

# Ejecutar el proceso para todos los archivos blk y guardar en CSV
process_all_blk_files_and_save_csv(blk_dir, csv_file_path)
