import os
import chardet
import tqdm
import time

if __name__ == "__main__":

    BASE_LOGS_PATH = "./data/logs"
    directories = os.listdir(BASE_LOGS_PATH)
    
    tic = time.time()
    
    for directory in directories:
        path = os.path.join(BASE_LOGS_PATH, directory)
        if not os.path.isdir(path):
            continue

        files = os.listdir(path)
        files = [file for file in files if file.endswith(".csv") and not file.endswith("_new.csv")]
        print(f"Processando diretório {directory} com {len(files)} arquivos")

        for file in tqdm.tqdm(files):
            # Caminhos dos arquivos
            path_old_file = os.path.join(path, file)
            filename = file.split(".")[0]
            new_filename = filename + "_new.csv"
            path_new_file = os.path.join(path, new_filename)

            # Detecta a codificação do arquivo
            with open(path_old_file, 'rb') as f:
                result = chardet.detect(f.read())

            encoding = result['encoding'] or 'ISO-8859-1'  # Fallback para 'ISO-8859-1'

            # Converte o arquivo para UTF-8
            try:
                with open(path_old_file, 'r', encoding=encoding, errors='ignore') as src:
                    with open(path_new_file, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
                os.remove(path_old_file)  # Remove o arquivo original (se necessário)
            except Exception as e:
                print(f"Erro ao converter {file}: {e}")

    toc = time.time()

    print(f"A conversão durou {toc - tic} segundos")
