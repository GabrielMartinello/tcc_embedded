import os
import tqdm
import time

if __name__ == "__main__":

    BASE_LOGS_PATH = "./data/logs"
    # lista todos os diretórios com base no caminho
    directories = os.listdir(BASE_LOGS_PATH)
    #command = "touch {} && iconv -f ISO-8859-1 -t UTF-8//TRANSLIT {} > {} && rm {}"
    #Comando utilizando powershell do Windows para fazer a codificação do arquivo
    command = command = "powershell -Command \"New-Item -ItemType File -Path {} ; Get-Content {} | Out-File -Encoding UTF8 {} ; Remove-Item {}\""

    tic = time.time()
    for directory in directories:
        path = BASE_LOGS_PATH + "/" + directory
        if not os.path.isdir(path):
            continue

        files = os.listdir(path)
        files = [file for file in files if file.endswith(".csv") and not file.endswith("_new.csv")]
        print(f"Processando diretório {directory} com {len(files)} arquivos")

        for file in tqdm.tqdm(files):
            # converte o encoding do arquivo
            filename = file.split(".")[0]
            new_filename = filename + "_new.csv"
            
            path_old_file = path + "/" + file
            path_new_file = path + "/" + new_filename

            os.system(command.format(path_new_file, path_old_file, path_new_file, path_old_file))
    toc = time.time()

    print(f"A conversão durou {toc - tic} segundos")