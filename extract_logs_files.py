import os
import subprocess

BASE_PATH = './data/logs'

# Cada arquivo zipado tem um monte de arquivo .logjez
# Cada arquivo tem um logd.dat que é o log que a gente precisa
# Temos que extrair todos os .logjez e renomear pra logd.dat 
# E depois converter pra um .csv

#Função pra deszipar 
def unzip_log_files(zip_file):
    filepath = zip_file[:-4]

    if not os.path.exists(filepath):
        os.makedirs(filepath)
    
    try:
        print(f"Extraindo para: {filepath}")
        cmd = f'7z e "{zip_file}" -o"{filepath}" *.jez -r -y >nul'
        subprocess.run(cmd, shell=True, check=True)
        print("Extração concluída!")
    except subprocess.CalledProcessError as e:
        print(f" Erro ao extrair: {e}")

    # Remove os arquivos zips pai
    #os.system(f'del {zip_file}')  # Zip file

    if not os.path.exists(filepath):
        print(f"Diretório {filepath} não existe.")
        return

    # lista todos os arquivos do diretório
    files = os.listdir(filepath)
    print('Files: ', files)

    if files:
        for file in files:
            # extract .logjez files
            # and rename to .csv
            if file.endswith('.jez'):
                new_filename = file[:-7]
                #Deszipa o arquivo
                os.system(f'7z e "{filepath}\\{file}" -y -o"{filepath}\\{new_filename}" > nul')
                # Renomeia para .csv
                os.system(f'move "{filepath}\\{new_filename}\\logd.dat" "{filepath}\\{new_filename}.csv"')
                #remove arquivo antigo
                os.remove(f"{filepath}\\{file}")
                #remove diretório do arquivo
                os.system(f'rmdir /s /q "{filepath}\\{new_filename}"')

    #os.system(f'del {filepath}/*.logjez')


if __name__ == "__main__":
    for file in os.listdir(BASE_PATH):
        if file.endswith('.zip'):
            unzip_log_files(os.path.join(BASE_PATH, file))