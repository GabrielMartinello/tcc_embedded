import os

BASE_PATH = './data/logs'

# Cada arquivo zipado tem um monte de arquivo .logjez
# Cada arquivo tem um logd.dat que é o log que a gente precisa
# Temos que extrair todos os .logjez e renomear pra logd.dat 
# E depois converter pra um .csv

#Função pra deszipar 
def unzip_log_files(zip_file):
    filepath = zip_file[:-4]
    os.system(f'7z e {zip_file} -o{filepath} *.logjez -r')

    # Remove unnecessary files
    os.system(f'rm {zip_file}')  # Zip file

    # list all files in the directory
    files = os.listdir(filepath)

    for file in files:
        # extract .logjez files
        # and rename to .csv
        if file.endswith('.logjez'):
            new_filename = file[:-7]
            os.system(
                f'7z e {filepath}/{file} -y -o{filepath}/{new_filename} \
                > /dev/null'
            )
            os.system(
                f'mv \
                {filepath}/{new_filename}/logd.dat \
                {filepath}/{new_filename}.csv'
            )
            os.system(
                f'rm -r {filepath}/{new_filename}'
            )

    os.system(f'chmod 777 -R {filepath}')
    os.system(f'rm {filepath}/*.logjez')


if __name__ == "__main__":
    for file in os.listdir(BASE_PATH):
        if file.endswith('.zip'):
            unzip_log_files(os.path.join(BASE_PATH, file))