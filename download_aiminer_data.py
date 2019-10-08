import os
from util.data_io import download_data

if __name__ == '__main__':
    base_url = 'https://academicgraphv2.blob.core.windows.net/oag/mag/paper'
    data_folder = '/docker-share/data/MAG_papers'
    for k in range(9):
        file_name = 'mag_papers_%d.zip' %k
        download_data(base_url,file_name,data_folder,unzip_it=True,verbose=True)

    for file_name in os.listdir(data_folder):
        file = data_folder + '/' + file_name
        if file_name.endswith('.txt'):
            os.system('gzip %s' % file)
