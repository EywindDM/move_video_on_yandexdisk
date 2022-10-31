import re
import time
import inspect
import datetime
import ast

import requests
import aiohttp
import asyncio
import aiofiles
from fake_useragent import UserAgent
# import yadisk

from settings import *


def check_token(headers):
    r = requests.get(f'https://oauth.yandex.ru/authorize?response_type=token&client_id={app_client_id}', headers=headers)
    if r.status_code == 200:
        return True
    else:
        return False


def get_token(user):
    r = requests.get(f'https://oauth.yandex.ru/authorize?response_type=token&client_id={app_client_id}')

    headers = {'user-agent': user}

    data = {'login': user_email,
            'passwd': user_password}

    r = requests.post(r.url, headers=headers, data=data)
    token = re.findall(r'disk.yandex.ru/client/disk#access_token=[^&]*', r.text)[0].split('=')[-1]

    return token


def find_all_files_on_pc_to_load(folder_with_cams):
    folders_to_load = []
    for camera in os.listdir(folder_with_cams):
        unchanged_folders = [folder_with_cams + '/' + camera + '/' + folder for folder in
                             os.listdir(folder_with_cams + '/' + camera) if re.search('_', folder) is None]
        folders_to_load = [*folders_to_load, *unchanged_folders]

    files_to_load = []
    for folder in folders_to_load:
        files_in_folder = [folder + '/' + file for file in os.listdir(folder)]
        files_to_load = [*files_to_load, *files_in_folder]

    return files_to_load


# def creating_new_dirs(folders, url):
#     for folder in folders:
#         y.mkdir(url + '/' + folder)


# def load_files(files, url):
#     print('starting load files to folder --> ' + url.split('/')[-1])
#     script_info['messages'].append('starting load files to folder --> ' + url.split('/')[-1])
#
#     links_to_upload = []
#     for file in files:
#         file_url = url + '/' + file.split('/')[-1]
#
#         r = s.get(f'https://cloud-api.yandex.net/v1/disk/resources/upload?path={file_url}')
#         links_to_upload.append(r.json()['href'])
#
#
#     print(str(len(files)) + ' -- files loaded in folder -->', url.split('/')[-1])
#     script_info['messages'].append(str(len(files)) + ' -- files loaded in folder -->' + url.split('/')[-1])


# def load_files_in_folders(files_to_load, new_dirs, url):
#     # print('start load files to new folder')
#     for folder in new_dirs:
#         folder_url = url + '/' + folder
#
#         # load files to new dirs
#         files_to_load_on_this_folder = [file for file in files_to_load if re.search(folder, file) is not None]
#         print(len(files_to_load_on_this_folder))
#         print(len(set(files_to_load_on_this_folder)))
#         load_files(files_to_load_on_this_folder, folder_url)
#     # print('finished load files to new folder')
#
#     # load files in old dirs if files not in this dirs
#     files_not_in_new_folders = [file for file in files_to_load if file.split('/')[-2] not in new_dirs]
#     # print('start load files to old folder')
#     # check_file_in_last_folder_and_download_new_files_if_it_existed(files_not_in_new_folders, cam_name, driver)
#     # print('finished load files to old folder')


def get_folder_with_last_date(folders_to_check):
    if folders_to_check:
        text_to_date = [datetime.datetime.strptime(folder.split('/')[-1], "%Y%m%d%H") for folder in folders_to_check]
        sorted_folders_to_check = [x for _, x in sorted(zip(text_to_date, folders_to_check))]
        return sorted_folders_to_check[-1]
    return None


def changing_files_local_links(new_files_url, cam_name):
    new_files_data = []
    for file_url_data in new_files_url:
        file_data = dict()
        for element in file_url_data:
            if type(element) == type(dict()):
                file_data['load_href'] = element['href']
            else:
                file_data['local_href'] = element
        new_files_data.append(file_data)

    new_files_data = [{'load_href': file_data['load_href'],
                       'local_href': folder_with_cams +
                                     file_data['local_href'].replace('%2F', '/').split(folder_with_cams_on_disk)[
                                         -1].replace(cam_name, camera_dict[cam_name]).split('&')[0]}
                      for file_data in new_files_data]
    return new_files_data


def commented(text):
    def inner(func):
        def wrapper(*arg):
            cam_name = str(inspect.getfullargspec(func).args[0])
            print('start ' + text + cam_name)
            script_info['messages'].append('start ' + text + cam_name)
            func(*arg)
            print('finish ' + text + cam_name)
            script_info['messages'].append('finish ' + text + cam_name)
        return wrapper
    return inner


@commented(text='upload camera --> ')
def upload_data_from_camera(cam_name, allfiles, session, headers):
    camera_url = '/' + folder_with_cams_on_disk + '/' + cam_name
    # print(camera_url)

    # ищем все папки для данной камеры на диске
    limit = str(storage_date * 24)
    r = session.get(f'https://cloud-api.yandex.net/v1/disk/resources?path={camera_url}&type=dir&limit={limit}&fields=_embedded.items.path')
    exsisted_folders_on_disk = [val for folder in r.json()['_embedded']['items'] for key, val in folder.items()]

    # нужно найти последнюю созданную папку на диске, чтобы проверить не нужно ли догрузить в нее файлы
    last_folder = get_folder_with_last_date(exsisted_folders_on_disk)

    # из всех файлов (на локальной машине) выбираем файлы камеры
    camera_files_to_load = [file for file in allfiles if re.search(camera_dict[cam_name], file) is not None]

    # смотрим все папки камеры на локальной машине и проверяем нет ли их на диске, если нет то добавляем в list на создание
    camera_new_folders = set([file.split('/')[-2] for file in camera_files_to_load if file.split('/')[-2] not in camera_files_to_load])
    camera_new_dirs_path = [camera_url + '/' + folder for folder in camera_new_folders]
    camera_new_dirs_path = [folder.replace('/', '%2F') for folder in camera_new_dirs_path]

    # добавляем последнюю папку на диске в список новых
    camera_new_folders = list(camera_new_folders)
    camera_new_folders.append(last_folder.split('/')[-1])

    # из файлов камеры выбираем только те что есть в новых папках и в последней папке, на диске
    camera_files_to_load = [file for file in camera_files_to_load if file.split('/')[-2] in camera_new_folders]
    urls_files_to_load = [file.split('/share')[-1].replace(folder_with_cams.split('/')[-1], folder_with_cams_on_disk) for file in camera_files_to_load]
    urls_files_to_load = [file.replace(camera_dict[cam_name], cam_name).replace('/', '%2F') for file in urls_files_to_load]

    # создаем папки на диске
    asyncio.run(create_folders(camera_new_dirs_path, headers))

    # с помощью get запроса получаем url на добавление файлов
    new_files_url = asyncio.run(get_files_urls(urls_files_to_load, headers))

    # добавляем к созданным ссылкам на загрузку ссылки на локальный путь к файлу
    new_files_data = changing_files_local_links(new_files_url, cam_name)

    # загружаем файлы на диск (1-я ссылка - url загрузки, 2-я на диск)
    asyncio.run(upload_files(new_files_data, headers))






    return camera_new_dirs_path, last_folder
    # creating_new_dirs(camera_new_dirs, camera_url)
    #
    # load_files_in_folders(camera_files_to_load, camera_new_dirs, camera_url)
    #
    #


# async def get_operations_info(operations, headers):
#     tasks = []
#     async with aiohttp.ClientSession(headers=headers) as async_session:
#         for operation in operations:
#             task = asyncio.create_task(get_file_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources/upload?path={file}&overwrite=false'))
#             tasks.append(task)
#         camera_files = await asyncio.gather(*tasks)
#         return camera_files



async def upload_file(session, url, data):
    async with aiofiles.open(data, mode='rb') as f:
        file_data = await f.read()

    async with session.put(url, data=file_data) as r:
        print('upload_file  -->' + str(r.status))
        print(url)
        if str(r.status)[0] != '2':
            script_info['errors'].append({'upload_file': r.status,
                                          'url': url})

async def upload_files(urls_data, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for data in urls_data:
            task = asyncio.create_task(upload_file(async_session, data['load_href'], data['local_href']))
            tasks.append(task)
        await asyncio.gather(*tasks)


async def get_file_url(session, url):
    async with session.get(url) as r:
        # print('get_file_url -->' + str(r.status))
        # print(url)
        if str(r.status)[0] != '2':
            script_info['errors'].append({'get_file_url': r.status,
                                          'url': url})
        return await r.json(), url


async def get_files_urls(camera_files, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for file in camera_files:
            task = asyncio.create_task(get_file_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources/upload?path={file}&overwrite=false'))
            tasks.append(task)
        camera_files = await asyncio.gather(*tasks)
        return camera_files


async def put_folder_url(session, url):
    async with session.put(url) as r:
        # print('put_folder_url  -->' + str(r.status))
        # print(url)
        if str(r.status)[0] != '2':
            script_info['errors'].append({'put_folder_url': r.status,
                                          'url': url})

async def create_folders(folders, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for folder in folders:
            tasks.append(asyncio.create_task(put_folder_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources?path={folder}')))
        return await asyncio.gather(*tasks)


async def put_file_url(session, url):
    async with session.put(url) as r:
        print('put_file_url  -->' + str(r.status))
        print(url)
        # if str(r.status)[0] != '2':
        #     script_info['errors'].append({'put_folder_url': r.status,
        #                                   'url': url})


async def create_files(files_url, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for file_url in files_url:
            tasks.append(asyncio.create_task(put_file_url(async_session, file_url)))
        return await asyncio.gather(*tasks)



if __name__ == '__main__':
    start_time = time.time()
    script_info['cameras'] = cameras_to_write_on_disk

    print(app_client_id)
    print(token)

    files_to_load = find_all_files_on_pc_to_load(folder_with_cams)

    # y = yadisk.YaDisk(token=f"{token}")

    user = UserAgent().random
    headers = {'user-agent': user,
               'Authorization': token}

    # data = {
    #     'login': user_email,
    #     'passwd': user_password,
    # }

    if check_token(headers) is False:
        token = get_token(user)

    session = requests.Session()
    user = UserAgent().random

    session.headers.update(headers)

    try:
        # cameras_files = []
        for key in camera_dict:
            if key in cameras_to_write_on_disk:
                upload_data_from_camera(key, files_to_load, session, headers)
                # cameras_files = [*cameras_files, *new_camera_files]


        # cameras_urls = asyncio.run(get_cameras_urls(cameras_files, headers))
        # cameras_urls = [re.findall(r'https[^"]*', data.decode('utf-8'))[0] for data in cameras_urls]
        #



        # print(cameras_urls)

        # cameras_files_urls = []
        # start_time = time.time()
        # for file in cameras_files:
        #     r = session.get(f'https://cloud-api.yandex.net/v1/disk/resources/upload?path={file}&overwrite=false')
        #     cameras_files_urls.append(r.json()['href'])
        # print(cameras_files_urls)
        # print(time.time() - start_time)

    except Exception as err:
        print(err)



































