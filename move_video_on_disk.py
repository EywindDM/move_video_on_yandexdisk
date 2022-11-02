import re
import time
import datetime
import json

import requests
import aiohttp
import asyncio
import aiofiles
from fake_useragent import UserAgent

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


def get_folder_with_last_date(folders_to_check):
    if folders_to_check:
        text_to_date = [datetime.datetime.strptime(folder, "%Y%m%d%H") for folder in folders_to_check]
        sorted_folders_to_check = [x for _, x in sorted(zip(text_to_date, folders_to_check))]
        return sorted_folders_to_check[-1]
    return None


def changing_files_local_links(new_files_url, cam_name):
    new_files_data = []
    for file_url_data in new_files_url:
        # print(file_url_data)
        # print(len(file_url_data))
        file_data = dict()
        for element in file_url_data:
            if type(element) == type(dict()):
                try:
                    file_data['load_href'] = element['href']
                except Exception as err:
                    # ресурс на загрузку уже существует
                    break
            else:
                file_data['local_href'] = element
        if file_data:
            if len(file_data) == 2:
                new_files_data.append(file_data)

    if new_files_data:
        new_files_data = [{'load_href': file_data['load_href'],
                           'local_href': folder_with_cams +
                                         file_data['local_href'].replace('%2F', '/').split(folder_with_cams_on_disk)[
                                             -1].replace(cam_name, camera_dict[cam_name]).split('&')[0]}
                          for file_data in new_files_data]
    return new_files_data


def commented(text):
    def inner(func):
        def wrapper(cam_name, *arg):
            print('start ' + text + cam_name)
            script_info['messages'].append('start ' + text + cam_name)
            func(cam_name, *arg)
            print('finish ' + text + cam_name)
            script_info['messages'].append('finish ' + text + cam_name)
        return wrapper
    return inner


@commented(text='upload camera --> ')
def upload_data_from_camera(cam_name, allfiles, session, headers):
    camera_url = '/' + folder_with_cams_on_disk + '/' + cam_name

    # ищем все папки для данной камеры на диске
    limit = str(storage_date * 24)
    r = session.get(f'https://cloud-api.yandex.net/v1/disk/resources?path={camera_url}&type=dir&limit={limit}&fields=_embedded.items.path')
    exsisted_folders_on_disk = [val for folder in r.json()['_embedded']['items'] for key, val in folder.items()]
    exsisted_folders_on_disk = [folder.split('/')[-1] for folder in exsisted_folders_on_disk]


    # нужно найти последнюю созданную папку на диске, чтобы проверить не нужно ли догрузить в нее файлы
    last_folder = get_folder_with_last_date(exsisted_folders_on_disk)

    # из всех файлов (на локальной машине) выбираем файлы камеры
    camera_files_to_load = [file for file in allfiles if re.search(camera_dict[cam_name], file) is not None]

    # смотрим все папки камеры на локальной машине и проверяем нет ли их на диске, если нет то добавляем в list на создание
    camera_new_folders = set([file.split('/')[-2] for file in camera_files_to_load if file.split('/')[-2] not in exsisted_folders_on_disk])
    if camera_new_folders:
        camera_new_dirs_path = [camera_url + '/' + folder for folder in camera_new_folders]
        camera_new_dirs_path = [folder.replace('/', '%2F') for folder in camera_new_dirs_path]

    # добавляем последнюю папку на диске в список новых
    camera_new_folders = list(camera_new_folders)
    if last_folder:
        camera_new_folders.append(last_folder.split('/')[-1])

    # из файлов камеры выбираем только те что есть в новых папках и в последней папке, на диске
    camera_files_to_load = [file for file in camera_files_to_load if file.split('/')[-2] in camera_new_folders]
    urls_files_to_load = [file.split('/share')[-1].replace(folder_with_cams.split('/')[-1], folder_with_cams_on_disk) for file in camera_files_to_load]
    urls_files_to_load = [file.replace(camera_dict[cam_name], cam_name).replace('/', '%2F') for file in urls_files_to_load]

    if len(camera_new_folders) > 1:
        # создаем папки на диске
        asyncio.run(create_folders(camera_new_dirs_path, headers))

    # с помощью get запроса получаем url на добавление файлов
    new_files_url = asyncio.run(get_files_urls(urls_files_to_load, headers))
    # print(new_files_url)
    # print(len(new_files_url))

    # добавляем к созданным ссылкам на загрузку ссылки на локальный путь к файлу
    new_files_data = changing_files_local_links(new_files_url, cam_name)
    # print(new_files_data)
    # print(len(new_files_data))

    if new_files_data:
        # загружаем файлы на диск (1-я ссылка - url загрузки, 2-я на диск)
        asyncio.run(upload_files(new_files_data, headers))

    # удаляем устаревшие папки с файлами
    date_to_delete = (datetime.datetime.today() - datetime.timedelta(days=storage_date))
    folders_to_delete = [folder for folder in exsisted_folders_on_disk if datetime.datetime.strptime(folder.split('/')[-1], '%Y%m%d%H') < date_to_delete]
    asyncio.run(delete_folders(folders_to_delete, headers))


async def upload_file(session, url, data):
    async with aiofiles.open(data, mode='rb') as f:
        file_data = await f.read()

    async with session.put(url, data=file_data) as r:
        # print('upload_file  -->' + str(r.status))
        if str(r.status)[0] != '2':
            script_info['errors'].append({'upload_file': r.status,
                                          'url': url})
        return r.status


async def upload_files(urls_data, headers):
    tasks = []
    timeout = aiohttp.ClientTimeout(total=1000)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as async_session:
        for data in urls_data:
            tasks.append(asyncio.create_task(upload_file(async_session, data['load_href'], data['local_href'])))
        status_codes = await asyncio.gather(*tasks)
        new_files = [status_code for status_code in status_codes if str(status_code)[0] == '2']
        print(str(len(new_files)) + ' new files loaded on disk')
        script_info['messages'].append(str(len(new_files)) + ' new files loaded on disk')


async def get_file_url(session, url):
    async with session.get(url) as r:
        # print('get_file_url -->' + str(r.status))
        # print(url)
        if str(r.status)[0] != '2' and str(r.status) != '409':
            script_info['errors'].append({'get_file_url': r.status,
                                          'url': url})

        # if str(r.status)[0] == '2':
        return await r.json(), url, r.status


async def get_files_urls(camera_files, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for file in camera_files:
            tasks.append(asyncio.create_task(get_file_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources/upload?path={file}&overwrite=false')))
        camera_files = await asyncio.gather(*tasks)
        status_codes = [file[-1] for file in camera_files if str(file[-1])[0] == '2']
        camera_files = [(file[0], file[1]) for file in camera_files]

        print(str(len(status_codes)) + ' links to create file received')
        script_info['messages'].append(str(len(status_codes)) + ' links to create file received')
        return camera_files


async def put_folder_url(session, url):
    async with session.put(url) as r:
        # print('put_folder_url  -->' + str(r.status))
        # print(url)
        if str(r.status)[0] != '2':
            script_info['errors'].append({'put_folder_url': r.status, 'url': url})
        return r.status


async def create_folders(folders, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for folder in folders:
            tasks.append(asyncio.create_task(put_folder_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources?path={folder}')))
        new_folders = await asyncio.gather(*tasks)
        new_folders = [status_code for status_code in new_folders if str(status_code)[0] == '2']
        print(str(len(new_folders)) + ' new folders created ')
        script_info['messages'].append(str(len(new_folders)) + ' new folders created ')
        # return await asyncio.gather(*tasks)


async def delete_folder_url(session, url):
    async with session.delete(url) as r:
        if str(r.status)[0] != '2':
            script_info['errors'].append({'put_folder_url': r.status, 'url': url})
        return r.status


async def delete_folders(folders, headers):
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as async_session:
        for folder in folders:
            tasks.append(asyncio.create_task(delete_folder_url(async_session, f'https://cloud-api.yandex.net/v1/disk/resources?path={folder}&permanently=true')))
        deleted_folders = await asyncio.gather(*tasks)
        deleted_folders = [status_code for status_code in deleted_folders if str(status_code)[0] == '2']
        print(str(len(deleted_folders)) + ' folders deleted')
        script_info['messages'].append(str(len(deleted_folders)) + ' folders deleted')
        # return await asyncio.gather(*tasks)


def write_changes_on_file(script_info):
    with open(logging_file, 'r', encoding='utf-8') as f:
        data_dict = json.load(f)
        f.close()

    if data_dict:
        data_dict['data'].append(script_info)
        data_dict['len'] = len(data_dict['data'])

        if len(data_dict['data']) > loggin_data_len:
            data_dict['data'] = data_dict['data'][(len(data_dict['data']) - loggin_data_len):]

        with open(logging_file, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False)
            f.close()


def get_disk_info(session):
    r = session.get('https://cloud-api.yandex.net/v1/disk/')
    try:
        free_space = round((r.json()['total_space'] - r.json()['used_space']) / 1024**3, 2)
    except KeyError:
        free_space = None

    script_info['free_space_on_disk'] = free_space
    print('free space on disk --> ', free_space)


if __name__ == '__main__':
    start_time = time.time()
    script_info['cameras'] = cameras_to_write_on_disk

    files_to_load = find_all_files_on_pc_to_load(folder_with_cams)

    user = UserAgent().random
    headers = {'user-agent': user,
               'Authorization': token}

    if check_token(headers) is False:
        token = get_token(user)

    session = requests.Session()
    user = UserAgent().random

    session.headers.update(headers)

    try:
        for key in camera_dict:
            if key in cameras_to_write_on_disk:
                upload_data_from_camera(key, files_to_load, session, headers)

        script_info['is_success'] = True
    except Exception as err:
        print('SCRIP CRUSHED  --->' + str(err))
        script_info['errors'].append({'SCRIP CRUSHED': str(err)})
        # script_info['is_success'] = False

    script_info['date'] = datetime.datetime.today().strftime('%d-%m-%Y---%H:%M')
    script_info['script_time_work'] = time.time() - start_time
    get_disk_info(session)
    session.close()
    write_changes_on_file(script_info)
    print('script work time --> ', time.time() - start_time)





























