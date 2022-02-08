import requests
import csv
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta


def get_uuid(user, psw, base_url, name):
    params = {'friendlyName': name}
    url = base_url + "/api/v1/endpoints"
    endpoint = requests.get(url, auth=HTTPBasicAuth(user, psw), params=params).json()
    return endpoint[0]['uuid']


def get_time_series(user, psw, base_url, name, node, objects, attribute, start_time, stop_time, resample):
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_format_2 = "%Y-%m-%dT%H:%M:%SZ"

    url = base_url + "/api/v1/history/"
    uuid = get_uuid(user, psw, base_url, name)
    url += uuid + '/' + node
    for i in objects:
        url += '/' + i
    url += '/' + attribute

    finished = False

    start = datetime.strptime(start_time, date_format_2)
    stop = datetime.strptime(stop_time, date_format_2)

    max = 50000
    total = 0

    params = {"max": max}

    if resample is not None:
        params["resampleInterval"] = resample

    # generate the filename
    filename = uuid + '-' + node
    for i in objects:
        filename += '-' + i
    filename += '-' + attribute
    filename += '-' + datetime.now().strftime(date_format_2.replace(':', '-')) + '.csv'

    # prepare csv file
    file = open(filename, 'w')
    writer = csv.writer(file)
    headers = ['time', 'value']
    writer.writerow(headers)

    # request 50000 datapoint per loop
    while not finished:
        params['from'] = start.strftime(date_format)

        total += max
        print("Request " + str(total) + " datapoints")

        data = requests.get(url, auth=HTTPBasicAuth(user, psw), params=params).json()

        # exit if list is empty
        if not data:
            break

        for i in data:
            # get the datapoint time
            try:
                time = datetime.strptime(i['time'], date_format)
            except ValueError:
                time = datetime.strptime(i['time'], date_format_2)

            # check if stop time is reached
            if time < stop:
                datapoint = [i['time'], str(i['value'])]
                writer.writerow(datapoint)
            else:
                finished = True

        # get the last datapoint time
        try:
            start = datetime.strptime(data[-1]['time'], date_format)
        except ValueError:
            start = datetime.strptime(data[-1]['time'], date_format_2)

        # add a second to the next start time
        start = start + timedelta(seconds=1)

    file.close()


if __name__ == '__main__':
    cloudio_base_url = "http://example.com"
    cloudio_user = "USERNAME"
    cloudio_password = "PASSWORD"

    friendlyname = "myEndpoint"
    node = "nodeName"
    objects = ["objectName"]
    attribute = "attrName"

    start_time = "2021-08-01T00:00:00Z"  # format: YYYY-MM-DDThh:mm:ssZ
    stop_time = "2022-01-15T00:00:00Z"  # format: YYYY-MM-DDThh:mm:ssZ
    resample_interval = None  # example: 5s, 15m, 2h, 5d | if set to None, plain data

    get_time_series(cloudio_user, cloudio_password, cloudio_base_url, friendlyname,
            node, objects, attribute, start_time, stop_time, resample_interval)
