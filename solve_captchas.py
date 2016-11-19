#!/usr/bin/env python

from queue import Queue
from multiprocessing.managers import SyncManager
from pgoapi import (
    exceptions as pgoapi_exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)
from random import uniform
from utils import get_altitude
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from config import MAP_START, MAP_END
from sys import exit

def get_device_info(account):
    hardware = {'iPhone5,1': 'N41AP',
                'iPhone5,2': 'N42AP',
                'iPhone5,3': 'N48AP',
                'iPhone5,4': 'N49AP',
                'iPhone6,1': 'N51AP',
                'iPhone6,2': 'N53AP',
                'iPhone7,1': 'N56AP',
                'iPhone7,2': 'N61AP',
                'iPhone8,1': 'N71AP',
                'iPhone8,2': 'N66AP',
                'iPhone8,4': 'N69AP'}
    device_info = {'device_brand': 'Apple',
                   'device_model': 'iPhone',
                   'hardware_manufacturer': 'Apple',
                   'firmware_brand': 'iPhone OS'
                   }
    device_info['device_comms_model'] = account[3]
    device_info['hardware_model'] = hardware[account[3]]
    device_info['firmware_type'] = account[4]
    device_info['device_id'] = account[5]
    return device_info

def resolve_captcha(url, api, driver):
    try:
        driver.get(url)
        WebDriverWait(driver, 86400).until(EC.text_to_be_present_in_element_value((By.NAME, "g-recaptcha-response"), ""))
        driver.switch_to.frame(driver.find_element_by_xpath("//*/iframe[@title='recaptcha challenge']"))
        token = driver.find_element_by_id("recaptcha-token").get_attribute("value")
        response = api.verify_challenge(token=token)
        success = response.get('responses', {}).get('VERIFY_CHALLENGE', {}).get('success', False)
        return success
    except Exception:
        return False


captcha = Queue()
extra = Queue()
class QueueManager(SyncManager): pass
QueueManager.register('captcha_queue', callable=lambda:captcha)
QueueManager.register('extra_queue', callable=lambda:extra)
manager = QueueManager(address='queue.sock', authkey=b'monkeys')
manager.connect()
captcha_queue = manager.captcha_queue()
extra_queue = manager.extra_queue()

middle_lat = (MAP_START[0] + MAP_END[0]) / 2
middle_lon = (MAP_START[1] + MAP_END[1]) / 2
middle_alt = get_altitude((middle_lat, middle_lon))

driver = webdriver.Chrome()
driver.set_window_size(803, 807)

while not captcha_queue.empty():
    lat = uniform(middle_lat - 0.001, middle_lat + 0.001)
    lon = uniform(middle_lon - 0.001, middle_lon + 0.001)
    alt = uniform(middle_alt - 10, middle_alt + 10)
    account = captcha_queue.get()

    try:
        device_info = get_device_info(account)
        api = PGoApi(device_info=device_info)
        api.set_position(lat, lon, alt)
        api.set_authentication(username=account[0], password=account[1], provider=account[2])
        response_dict = api.check_challenge()
        challenge_url = response_dict.get('responses', {}).get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        if challenge_url != ' ':
            if resolve_captcha(challenge_url, api, driver):
                extra_queue.put(account)
            else:
                captcha_queue.put(account)
    except KeyboardInterrupt:
        captcha_queue.put(account)
        driver.close()
        exit(0)
    except Exception:
        captcha_queue.put(account)

driver.close()
