#!/usr/bin/env python3

import pickle
from queue import Queue
from multiprocessing.managers import BaseManager
from pgoapi import (
    exceptions as pgoapi_exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)
from pgoapi.auth_ptc import AuthPtc
from random import uniform
from utils import random_altitude, get_device_info
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from config import MAP_START, MAP_END
from sys import exit

DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"

def resolve_captcha(url, api, driver, timestamp):
    driver.get(url)
    WebDriverWait(driver, 86400).until(EC.text_to_be_present_in_element_value((By.NAME, "g-recaptcha-response"), ""))
    driver.switch_to.frame(driver.find_element_by_xpath("//*/iframe[@title='recaptcha challenge']"))
    token = driver.find_element_by_id("recaptcha-token").get_attribute("value")
    request = api.create_request()
    request.verify_challenge(token=token)
    request.check_challenge()
    request.get_hatched_eggs()
    request.get_inventory(last_timestamp_ms = timestamp)
    request.check_awarded_badges()
    request.download_settings(hash=DOWNLOAD_HASH)
    request.get_buddy_walked()

    response = request.call()
    success = response.get('responses', {}).get('VERIFY_CHALLENGE', {}).get('success', False)
    return success

with open('accounts.pickle', 'rb') as f:
    ACCOUNTS = pickle.load(f)

captcha_queue = Queue()
extra_queue = Queue()
class AccountManager(BaseManager): pass
AccountManager.register('captcha_queue', callable=lambda:captcha_queue)
AccountManager.register('extra_queue', callable=lambda:extra_queue)
manager = AccountManager(address='queue.sock', authkey=b'monkeys')
manager.connect()
captcha_queue = manager.captcha_queue()
extra_queue = manager.extra_queue()


middle_lat = (MAP_START[0] + MAP_END[0]) / 2
middle_lon = (MAP_START[1] + MAP_END[1]) / 2
middle_alt = random_altitude()

driver = webdriver.Chrome()
driver.set_window_size(803, 807)

while not captcha_queue.empty():
    username = captcha_queue.get()
    account = ACCOUNTS[username]
    location = account.get('location')
    if location and location != (0,0,0):
        lat = location[0]
        lon = location[1]
        alt = location[2]
    else:
        lat = uniform(middle_lat - 0.001, middle_lat + 0.001)
        lon = uniform(middle_lon - 0.001, middle_lon + 0.001)
        alt = uniform(middle_alt - 10, middle_alt + 10)

    try:
        device_info = get_device_info(account)
        api = PGoApi(device_info=device_info)
        api.set_position(lat, lon, alt)

        authenticated = False
        if account.get('provider') == 'ptc' and account.get('refresh'):
            api._auth_provider = AuthPtc()
            api._auth_provider.set_refresh_token(account.get('refresh'))
            api._auth_provider._access_token = account.get('auth')
            api._auth_provider._access_token_expiry = account.get('expiry')
            if api._auth_provider.check_access_token():
                print(username, 'already authenticated')
                api._auth_provider._login = True
                authenticated = True

        if not authenticated:
            print(username)
            api.set_authentication(username=username,
                                   password=account.get('password'),
                                   provider=account.get('provider'))

        request = api.create_request()
        request.download_remote_config_version(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory()
        request.check_awarded_badges()
        request.download_settings()
        response = request.call()

        responses = response.get('responses', {})
        challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        download_hash = responses.get('DOWNLOAD_SETTINGS', {}).get('hash')
        if download_hash and download_hash != DOWNLOAD_HASH:
            DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"
        timestamp = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
        if challenge_url == ' ':
            extra_queue.put(username)
        else:
            if resolve_captcha(challenge_url, api, driver, timestamp):
                extra_queue.put(username)
            else:
                print('failure')
                captcha_queue.put(username)
    except KeyboardInterrupt:
        captcha_queue.put(username)
        break
    except (WebDriverException, AttributeError):
        captcha_queue.put(username)
        break
    except Exception as e:
        captcha_queue.put(username)
        raise

try:
    driver.close()
except Exception:
    pass
