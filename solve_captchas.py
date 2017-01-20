#!/usr/bin/env python3

import config
import socket
from queue import Queue
from multiprocessing.managers import BaseManager
from pgoapi import (
    exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)
from pgoapi.auth_ptc import AuthPtc
from random import uniform
from utils import random_altitude, get_device_info, get_address
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep, time


def resolve_captcha(url, api, driver, timestamp):
    driver.get(url)
    WebDriverWait(driver, 86400).until(EC.text_to_be_present_in_element_value((By.NAME, "g-recaptcha-response"), ""))
    driver.switch_to.frame(driver.find_element_by_xpath("//*/iframe[@title='recaptcha challenge']"))
    token = driver.find_element_by_id("recaptcha-token").get_attribute("value")
    request = api.create_request()
    request.verify_challenge(token=token)
    request.check_challenge()
    request.get_hatched_eggs()
    request.get_inventory(last_timestamp_ms=timestamp)
    request.check_awarded_badges()
    request.get_buddy_walked()

    response = request.call()
    success = response.get('responses', {}).get('VERIFY_CHALLENGE', {}).get('success', False)
    return success

if hasattr(config, 'AUTHKEY'):
    authkey = config.AUTHKEY
else:
    authkey = b'm3wtw0'

if hasattr(config, 'HASH_KEY'):
    HASH_KEY = config.HASH_KEY
else:
    HASH_KEY = None

class AccountManager(BaseManager): pass
AccountManager.register('captcha_queue')
AccountManager.register('extra_queue')
manager = AccountManager(address=get_address(), authkey=authkey)
manager.connect()
captcha_queue = manager.captcha_queue()
extra_queue = manager.extra_queue()


middle_lat = (config.MAP_START[0] + config.MAP_END[0]) / 2
middle_lon = (config.MAP_START[1] + config.MAP_END[1]) / 2
middle_alt = random_altitude()

driver = webdriver.Chrome()
driver.set_window_size(803, 807)

while not captcha_queue.empty():
    account = captcha_queue.get()
    username = account.get('username')
    location = account.get('location')
    if location and location != (0,0,0):
        lat, lon, alt = location
    else:
        lat = uniform(middle_lat - 0.001, middle_lat + 0.001)
        lon = uniform(middle_lon - 0.001, middle_lon + 0.001)
        alt = uniform(middle_alt - 10, middle_alt + 10)

    try:
        device_info = get_device_info(account)
        api = PGoApi(device_info=device_info)
        if HASH_KEY:
            api.activate_hash_server(HASH_KEY)
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
        request.download_remote_config_version(platform=1, app_version=5301)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory()
        request.check_awarded_badges()
        request.download_settings()
        response = request.call()
        account['time'] = time()

        responses = response.get('responses', {})
        challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        timestamp = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
        account['location'] = lat, lon, alt
        account['inventory_timestamp'] = timestamp
        if challenge_url == ' ':
            account['captcha'] = False
            extra_queue.put(account)
        else:
            if resolve_captcha(challenge_url, api, driver, timestamp):
                account['time'] = time()
                account['captcha'] = False
                extra_queue.put(account)
            else:
                account['time'] = time()
                print('failure')
                captcha_queue.put(account)
    except (KeyboardInterrupt, Exception) as e:
        print(e)
        captcha_queue.put(account)
        break
    except (exceptions.AuthException, exceptions.AuthTokenExpiredException, exceptions.AuthTokenExpiredException):
        print('Authentication error')
        captcha_queue.put(account)
        sleep(2)

try:
    driver.close()
except Exception:
    pass
