#!/usr/bin/env python3

from multiprocessing.managers import BaseManager
from pogo_async import PGoApi, exceptions as ex
from pogo_async.auth_ptc import AuthPtc
from pogo_async.session import Session
from asyncio import get_event_loop, sleep
from random import uniform
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import time

import socket

from monocle import config
from monocle.utils import random_altitude, get_device_info, get_address, LAT_MEAN, LON_MEAN


async def solve_captcha(url, api, driver, timestamp):
    driver.get(url)
    WebDriverWait(driver, 86400).until(EC.text_to_be_present_in_element_value((By.NAME, "g-recaptcha-response"), ""))
    driver.switch_to.frame(driver.find_element_by_xpath("//*/iframe[@title='recaptcha challenge']"))
    token = driver.find_element_by_id("recaptcha-token").get_attribute("value")
    request = api.create_request()
    request.verify_challenge(token=token)
    request.get_hatched_eggs()
    request.get_inventory(last_timestamp_ms=timestamp)
    request.check_awarded_badges()
    request.get_buddy_walked()
    request.check_challenge()

    response = await request.call()
    return response.get('responses', {}).get('VERIFY_CHALLENGE', {}).get('success', False)


async def main():
    try:
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

        driver = webdriver.Chrome()
        driver.set_window_size(803, 807)

        while not captcha_queue.empty():
            account = captcha_queue.get()
            username = account.get('username')
            location = account.get('location')
            if location and location != (0,0,0):
                lat = location[0]
                lon = location[1]
                try:
                    alt = location[2]
                except IndexError:
                    alt = random_altitude()
            else:
                lat = uniform(LAT_MEAN - 0.001, LAT_MEAN + 0.001)
                lon = uniform(LON_MEAN - 0.001, LON_MEAN + 0.001)
                alt = random_altitude()

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
                        api._auth_provider._login = True
                        authenticated = True

                if not authenticated:
                    await api.set_authentication(username=username,
                                                 password=account['password'],
                                                 provider=account.get('provider', 'ptc'))

                request = api.create_request()
                request.download_remote_config_version(platform=1, app_version=5302)
                request.check_challenge()
                request.get_hatched_eggs()
                request.get_inventory()
                request.check_awarded_badges()
                request.download_settings()
                response = await request.call()
                account['time'] = time()

                if response['status_code'] == 3:
                    print('{} appears to be banned.'.format(username))
                    continue

                responses = response['responses']
                challenge_url = responses['CHECK_CHALLENGE']['challenge_url']
                timestamp = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
                account['location'] = lat, lon, alt
                account['inventory_timestamp'] = timestamp
                if challenge_url == ' ':
                    account['captcha'] = False
                    print('No CAPTCHA was pending on {}.'.format(username))
                    extra_queue.put(account)
                else:
                    if await solve_captcha(challenge_url, api, driver, timestamp):
                        account['time'] = time()
                        account['captcha'] = False
                        print('Solved CAPTCHA for {}, putting back in rotation.'.format(username))
                        extra_queue.put(account)
                    else:
                        account['time'] = time()
                        print('Failed to solve for {}'.format(username))
                        captcha_queue.put(account)
            except KeyboardInterrupt:
                captcha_queue.put(account)
                break
            except KeyError:
                print('Unexpected or empty response for {}, putting back on queue.'.format(username))
                captcha_queue.put(account)
                try:
                    print(response)
                except Exception:
                    pass
                await sleep(3)
            except (ex.AuthException, ex.AuthTokenExpiredException) as e:
                print('Authentication error on {}: {}'.format(username, e))
                captcha_queue.put(account)
                await sleep(3)
            except ex.PgoapiError as e:
                print('pgoapi error on {}: {}'.format(username, e))
                captcha_queue.put(account)
                await sleep(3)
            except Exception:
                captcha_queue.put(account)
                raise
    finally:
        try:
            driver.close()
            Session.close()
        except Exception:
            pass

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())
