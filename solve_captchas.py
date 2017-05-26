#!/usr/bin/env python3

from asyncio import get_event_loop, sleep
from multiprocessing.managers import BaseManager
from time import time

from aiopogo import PGoApi, close_sessions, activate_hash_server, exceptions as ex
from aiopogo.auth_ptc import AuthPtc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from monocle import altitudes, sanitized as conf
from monocle.utils import get_device_info, get_address, randomize_point
from monocle.bounds import center


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

    for attempt in range(-1, conf.MAX_RETRIES):
        try:
            responses = await request.call()
            return responses['VERIFY_CHALLENGE'].success
        except (ex.HashServerException, ex.MalformedResponseException, ex.ServerBusyOrOfflineException) as e:
            if attempt == conf.MAX_RETRIES - 1:
                raise
            else:
                print('{}, trying again soon.'.format(e))
                await sleep(4)
        except (KeyError, TypeError):
            return False


async def main():
    try:
        class AccountManager(BaseManager): pass
        AccountManager.register('captcha_queue')
        AccountManager.register('extra_queue')
        manager = AccountManager(address=get_address(), authkey=conf.AUTHKEY)
        manager.connect()
        captcha_queue = manager.captcha_queue()
        extra_queue = manager.extra_queue()

        activate_hash_server(conf.HASH_KEY)

        driver = webdriver.Chrome()
        driver.set_window_size(803, 807)

        while not captcha_queue.empty():
            account = captcha_queue.get()
            username = account.get('username')
            location = account.get('location')
            if location and location != (0,0,0):
                lat = location[0]
                lon = location[1]
            else:
                lat, lon = randomize_point(center, 0.0001)

            try:
                alt = altitudes.get((lat, lon))
            except KeyError:
                alt = await altitudes.fetch((lat, lon))

            try:
                device_info = get_device_info(account)
                api = PGoApi(device_info=device_info)
                api.set_position(lat, lon, alt)

                authenticated = False
                try:
                    if account['provider'] == 'ptc':
                        api.auth_provider = AuthPtc()
                        api.auth_provider._access_token = account['auth']
                        api.auth_provider._access_token_expiry = account['expiry']
                        if api.auth_provider.check_access_token():
                            api.auth_provider.authenticated = True
                            authenticated = True
                except KeyError:
                    pass

                if not authenticated:
                    await api.set_authentication(username=username,
                                                 password=account['password'],
                                                 provider=account.get('provider', 'ptc'))

                request = api.create_request()
                await request.call()

                await sleep(.6)

                request.download_remote_config_version(platform=1, app_version=6301)
                request.check_challenge()
                request.get_hatched_eggs()
                request.get_inventory(last_timestamp_ms=account.get('inventory_timestamp', 0))
                request.check_awarded_badges()
                request.download_settings()
                responses = await request.call()
                account['time'] = time()

                challenge_url = responses['CHECK_CHALLENGE'].challenge_url
                timestamp = responses['GET_INVENTORY'].inventory_delta.new_timestamp_ms
                account['location'] = lat, lon
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
            except ex.AiopogoError as e:
                print('aiopogo error on {}: {}'.format(username, e))
                captcha_queue.put(account)
                await sleep(3)
            except Exception:
                captcha_queue.put(account)
                raise
    finally:
        try:
            driver.close()
            close_sessions()
        except Exception:
            pass

if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main())
