from random import choice
from uuid import uuid4

from config import ACCOUNTS

def get_device():
    devices = ('iPhone5,1', 'iPhone5,2', 'iPhone5,3', 'iPhone5,4', 'iPhone6,1', 'iPhone6,2', 'iPhone7,1', 'iPhone7,2', 'iPhone8,1', 'iPhone8,2', 'iPhone8,4', 'iPhone9,1', 'iPhone9,2', 'iPhone9,3', 'iPhone9,4')
    ios8 = ('8.0', '8.0.1', '8.0.2', '8.1', '8.1.1', '8.1.2', '8.1.3', '8.2', '8.3', '8.4', '8.4.1')
    ios9 = ('9.0', '9.0.1', '9.0.2', '9.1', '9.2', '9.2.1', '9.3', '9.3.1', '9.3.2', '9.3.3', '9.3.4', '9.3.5')
    ios10 = ('10.0', '10.0.1', '10.0.2', '10.0.3', '10.1', '10.1.1')

    model = choice(devices)
    device_id = uuid4().hex

    if model in ('iPhone9,1', 'iPhone9,2',
                 'iPhone9,3', 'iPhone9,4'):
        version = choice(ios10)
    elif model in ('iPhone8,1', 'iPhone8,2', 'iPhone8,4'):
        version = choice(ios9 + ios10)
    else:
        version = choice(ios8 + ios9 + ios10)

    return model, version, device_id

accounts = []

for account in ACCOUNTS:
    accounts.append(account + get_device())

print(accounts)
