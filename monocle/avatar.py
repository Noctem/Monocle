from cyrandom import choice, randint

class MaleAvatar:
    hats = (
        "AVATAR_m_hat_default_0",
        "AVATAR_m_hat_default_1",
        "AVATAR_m_hat_default_2",
        "AVATAR_m_hat_default_3",
        "AVATAR_m_hat_default_4",
        "AVATAR_m_hat_default_5",
        "AVATAR_m_hat_empty")
    shirts = (
        "AVATAR_m_shirt_default_0",
        "AVATAR_m_shirt_default_1",
        "AVATAR_m_shirt_default_2",
        "AVATAR_m_shirt_default_3",
        "AVATAR_m_shirt_default_4",
        "AVATAR_m_shirt_default_5",
        "AVATAR_m_shirt_default_6",
        "AVATAR_m_shirt_default_7",
        "AVATAR_m_shirt_default_8",
        "AVATAR_m_shirt_default_2B")
    bags = (
        "AVATAR_m_backpack_default_0",
        "AVATAR_m_backpack_default_1",
        "AVATAR_m_backpack_default_2",
        "AVATAR_m_backpack_default_3",
        "AVATAR_m_backpack_default_4",
        "AVATAR_m_backpack_default_5",
        "AVATAR_m_backpack_empty")
    gloves = (
        "AVATAR_m_gloves_default_0",
        "AVATAR_m_gloves_default_1",
        "AVATAR_m_gloves_default_2",
        "AVATAR_m_gloves_default_3",
        "AVATAR_m_gloves_empty")
    socks = (
        "AVATAR_m_socks_default_0",
        "AVATAR_m_socks_default_1",
        "AVATAR_m_socks_default_2",
        "AVATAR_m_socks_default_3",
        "AVATAR_m_socks_empty")
    footwear = (
        "AVATAR_m_shoes_default_0",
        "AVATAR_m_shoes_default_1",
        "AVATAR_m_shoes_default_2",
        "AVATAR_m_shoes_default_3",
        "AVATAR_m_shoes_default_4",
        "AVATAR_m_shoes_default_5",
        "AVATAR_m_shoes_default_6",
        "AVATAR_m_shoes_empty")

    def __init__(self):
        self.avatar = 0
        self.avatar_hair = 'AVATAR_m_hair_default_{}'.format(randint(0, 5))
        self.avatar_eyes = 'AVATAR_m_eyes_{}'.format(randint(0, 4))
        self.skin = randint(0, 3)
        self.avatar_hat = choice(self.hats)
        self.avatar_shirt = choice(self.shirts)
        self.avatar_backpack = choice(self.bags)
        self.avatar_gloves = choice(self.gloves)
        self.avatar_pants = "AVATAR_m_pants_default_0"
        self.avatar_socks = choice(self.socks)
        self.avatar_shoes = choice(self.footwear)
        self.avatar_glasses = "AVATAR_m_glasses_empty"


class FemaleAvatar:
    hats = (
        "AVATAR_f_hat_default_A_0",
        "AVATAR_f_hat_default_A_1",
        "AVATAR_f_hat_default_A_2",
        "AVATAR_f_hat_default_A_3",
        "AVATAR_f_hat_default_A_4",
        "AVATAR_f_hat_default_A_5",
        "AVATAR_f_hat_default_B_0",
        "AVATAR_f_hat_default_B_1",
        "AVATAR_f_hat_empty")
    necklaces = (
        "AVATAR_f_necklace_heart_0",
        "AVATAR_f_necklace_star_0",
        "AVATAR_f_necklace_default_0",
        "AVATAR_f_necklace_default_1",
        "AVATAR_f_necklace_empty")
    bags = (
        "AVATAR_f_backpack_default_0",
        "AVATAR_f_backpack_default_1",
        "AVATAR_f_backpack_default_2",
        "AVATAR_f_backpack_default_3",
        "AVATAR_f_backpack_empty")
    gloves = (
        "AVATAR_f_gloves_default_0",
        "AVATAR_f_gloves_default_1",
        "AVATAR_f_gloves_default_2",
        "AVATAR_f_gloves_default_3",
        "AVATAR_f_gloves_empty")
    belts = (
        "AVATAR_f_belt_default_0",
        "AVATAR_f_belt_default_1",
        "AVATAR_f_belt_default_2",
        "AVATAR_f_belt_default_3",
        "AVATAR_f_belt_default_4",
        "AVATAR_f_belt_default_5",
        "AVATAR_f_belt_default_6",
        "AVATAR_f_belt_default_7",
        "AVATAR_f_belt_default_8",
        "AVATAR_f_belt_empty")
    bottoms = (
        "AVATAR_f_pants_miniskirt_wave_0",
        "AVATAR_f_pants_miniskirt_wave_1",
        "AVATAR_f_pants_miniskirt_wave_2",
        "AVATAR_f_pants_default_0",
        "AVATAR_f_pants_default_1",
        "AVATAR_f_pants_default_2",
        "AVATAR_f_pants_default_3",
        "AVATAR_f_pants_default_4",
        "AVATAR_f_pants_default_5")
    socks = (
        "AVATAR_f_socks_thighhighs_0",
        "AVATAR_f_socks_default_0",
        "AVATAR_f_socks_default_1",
        "AVATAR_f_socks_default_2",
        "AVATAR_f_socks_empty")
    footwear = (
        "AVATAR_f_shoes_default_0",
        "AVATAR_f_shoes_default_1",
        "AVATAR_f_shoes_default_2",
        "AVATAR_f_shoes_default_3",
        "AVATAR_f_shoes_default_4",
        "AVATAR_f_shoes_default_5",
        "AVATAR_f_shoes_default_6",
        "AVATAR_f_shoes_empty")

    def __init__(self):
        self.avatar = 1
        self.avatar_hair = 'AVATAR_f_hair_default_{}'.format(randint(0, 5))
        self.avatar_eyes = 'AVATAR_f_eyes_{}'.format(randint(0, 4))
        self.skin = randint(0, 3)
        self.avatar_hat = choice(self.hats)
        self.avatar_necklace = choice(self.necklaces)
        self.avatar_shirt = 'AVATAR_f_shirt_default_{}'.format(randint(0,8))
        self.avatar_backpack = choice(self.bags)
        self.avatar_gloves = choice(self.gloves)
        self.avatar_belt = choice(self.belts)
        self.avatar_pants = choice(self.bottoms)
        self.avatar_socks = choice(self.socks)
        self.avatar_shoes = choice(self.footwear)
        self.avatar_glasses = "AVATAR_f_glasses_empty"

def new():
    NewAvatar = choice((FemaleAvatar, MaleAvatar))
    return vars(NewAvatar())
