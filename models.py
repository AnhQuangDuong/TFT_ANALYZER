from dataclasses import dataclass
from datetime import datetime
from typing import List

@dataclass
class Player:
    player_id: str
    summoner_name: str
    rank: str

@dataclass
class Match:
    match_id: str
    game_creation: datetime
    game_datetime: datetime
    tft_game_type: str
    tft_set_core_name: str

@dataclass
class Participant:
    match_id: str
    player_id: str
    placement: int
    level: int
    last_round: int

@dataclass
class Unit:
    match_id: str
    player_id: str
    character_id: str
    rarity: int # giá tiền (tính từ 0)
    tier: int  # số sao
    item_names: List[str]

@dataclass
class Trait:
    match_id: str
    player_id: str
    trait_name: str
    tier_current: int
    tier_total: int
    num_units: int