import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, unquote
import time

from kafka import KafkaProducer
import json

from models import Player, Match, Participant, Unit, Trait

def game_datetime_to_vn_time(game_datetime: int, type_time: str) -> str:
    """
    Chuyển đổi game_datetime (ms từ epoch) 
    sang thời gian thực tế tại Việt Nam (UTC+7).
    
    game_datetime: thời gian theo định dạng game_datetime
    type_time: 's' hoặc 'ms'
    """
    if type_time == 'ms':
        # chuyển ms -> s
        game_datetime_s = game_datetime / 1000
    elif type_time == 's':
        game_datetime_s = game_datetime
    else:
        raise ValueError("type_time must be 's' or 'ms'")

    # tạo datetime UTC
    utc_dt = datetime.fromtimestamp(game_datetime_s, tz=timezone.utc)
    
    # đổi sang múi giờ VN (UTC+7)
    vn_dt = utc_dt.astimezone(timezone(timedelta(hours=7)))
    
    # trả về chuỗi theo format "YYYY-MM-DD HH:MM:SS"
    return vn_dt.strftime("%Y-%m-%d %H:%M:%S")

def vn_time_to_game_datetime(vn_time_str: str, type_time: str) -> int:
    """
    Chuyển đổi thời gian thực tế tại Việt Nam (UTC+7) 
    sang game_datetime (s hoặc ms từ epoch).
    
    vn_time_str: chuỗi thời gian theo format "YYYY-MM-DD HH:MM:SS"
    type_time: 's' hoặc 'ms'
    """
    # parse chuỗi thành datetime VN
    vn_dt = datetime.strptime(vn_time_str, "%Y-%m-%d %H:%M:%S")
    vn_dt = vn_dt.replace(tzinfo=timezone(timedelta(hours=7)))
    
    # đổi sang UTC
    utc_dt = vn_dt.astimezone(timezone.utc)
    
    # chuyển thành timestamp (s hoặc ms)
    if type_time == 's':
        game_datetime = int(utc_dt.timestamp())
    elif type_time == 'ms':
        game_datetime = int(utc_dt.timestamp() * 1000)
    else:
        raise ValueError("type_time must be 's' or 'ms'")
    return game_datetime

def extract_match_data(match_info_json):
    match_id = match_info_json['metadata']['match_id']
    info = match_info_json['info']
    game_creation = game_datetime_to_vn_time(info['gameCreation'], 'ms')
    game_datetime = game_datetime_to_vn_time(info['game_datetime'], 'ms')  # chuỗi thời gian VN
    tft_game_type = info['tft_game_type']
    tft_set_core_name = info['tft_set_core_name']
    
    match = Match(
        match_id=match_id,
        game_creation=game_creation,
        game_datetime=game_datetime,
        tft_game_type=tft_game_type,
        tft_set_core_name=tft_set_core_name
    )
    
    participants = []
    units = []
    traits = []
    
    for participant_info in info['participants']:
        player_id = participant_info['puuid']
        placement = participant_info['placement']
        level = participant_info['level']
        last_round = participant_info['last_round']
        
        participant = Participant(
            match_id=match_id,
            player_id=player_id,
            placement=placement,
            level=level,
            last_round=last_round
        )
        participants.append(participant)
        
        for unit_info in participant_info.get('units', []):
            character_id = unit_info['character_id']
            rarity = unit_info['rarity']
            tier = unit_info['tier']
            item_names = [item for item in unit_info.get('itemNames', [])]
            
            unit = Unit(
                match_id=match_id,
                player_id=player_id,
                character_id=character_id,
                rarity=rarity,
                tier=tier,
                item_names=item_names
            )
            units.append(unit)
        
        for trait_info in participant_info.get('traits', []):
            trait_name = trait_info['name']
            num_units = trait_info['num_units']
            tier_current = trait_info['tier_current']
            tier_total = trait_info['tier_total']
            
            trait = Trait(
                match_id=match_id,
                player_id=player_id,
                trait_name=trait_name,
                num_units=num_units,
                tier_current=tier_current,
                tier_total=tier_total
            )
            traits.append(trait)
    
    return match, participants, units, traits

def get_match_information(match_id, api_key):
    request_url_get_match_info = f"https://sea.api.riotgames.com/tft/match/v1/matches/{match_id}?api_key={api_key}"
    request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com"
    }
    response = requests.get(request_url_get_match_info, headers = request_headers)
    if response.status_code == 200:
        match_info = response.json()
        return match_info
    elif response.status_code == 429:
        print("Limit exceeded. We will wait for 2 minutes before retrying...")
        time.sleep(120)
        response = requests.get(request_url_get_match_info, headers = request_headers)
        # thu gui lai trong 3 lan
        for _ in range(3):
            if response.status_code == 200:
                match_info = response.json()
                return match_info
        raise RuntimeError(f"Error get match info after retrying 3 times with status code {response.status_code}")

    elif response.status_code == 401:
        raise RuntimeError(f"Please update your api_key in .env file")
    else:
        raise RuntimeError(f"Error get match history with status code {response.status_code}")

def get_match_history(puuid, player_rank, api_key, start = 0, endTime = None, startTime = None, count = 20):
    """
- puuid: PUUID của người chơi.
- player_rank: Hạng của người chơi (ví dụ: "Challenger", "Grandmaster", "Master", v.v.)
- api_key: Khóa API của Riot Games.
- start (int, tùy chọn): Chỉ số bắt đầu của danh sách kết quả. Mặc định là 0.
- endTime (str, tùy chọn): Thời điểm kết thúc theo format YYYY-MM-DD HH:MM:SS. Dùng để lọc các trận đấu diễn ra trước thời điểm này.
- startTime (str, tùy chọn): Thời điểm bắt đầu theo format YYYY-MM-DD HH:MM:SS. Lưu ý: hệ thống chỉ lưu timestamp từ ngày 16/06/2021. Các trận đấu trước thời điểm này sẽ không được trả về nếu sử dụng bộ lọc startTime.
- count (int, tùy chọn): Số lượng ID trận đấu cần lấy. Mặc định là 20.
    """
    startTime_s = vn_time_to_game_datetime(startTime, 's') if startTime else None
    endTime_s = vn_time_to_game_datetime(endTime, 's') if endTime else None
    
    params_str = ""
    if start != 0:
        params_str += f"start={start}&"
    if startTime_s:
        params_str += f"startTime={startTime_s}&"
    if endTime_s:
        params_str += f"endTime={endTime_s}&"
    if count != 20:
        params_str += f"count={count}&"
    params_str += f"api_key={api_key}"
    request_url_get_match_history = f"https://sea.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?{params_str}"

    request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com"
    }

    response = requests.get(request_url_get_match_history, headers = request_headers)
    if response.status_code == 200:
        list_match_ids = response.json()
        list_match_json = []
        for match_id in list_match_ids:
            match_info_json = get_match_information(match_id, api_key)
            if match_info_json:
                #match, participants, units, traits = extract_match_data(match_info_json)
                match_info_json['info']['player_rank'] = player_rank
                list_match_json.append(match_info_json)
        return list_match_json
    elif response.status_code == 429:
        print("Limit exceeded. We will wait for 2 minutes before retrying...")
        time.sleep(120)
        # try 3 times more
        for _ in range(3):
            response = requests.get(request_url_get_match_history, headers = request_headers)
            if response.status_code == 200:
                list_match_ids = response.json()
                list_match_json = []
                for match_id in list_match_ids:
                    match_info_json = get_match_information(match_id, api_key)
                    if match_info_json:
                        #match, participants, units, traits = extract_match_data(match_info_json)
                        list_match_json.append(match_info_json)
                return list_match_json
        raise RuntimeError(f"Error get match history after retrying 3 times with status code {response.status_code}")
    elif response.status_code == 401:
        raise RuntimeError(f"Please update your api_key in .env file")
    else:
        raise RuntimeError(f"Error get match history with status code {response.status_code}")
            
                



load_dotenv()  # tự động đọc .env ở cwd
api_key = os.getenv("API_RIOT")
if not api_key:
    raise RuntimeError("Update API key for Riot Games in .env file dayly")

# take name and tag from op.gg
root_url = "https://op.gg/vi/tft/leaderboards/ranked?region=vn&page="

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://op.gg/vi",
    "Connection": "keep-alive",
}

# Khoi tao kafka producer
# Kết nối tới Kafka cluster (chỉ cần 1 broker)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],  # broker chính, Kafka tự biết các broker còn lại trong cluster
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # chuyển dict → JSON → bytes
)

# Topic bạn muốn gửi tới
topic_name = "match_history"

num_crawled_players = 1000
current_crawled_players = 0

idx_page = 1

while True: # Crawl until getting total 1000 players 
    response = requests.get(root_url + str(idx_page), headers = headers)
    idx_page+= 1

    target_class = "flex items-center gap-2 md:min-w-max"
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        for player in soup.find_all(attrs = {"class": target_class}):
            player_name = player.find("strong").text
            player_tag = player.find("em").text[1:]
            player_rank = soup.find(id=player_name+"-"+player_tag).find_all("span")[-1].text

            print(f"Player Name: {player_name}, Tag: {player_tag}, Rank: {player_rank}")


            header_request = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://developer.riotgames.com"
            }

            player_name_encoded = quote(player_name)
            player_tag_encoded = quote(player_tag)
            request_url_get_puuid = f"https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{player_name_encoded}/{player_tag_encoded}?api_key={api_key}"
            
            response = requests.get(request_url_get_puuid, headers = header_request)
            if response.status_code == 200:
                pass
            elif response.status_code == 429:
                print("Limit exceeded. We will wait for 2 minutes before retrying...")
                time.sleep(120)
                response = requests.get(request_url_get_puuid, headers = header_request)
                # thu gui lai trong 3 lan
                for _ in range(3):
                    if response.status_code == 200:
                        break
                if response.status_code != 200:
                    raise RuntimeError(f"Error get PUUID from Riot after retrying 3 times with status code {response.status_code}")
            elif response.status_code == 401:
                raise RuntimeError(f"Please update your api_key in .env file")
            elif response.status_code == 404:
                print(f"Player {player_name}#{player_tag} not found in Riot database.")
                continue
            else:
                raise RuntimeError(f"Error get PUUID from Riot with status code {response.status_code}")
            
            current_crawled_players += 1
            player_puuid = response.json()['puuid']
            print(f"Player PUUID: {player_puuid}")

            list_match_json = get_match_history(player_puuid, player_rank, api_key, startTime = "2025-08-01 00:00:00" ,count = 10)
            

            # Gửi từng record
            for match in list_match_json:
                #print(match)
                future = producer.send(topic_name, value=match)
                result = future.get(timeout=10)
                print("✅ Message sent to:", result.topic, "partition:", result.partition, "offset:", result.offset)

            # Đảm bảo gửi hết message trong buffer
            producer.flush()

            print(f"✅ All match records of {player_name}#{player_tag} have been sent to Kafka!")
            print("===============================================")
            
    else:
        raise RuntimeError(f"Error from op.gg with {response.status_code}")
    
    if current_crawled_players == num_crawled_players:
        break