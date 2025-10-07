import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, unquote

from kafka import KafkaProducer
import json

from models import Player, Match, Participant, Unit, Trait

def game_datetime_to_vn_time(game_datetime_ms: int) -> str:
    """
    Chuyển đổi game_datetime (ms từ epoch) 
    sang thời gian thực tế tại Việt Nam (UTC+7).
    
    game_datetime_ms: thời gian theo định dạng game_datetime (ms từ epoch)
    """
    # chuyển ms -> s
    game_datetime_s = game_datetime_ms / 1000
    
    # tạo datetime UTC
    utc_dt = datetime.fromtimestamp(game_datetime_s, tz=timezone.utc)
    
    # đổi sang múi giờ VN (UTC+7)
    vn_dt = utc_dt.astimezone(timezone(timedelta(hours=7)))
    
    # trả về chuỗi theo format "YYYY-MM-DD HH:MM:SS"
    return vn_dt.strftime("%Y-%m-%d %H:%M:%S")

def vn_time_to_game_datetime(vn_time_str: str) -> int:
    """
    Chuyển đổi thời gian thực tế tại Việt Nam (UTC+7) 
    sang game_datetime (ms từ epoch).
    
    vn_time_str: chuỗi thời gian theo format "YYYY-MM-DD HH:MM:SS"
    """
    # parse chuỗi thành datetime VN
    vn_dt = datetime.strptime(vn_time_str, "%Y-%m-%d %H:%M:%S")
    vn_dt = vn_dt.replace(tzinfo=timezone(timedelta(hours=7)))
    
    # đổi sang UTC
    utc_dt = vn_dt.astimezone(timezone.utc)
    
    # chuyển thành timestamp (s), rồi nhân 1000 -> ms
    game_datetime_ms = int(utc_dt.timestamp() * 1000)
    return game_datetime_ms

def extract_match_data(match_info_json):
    match_id = match_info_json['metadata']['match_id']
    info = match_info_json['info']
    game_creation = game_datetime_to_vn_time(info['gameCreation'])
    game_datetime = game_datetime_to_vn_time(info['game_datetime'])  # chuỗi thời gian VN
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
    print(f"Error get match info with status code {response.status_code}")
    return None

def get_match_history(puuid, api_key, start = 0, endTime = None, startTime = None, count = 20):
    """
- puuid: PUUID của người chơi.
- start (int, tùy chọn): Chỉ số bắt đầu của danh sách kết quả. Mặc định là 0.
- endTime (str, tùy chọn): Thời điểm kết thúc theo format YYYY-MM-DD HH:MM:SS. Dùng để lọc các trận đấu diễn ra trước thời điểm này.
- startTime (str, tùy chọn): Thời điểm bắt đầu theo format YYYY-MM-DD HH:MM:SS. Lưu ý: hệ thống chỉ lưu timestamp từ ngày 16/06/2021. Các trận đấu trước thời điểm này sẽ không được trả về nếu sử dụng bộ lọc startTime.
- count (int, tùy chọn): Số lượng ID trận đấu cần lấy. Mặc định là 20.
    """
    startTime_ms = vn_time_to_game_datetime(startTime) if startTime else None
    endTime_ms = vn_time_to_game_datetime(endTime) if endTime else None
    
    params_str = ""
    if start != 0:
        params_str += f"start={start}&"
    if startTime_ms:
        params_str += f"startTime={startTime_ms}&"
    if endTime_ms:
        params_str += f"endTime={endTime_ms}&"
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
                list_match_json.append(match_info_json)
        return list_match_json
    else:
        raise RuntimeError(f"Error get match history with status code {response.status_code}")
            
                



load_dotenv()  # tự động đọc .env ở cwd
api_key = os.getenv("API_RIOT")
if not api_key:
    raise RuntimeError("Update API key for Riot Games in .env file dayly")

# take name and tag from op.gg
url = "https://op.gg/vi/tft/leaderboards/ranked?region=vn"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://op.gg/vi",
    "Connection": "keep-alive",
}

response = requests.get(url, headers = headers)

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
        player_puuid = response.json()['puuid']
        print(f"Player PUUID: {player_puuid}")

        # lay ra lich su 3 tran dau gan nhat
        list_match_json = get_match_history(player_puuid, api_key, count = 2)
        
        # Khoi tao kafka producer
        # Kết nối tới Kafka cluster (chỉ cần 1 broker)
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],  # broker chính, Kafka tự biết các broker còn lại trong cluster
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # chuyển dict → JSON → bytes
        )

        # Topic bạn muốn gửi tới
        topic_name = "match_history"

        # Gửi từng record
        for match in list_match_json:
            future = producer.send(topic_name, value=match)
            result = future.get(timeout=10)
            print("✅ Message sent to:", result.topic, "partition:", result.partition, "offset:", result.offset)

        # Đảm bảo gửi hết message trong buffer
        producer.flush()

        print("✅ All match records have been sent to Kafka!")

        break
else:
    raise RuntimeError(f"Error from op.gg with {response.status_code}")