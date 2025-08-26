import re
import datetime

ID2ARTIST = {
    'a': "向晚大魔王",
    'b': "贝拉kira",
    'c': "珈乐Carol",
    'd': "嘉然今天吃什么",
    'e': "乃琳Queen",
    'f': "A-SOUL_Official"
}
ARTIST2ID = {v: k for k, v in ID2ARTIST.items()}


def resolve_live_raw_name(name: str):
    m = re.match(r'^\[(\d{6})] (.*) - (\S*)$', name)
    return {'date': m.group(1), 'title': m.group(2), 'artist': m.group(3)} if m else None


def build_live_raw_name(date: str, title: str, artist: str):
    return f"[{date}] {title} - {artist}"


# def build_compact_name(date: str, artist: str, path: str, index: str):
#     output = [date, ARTIST2ID[artist]]
#     folder = path.split('/', maxsplit=1)[0]
#     {'source': 's', 'transcoded': 't'}.get(folder.lower(), 'o')
#     return ''.join(output)

# def get_start_time(paths: list):
#     datetime.datetime.strptime(paths[0].split('/')[-1].split('_')[1], '%Y%m%d%H%M%S')
#     return int(paths[0].split('/')[-1].split('_')[0])
