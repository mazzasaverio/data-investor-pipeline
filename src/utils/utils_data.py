import ssl
import json
import certifi
import urllib.error
import urllib.request as urlopen
from loguru import logger


def get_jsonparsed_data(url, timeout=2):
    try:
        context = ssl.create_default_context(cafile=certifi.where())
        with urlopen(url, context=context, timeout=timeout) as response:
            data = response.read().decode("utf-8")
        return json.loads(data)
    except urllib.error.URLError as e:
        logger.error(f"URL error: {e.reason}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e.msg}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        return None
