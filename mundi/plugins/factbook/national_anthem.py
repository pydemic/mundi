from sidekick.properties import property

from .config import COUNTRIES

# TODO: https://pythonbasics.org/python-play-sound/
# There is also a sample.mp3 corresponding to some unknown country.
AUDIO_URL = "https://www.cia.gov/library/publications/the-world-factbook/attachments" \
            "/audios/original/{code}.mp3"


class NationalAnthemMixin:
    """
    Expose a national anthem audio files and interfaces.

    Data is retrieved from the internet and cached locally. This prevent us from
    storing 263mb of audio files files in this Python package.
    """
    code: str

    national_anthem_url = property(lambda c: AUDIO_URL.format(code=c.code))
    national_anthem_file = NotImplemented
    national_anthem = NotImplemented


def load_all_audio():
    """
    Load audio files from all countries in the database into cache.
    """
    return [load_audio(code) for code in COUNTRIES]


def load_audio(code):
    """
    Load audio file for the given country and store it in cache.
    """
    raise NotImplementedError
