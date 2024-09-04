import os
import toml
from dacite import from_dict
from dataclasses import dataclass


@dataclass
class Settings:
    """
    Settings class.
    """
    bucket: str
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


class Config:
    """
    Config class.
    """

    def __init__(self):
        self._config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "config.toml")
        self._set_config(self._config_file)

    @property
    def settings(self) -> Settings:
        """
        Getter for self._settings.
        :return: Settings.
        """
        return self._settings

    def _set_config(self, filename):
        """
        Set the config.
        :param filename: string, path to toml file.
        :return: None
        """
        self._settings = from_dict(data_class=Settings,
                                   data=dict(toml.load(filename)))

    @property
    def config_file(self) -> str:
        """
        Getter for self._config_file.
        :return: path to config file.
        """
        return self._config_file
