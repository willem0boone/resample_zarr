from resampling.object_store import ObjectStore
from resampling._config import Config
from pprint import pprint


def get_my_store(config_file=None):
    """
    A function that initiates an ObjectStore instance based on credentials
    stored in the config file. If no config file is given, it defaults to
    'config/config.toml'.

    :param config_file: Optional; Path to the configuration file. If not
        provided, it defaults to 'config/config.toml'.

    :return: ObjectStore instance based on credentials saved in the
        configuration file.

    :rtype: ObjectStore
    """
    try:
        # Pass the config_file if given, else use the default behavior
        config = Config(config_file=config_file)
    except Exception as e:
        print("Error loading configuration")
        raise

    # Display the loaded configuration for debugging purposes
    # print("Credentials for object store:")
    # pprint(config.settings)

    # Initialize ObjectStore with credentials from the config
    my_store = ObjectStore(
        endpoint_url=config.settings.endpoint_url,
        aws_access_key_id=config.settings.aws_access_key_id,
        aws_secret_access_key=config.settings.aws_secret_access_key,
        aws_session_token=config.settings.aws_session_token,
        bucket=config.settings.bucket,
    )

    return my_store




