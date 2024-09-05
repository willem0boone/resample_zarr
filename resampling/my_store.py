from resampling.object_store import ObjectStore
from resampling._config import Config
from pprint import pprint


def get_my_store():
    """
    A function that will read credentials from the config/config.toml file and
    create a ObjectStore instance based on this information. Use this if you
    want to autoconfigure your store management.

    :return: ObjectStore instance based on credentials saved in
        config/config.toml

    :rtype: ObjectStore
    """
    try:
        config = Config()
    except Exception as e:
        print("Error in config")
        raise

    print("Credentials for object store:")
    pprint(config.settings)

    my_store = ObjectStore(
        endpoint_url=config.settings.endpoint_url,
        aws_access_key_id=config.settings.aws_access_key_id,
        aws_secret_access_key=config.settings.aws_secret_access_key,
        aws_session_token=config.settings.aws_session_token,
        bucket=config.settings.bucket,)

    return my_store


x = get_my_store()


