Installation
============

Configuration
-------------

For IO with your S3 storage, the :class:`resampling.object_store.ObjectStore` class is
used. The initiation of this class requires your S3 credentials. You can
configure this package by storing your S3 credentials in a config file stored
at:

    resampling/config/config.toml

The content of the file should look like this:

.. code-block:: python

    endpoint_url=''
    bucket=''
    aws_access_key_id=''
    aws_secret_access_key=''
    aws_session_token=''

When providing the configuration, you can call :func:`resampling.my_store.get_my_store` to
create an ObjectStore instance using the credentials provided in the
configuration.

Alternatively, you can call :class:`resampling.object_store.ObjectStore` and initiate it
providing your credentials yourself.

Pip install
-----------

To be developed, maybe, (n)ever.

