Installation
============

Installation
------------

Using pip install:

.. code-block:: console

    pip install git+https://github.com/willem0boone/Edito_resampling_datasets


Configuration
-------------

For IO with your S3 storage, the :class:`resampling.object_store.ObjectStore`
class is used. The initiation of this class requires your S3 credentials.

You can configure this package by storing your S3 credentials in a config file.
Doing so, you can use :func:`resampling.my_store.get_my_store` to  create an
instance of :class:`resampling.object_store.ObjectStore`.

:func:`resampling.my_store.get_my_store` will look by default at a
config file stored at:

    resampling/config/config.toml

.. code-block:: python

    my_store = get_my_store()

However, you can also provide your own config.toml file.

.. code-block:: python

    my_store = get_my_store('path/to/my_config.toml')


The content of the file should look like this:

.. code-block:: python

    endpoint_url=''
    bucket=''
    aws_access_key_id=''
    aws_secret_access_key=''
    aws_session_token=''


Alternatively, you can initiate :class:`resampling.object_store.ObjectStore`
manually as follows:

.. code-block:: python

    my_object_store = ObjectStore(
        endpoint_url='str',
        aws_access_key_id='str',
        aws_secret_access_key='str',
        aws_session_token='str',
        bucket='str',
    )

