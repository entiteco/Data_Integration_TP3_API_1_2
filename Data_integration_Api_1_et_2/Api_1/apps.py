# Import the base AppConfig class from Django
from django.apps import AppConfig


# Define a configuration class for our app, inheriting from AppConfig
class Api1Config(AppConfig):
    # Set the default primary key field type to BigAutoField.
    # This tells Django to use a 64-bit integer (BIGINT) for automatic
    # primary keys (e.g., 'id' fields) instead of the older 32-bit integer.
    default_auto_field = 'django.db.models.BigAutoField'
    
    # Set the name of the application. This must match the folder name
    # of your app, which is 'Api_1'.
    name = 'Api_1'