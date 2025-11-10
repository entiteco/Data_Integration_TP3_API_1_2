# Import the main admin module from Django
from django.contrib import admin
# Import the Product model from the local models.py file
from .models import Product
# Import the UserPermission model from the local models.py file
from .models import UserPermission
# Import the ApiAuditLog model from the local models.py file
from .models import ApiAuditLog

# Register the Product model with the admin site.
# This makes the Product table visible and manageable on the /admin/ page.
admin.site.register(Product)

# Register the UserPermission model with the admin site.
# This allows administrators to view, add, and delete permissions for users.
admin.site.register(UserPermission)

# Register the ApiAuditLog model with the admin site.
# This allows administrators to view the log of all API requests.
admin.site.register(ApiAuditLog)