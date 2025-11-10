from django.db import models
from django.contrib.auth.models import User

# Defines the 'Product' table in the database
class Product(models.Model):
   # Stores the product's name, max 100 characters
   name = models.CharField(max_length=100)  
   # Stores the price using a high-precision decimal (e.g., 12345678.99)
   price = models.DecimalField(max_digits=10, decimal_places=2)
   # A long text field for the description, which can be left blank (null)
   description = models.TextField(blank=True, null=True)  
   # Automatically sets this timestamp when a new product is created
   created_at = models.DateTimeField(auto_now_add=True)  
   # Automatically updates this timestamp every time the product is saved
   updated_at = models.DateTimeField(auto_now=True)  

# NOTE: This method is incorrectly indented in the original file.
# It should be *inside* the Product class to work correctly.
def __str__(self):  
    # Returns the product's name as its string representation
    return self.name 

# Defines the 'UserPermission' table for custom, row-level permissions
class UserPermission(models.Model):
    """
    Custom model to link a User to a specific permission.
    """
    
    # Defines the list of available permissions that can be assigned
    PERMISSION_CHOICES = [
        ('view_products', 'Can view product list and details'),
        ('add_products', 'Can add new products'),
        ('edit_products', 'Can update or delete products'),
        ('view_expensive_product', 'Can view the most expensive product'),
        ('view_datalake', 'Can view Data Lake streams'),
        ('view_metrics', 'Can view aggregated metrics'),
        ('search_datalake', 'Can perform full-text search on Data Lake'),
        ('trigger_ml_model', 'Can trigger ML model training'),
        ('repush_transaction', 'Can re-push a single transaction to Kafka'),
        ('repush_all_transactions', 'Can re-push all historical data to Kafka (DANGEROUS)'),
    ]

    # Links this permission to a built-in Django User.
    # If a User is deleted, all their associated permissions are also deleted (CASCADE).
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    
    # The specific right, chosen from the PERMISSION_CHOICES list
    permission = models.CharField(max_length=50, choices=PERMISSION_CHOICES)

    class Meta:
        # Ensures a user cannot have the same permission twice (database constraint)
        unique_together = ('user', 'permission')

    def __str__(self):
        # Provides a human-readable name in the Django admin interface
        return f"{self.user.username} - {self.get_permission_display()}"
    
# Defines the 'ApiAuditLog' table to store a record of all API requests
class ApiAuditLog(models.Model):
    """
    Stores a record of every request made to the API.
    """
    # Tracks which user made the call. Can be null if auth failed or was anonymous.
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Automatically records when the request happened
    timestamp = models.DateTimeField(auto_now_add=True)
    
    # The URL path that was accessed (e.g., "/api/products/")
    path = models.CharField(max_length=1024)
    
    # The HTTP method used (e.g., "GET", "POST")
    method = models.CharField(max_length=10)
    
    # The body of the request (e.g., the JSON from a POST)
    request_body = models.TextField(blank=True, null=True)

    class Meta:
        # Sorts logs from newest to oldest by default in the admin
        ordering = ['-timestamp']

    def __str__(self):
        # Provides a human-readable name in the admin
        username = self.user.username if self.user else "Anonymous"
        return f"{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')} - {username} - {self.method} {self.path}"