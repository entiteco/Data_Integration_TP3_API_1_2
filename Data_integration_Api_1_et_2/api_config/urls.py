# In api_config/urls.py
from django.contrib import admin
from django.urls import path, include
from rest_framework.authtoken import views as authtoken_views
from Api_1 import views # Import views from the local 'Api_1' app

# --- 1. Imports required for Swagger/drf-yasg ---
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
# -----------------------------------------------

# --- 2. Swagger view configuration ---
schema_view = get_schema_view(
   openapi.Info(
      title="Data Platform API",
      default_version='v1',
      description="API for exposing Data Lake and Data Warehouse data",
      contact=openapi.Contact(email="kzm@example.com"),
      license=openapi.License(name="BSD License"),
   ),
   public=True,
   permission_classes=(permissions.AllowAny,), # Allows anyone to view the API documentation
)
# -----------------------------------------

urlpatterns = [
    # --- Main Application Routes ---
    # Prefixes all routes from 'Api_1.urls' with 'api/'
    path("api/", include("Api_1.urls")),
    
    # Endpoint to obtain an authentication token
    path('api-token-auth/', authtoken_views.obtain_auth_token, name='api_token_auth'),
    
    # --- Admin Routes ---
    # The specific custom admin path must come *before* the generic one
    path("admin/permissions/", views.PermissionView.as_view(), name="admin-permissions"),
    
    # The default Django admin site
    path('admin/', admin.site.urls),

    # --- API Documentation Routes ---
    # Endpoint for the raw schema (JSON or YAML)
    path('swagger<format>/', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    # Endpoint for the Swagger interactive UI
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    # Endpoint for the ReDoc alternative UI
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]