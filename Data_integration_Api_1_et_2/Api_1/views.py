# --- Django Core Imports ---
from django.shortcuts import render
from django.http import JsonResponse, Http404
from django.conf import settings
from django.core.paginator import Paginator, EmptyPage
from django.db import connection
from django.contrib.auth.models import User
from django.utils import timezone

# --- Standard Library Imports ---
import json
import os
import logging  # For logging RPC tasks
import threading  # For running background tasks

# --- Third-Party Imports ---
from kafka import KafkaProducer  # For re-pushing transactions

# --- Django Rest Framework (DRF) Imports ---
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

# --- Local App Imports ---
from .models import Product, UserPermission, ApiAuditLog

# -----------------------------------------------------------------
# --- Test Views (from API 1 Lab)
# -----------------------------------------------------------------

@api_view(['GET']) # Defines this as a GET-only API view
def test_json_view(request):
    """A simple test endpoint to check if the API is running."""
    data = {
        'name': 'John Doe',
        'age': 30,
        'location': 'New York',
        'is_active': True,
    }
    return Response(data) # Use DRF's Response

@api_view(['POST']) # Defines this as a POST-only API view
def post_json_view(request):
    """A simple test endpoint to check POST data handling."""
    try:
        # DRF's request.data parses JSON automatically
        user_name = request.data.get('user') 
        if user_name is None:
            return Response({'error': 'Missing "user" key in body'}, status=400)
        
        response_data = {
            'name': user_name,
            'age': 30,
            'location': 'New York',
            'is_active': True,
        }
        return Response(response_data)
    except Exception as e:
        return Response({'error': 'Invalid JSON format'}, status=400)

# -----------------------------------------------------------------
# --- Product CRUD Views (from API 1 Lab, now secured)
# -----------------------------------------------------------------

@api_view(['GET', 'POST'])
@authentication_classes([TokenAuthentication]) # Requires "Authorization: Token ..." header
def product_list_create(request):
    """
    Handles:
    - GET: Returns a paginated list of all products. (Requires 'view_products' permission)
    - POST: Adds a new product to the table. (Requires 'add_products' permission)
    """
    
    # DRF's @authentication_classes handles the 401.
    # We now check for business-logic permissions.

    if request.method == 'GET':
        # Check for the custom 'view_products' permission
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_products').exists():
            return Response({'error': 'You do not have permission to view products.'}, status=403)
        
        # --- Pagination Logic ---
        page_number = request.GET.get('page', 1)
        page_size = 3
        all_products = Product.objects.all().order_by('id')
        paginator = Paginator(all_products, page_size)

        try:
            page_obj = paginator.page(page_number)
        except EmptyPage:
            return Response({'error': 'Page not found'}, status=404)

        results_list = []
        for product in page_obj.object_list:
            results_list.append({
                'id': product.id,
                'name': product.name,
                'price': str(product.price), # Convert Decimal to string
                'description': product.description
            })
        
        response = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'current_page': page_obj.number,
            'has_next': page_obj.has_next(),
            'has_previous': page_obj.has_previous(),
            'results': results_list
        }
        return Response(response)
    
    elif request.method == 'POST':
        # Check for the custom 'add_products' permission
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='add_products').exists():
            return Response({'error': 'You do not have permission to add products.'}, status=403)
        
        try:
            data = request.data # Use DRF's parsed data
            
            if not data.get('name') or not data.get('price'):
                return Response({'error': 'Missing required fields: name and price'}, status=400)

            new_product = Product.objects.create(
                name=data.get('name'),
                price=data.get('price'),
                description=data.get('description')
            )
            
            response_data = {
                'id': new_product.id,
                'name': new_product.name,
                'price': str(new_product.price),
                'description': new_product.description,
                'created_at': new_product.created_at
            }
            return Response(response_data, status=201) # 201 Created
        except Exception as e:
            return Response({'error': f'Could not create product: {str(e)}'}, status=400)
            
    else:
        return Response({'error': 'Method not allowed'}, status=405)


@api_view(['GET'])
@authentication_classes([TokenAuthentication])
def get_most_expensive_product(request):
    """
    Handles GET request. (Requires 'view_expensive_product' permission)
    """
    if not request.user.is_superuser and \
       not UserPermission.objects.filter(user=request.user, permission='view_expensive_product').exists():
        return Response({'error': 'Permission denied.'}, status=403)
    
    try:
        # Use Django ORM to order by price (descending) and get the first one
        product = Product.objects.order_by('-price').values().first()
        if product:
            return Response(product)
        else:
            return Response({'error': 'No products found'}, status=404)
    except Exception as e:
        return Response({'error': str(e)}, status=500)


@api_view(['GET', 'PUT', 'DELETE'])
@authentication_classes([TokenAuthentication])
def product_detail_view(request, pk):
    """
    Handles GET (retrieve), PUT (update), and DELETE for a single product.
    """
    try:
        product = Product.objects.get(pk=pk)
    except Product.DoesNotExist:
        return Response({'error': 'Product not found'}, status=404)

    if request.method == 'GET':
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_products').exists():
            return Response({'error': 'You do not have permission to view products.'}, status=403)
        
        data = {
            'id': product.id,
            'name': product.name,
            'price': str(product.price),
            'description': product.description,
            'created_at': product.created_at,
            'updated_at': product.updated_at
        }
        return Response(data)

    elif request.method == 'PUT':
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='edit_products').exists():
            return Response({'error': 'You do not have permission to update products.'}, status=403)
        
        try:
            data = request.data
            product.name = data.get('name', product.name) # Use new name, or default to old one
            product.price = data.get('price', product.price)
            product.description = data.get('description', product.description)
            product.save() # Save changes to DB
            
            response_data = {
                'id': product.id,
                'name': product.name,
                'price': str(product.price),
                'description': product.description,
                'updated_at': product.updated_at
            }
            return Response(response_data)
        except Exception as e:
            return Response({'error': f'Could not update product: {str(e)}'}, status=400)
            
    elif request.method == 'DELETE':
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='edit_products').exists():
            return Response({'error': 'You do not have permission to delete products.'}, status=403)
        
        product.delete()
        return Response(status=204) # 204 No Content (success)

# -----------------------------------------------------------------
# --- API 2: Part I Views (Security & Admin)
# -----------------------------------------------------------------
    
class PermissionView(APIView):
    """
    Admin endpoint to grant or revoke permissions.
    - POST: { "username": "some_user", "permission": "view_products" }
    - DELETE: { "username": "some_user", "permission": "view_products" }
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAdminUser] # Only Staff/Superusers can access

    def post(self, request):
        """Grants a permission to a user."""
        username = request.data.get('username')
        permission = request.data.get('permission')

        if not username or not permission:
            return Response({'error': '"username" and "permission" are required'}, status=400)

        try:
            user_to_modify = User.objects.get(username=username)
        except User.DoesNotExist:
            return Response({'error': f'User "{username}" not found'}, status=404)

        # Check if the permission is valid
        valid_permissions = [p[0] for p in UserPermission.PERMISSION_CHOICES]
        if permission not in valid_permissions:
            return Response({'error': f'Permission "{permission}" is not valid'}, status=400)

        # get_or_create prevents duplicates
        perm, created = UserPermission.objects.get_or_create(user=user_to_modify, permission=permission)

        if created:
            return Response({'status': f'Permission "{permission}" added to {username}'}, status=201)
        else:
            return Response({'status': f'User {username} already has this permission'}, status=200)

    def delete(self, request):
        """Revokes a permission from a user."""
        username = request.data.get('username')
        permission = request.data.get('permission')

        if not username or not permission:
            return Response({'error': '"username" and "permission" are required'}, status=400)

        try:
            user_to_modify = User.objects.get(username=username)
        except User.DoesNotExist:
            return Response({'error': f'User "{username}" not found'}, status=404)

        # Find and delete the permission
        deleted_count, _ = UserPermission.objects.filter(user=user_to_modify, permission=permission).delete()

        if deleted_count > 0:
            return Response({'status': f'Permission "{permission}" revoked for {username}'}, status=204)
        else:
            return Response({'error': f'User {username} did not have this permission'}, status=404)

# -----------------------------------------------------------------
# --- API 2: Part II & IV Views (Data Lake)
# -----------------------------------------------------------------
        
class DataLakeStreamView(APIView):
    """
    Endpoint to read data from STREAMS in the Data Lake.
    Answers Q3 (Pagination), Q4 (Projection), Q5 (Filtering), and IV (Versioning).
    """
    authentication_classes = [TokenAuthentication]
    
    def get(self, request, topic_name):
        # 1. Check for 'view_datalake' permission
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_datalake').exists():
            return Response({'error': 'Permission denied to access Data Lake.'}, status=403)

        # 2. Build path and load data
        try:
            # Build the base path, e.g., /path/to/project/data_lake/streams/TRANSACTIONS_PROCESSED
            base_path = os.path.join(settings.DATA_LAKE_ROOT, 'streams', topic_name)
            if not os.path.exists(base_path):
                return Response({'error': f"Topic '{topic_name}' not found in Data Lake."}, status=404)
            
            # Pass query_params to handle date versioning
            all_data = self.load_data_from_files(base_path, request.query_params)
        
        except FileNotFoundError as e:
            # This catches the custom error from load_data_from_files if a version is not found
            return Response({'error': str(e)}, status=404)
        except Exception as e:
            return Response({'error': f"Error reading files: {e}"}, status=500)

        # 3. Apply Filtering (Q5)
        filtered_data = self.filter_data(all_data, request.query_params)

        # 4. Apply Projection (Q4)
        projected_data = self.project_data(filtered_data, request.query_params)

        # 5. Apply Pagination (Q3)
        page_number = request.GET.get('page', 1)
        page_size = 10 # Max 10 messages per page
        paginator = Paginator(projected_data, page_size)

        try:
            page_obj = paginator.page(page_number)
        except EmptyPage:
            page_obj = paginator.page(1) # Default to page 1 if page is empty or invalid

        # 6. Build the response
        response = {
            'topic': topic_name,
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'current_page': int(page_number),
            'results': list(page_obj.object_list)
        }
        return Response(response)

    def load_data_from_files(self, base_path, params):
        """
        Scans subfolders (year/month/day) and loads all .json files.
        Also handles date versioning if provided in params.
        """
        all_data = []
        
        # 1. Check for version (date) parameters
        year = params.get('year')
        month = params.get('month')
        day = params.get('day')

        target_path = base_path
        
        # 2. Build the specific version path if all date parts are provided
        if year and month and day:
            target_path = os.path.join(base_path, f"year={year}", f"month={month}", f"day={day}")
            # Return an error if the specified version (directory) doesn't exist
            if not os.path.exists(target_path):
                raise FileNotFoundError(f"Version (date) not found: {year}-{month}-{day}")
        
        # 3. Walk the target path (either the base or the specific version)
        for root, dirs, files in os.walk(target_path):
            for file in files:
                if file.endswith('.json'):
                    try:
                        filepath = os.path.join(root, file)
                        with open(filepath, 'r') as f:
                            all_data.append(json.load(f))
                    except Exception as e:
                        print(f"Error reading JSON {filepath}: {e}") # Log for server
                        pass # Ignore corrupted files
        return all_data

    def filter_data(self, data, params):
        """Manually filters the list of data in Python (Q5)."""
        
        filtered = list(data)
        
        # Simple equality filters
        if params.get('payment_method'):
            filtered = [d for d in filtered if d.get('payment_method') == params.get('payment_method')]
        if params.get('country'):
            filtered = [d for d in filtered if d.get('LOCATION_COUNTRY') == params.get('country')]
        if params.get('product_category'):
            filtered = [d for d in filtered if d.get('product_category') == params.get('product_category')]
        if params.get('status'):
            filtered = [d for d in filtered if d.get('STATUS') == params.get('status')]

        # Comparison filters (amount)
        if params.get('amount_gt'):
            filtered = [d for d in filtered if float(d.get('AMOUNT_USD', 0)) > float(params.get('amount_gt'))]
        if params.get('amount_lt'):
            filtered = [d for d in filtered if float(d.get('AMOUNT_USD', 0)) < float(params.get('amount_lt'))]
        if params.get('amount_eq'):
            filtered = [d for d in filtered if float(d.get('AMOUNT_USD', 0)) == float(params.get('amount_eq'))]

        # Comparison filters (customer_rating)
        if params.get('rating_gt'):
            filtered = [d for d in filtered if d.get('CUSTOMER_RATING') is not None and int(d.get('CUSTOMER_RATING', 0)) > int(params.get('rating_gt'))]
        if params.get('rating_lt'):
            filtered = [d for d in filtered if d.get('CUSTOMER_RATING') is not None and int(d.get('CUSTOMER_RATING', 0)) < int(params.get('rating_lt'))]
        if params.get('rating_eq'):
            filtered = [d for d in filtered if d.get('CUSTOMER_RATING') is not None and int(d.get('CUSTOMER_RATING', 0)) == int(params.get('rating_eq'))]
            
        return filtered

    def project_data(self, data, params):
        """Selects only the requested fields (Q4)."""
        
        fields_to_keep = params.get('fields')
        if not fields_to_keep:
            return data # No projection, return full dict

        # e.g., fields = ["user_id", "amount", "status"]
        fields = fields_to_keep.split(',')
        
        projected_data = []
        for item in data:
            new_item = {}
            for field in fields:
                
                # 1. Translate friendly names to actual JSON keys (which are UPPERCASE)
                key_to_find = field.upper() 
                if field == 'amount':
                    key_to_find = 'AMOUNT_USD'
                elif field == 'user_id':
                    key_to_find = 'USER_ID'
                
                # 2. Handle nested fields (e.g., location.country)
                if '.' in field:
                    try:
                        key1, key2 = field.split('.') # e.g., 'location', 'country'
                        # Find parent (e.g., 'LOCATION'), then child (e.g., 'COUNTRY')
                        new_item[field] = item[key1.upper()][key2.upper()]
                    except KeyError:
                        new_item[field] = None # Handle if nested field doesn't exist
                else:
                    # 3. Handle normal fields
                    new_item[field] = item.get(key_to_find)

            projected_data.append(new_item)
            
        return projected_data

# -----------------------------------------------------------------
# --- API 2: Part III Views (Metrics from DWH)
# -----------------------------------------------------------------
    
class MetricsTotalByUserView(APIView):
    """
    Endpoint for: Get total spent per user and transaction type
    Reads directly from the 'total_depense_par_user_type' MySQL table.
    """
    authentication_classes = [TokenAuthentication]

    def get(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_metrics').exists():
            return Response({'error': 'Permission denied to access metrics.'}, status=403)

        try:
            # Use a raw SQL connection for tables not managed by Django models
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        user_id, 
                        transaction_type, 
                        total_depense_usd, 
                        last_updated_at 
                    FROM 
                        total_depense_par_user_type
                    ORDER BY 
                        user_id, total_depense_usd DESC
                """)
                
                # Format the raw SQL results into a list of dictionaries
                columns = [col[0] for col in cursor.description]
                results = [
                    dict(zip(columns, row))
                    for row in cursor.fetchall()
                ]
            
            return Response(results)

        except Exception as e:
            return Response({'error': f"Database read error: {e}"}, status=500)
        
class MetricsTopProductsView(APIView):
    """
    Endpoint for: Get the top x product bought
    Reads from the 'product_purchase_counts' MySQL table.
    Accepts a query parameter: ?x=5
    """
    authentication_classes = [TokenAuthentication]

    def get(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_metrics').exists():
            return Response({'error': 'Permission denied to access metrics.'}, status=403)

        # 1. Get the 'x' parameter (limit), default to 10
        try:
            limit = int(request.query_params.get('x', 10))
        except ValueError:
            return Response({'error': "The 'x' parameter must be an integer."}, status=400)

        try:
            with connection.cursor() as cursor:
                # 2. Execute the SQL query with a parameterized limit
                cursor.execute("""
                    SELECT product_id, purchase_count, last_updated_at
                    FROM product_purchase_counts
                    ORDER BY purchase_count DESC
                    LIMIT %s
                """, [limit]) # Use %s placeholder for security
                
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return Response(results)

        except Exception as e:
            return Response({'error': f"Database read error: {e}"}, status=500)


class MetricsLast5MinView(APIView):
    """
    Endpoint for: Get money spent the last 5 minutes
    Reads from the 'total_par_type_5min_sliding' MySQL table.
    """
    authentication_classes = [TokenAuthentication]

    def get(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_metrics').exists():
            return Response({'error': 'Permission denied to access metrics.'}, status=403)

        try:
            with connection.cursor() as cursor:
                # 1. Select only the totals from the *latest* available time window
                cursor.execute("""
                    SELECT transaction_type, total_amount_5min, window_start_time
                    FROM total_par_type_5min_sliding
                    WHERE window_start_time = (
                        SELECT MAX(window_start_time) 
                        FROM total_par_type_5min_sliding
                    )
                """)
                
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return Response(results)

        except Exception as e:
            return Response({'error': f"Database read error: {e}"}, status=500)

# -----------------------------------------------------------------
# --- API 2: Part IV Views (Audit & Discovery)
# -----------------------------------------------------------------
        
class ApiAuditLogView(APIView):
    """
    Admin endpoint for: Get Who queried or accessed a specific data.
    Reads the 'ApiAuditLog' table and allows filtering.
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAdminUser] # Only Admins can view logs

    def get(self, request):
        # 1. Start with all logs
        logs = ApiAuditLog.objects.all()

        # 2. Optional filter by user_id
        # e.g., ?user_id=2
        user_id = request.query_params.get('user_id')
        if user_id:
            logs = logs.filter(user__id=user_id)

        # 3. Optional filter by API path
        # e.g., ?path=/api/products/
        path = request.query_params.get('path')
        if path:
            logs = logs.filter(path__icontains=path)

        # 4. Paginate the results (essential for logs)
        paginator = Paginator(logs, 20) # 20 logs per page
        page_number = request.GET.get('page', 1)
        try:
            page_obj = paginator.page(page_number)
        except EmptyPage:
            return Response({'error': 'Page not found'}, status=404)

        # 5. Format the results
        results_list = []
        for log in page_obj.object_list:
            results_list.append({
                'id': log.id,
                'timestamp': log.timestamp,
                'user': log.user.username if log.user else "Anonymous",
                'path': log.path,
                'method': log.method,
                'request_body': log.request_body
            })

        response = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'current_page': page_obj.number,
            'results': results_list
        }
        return Response(response)
    
class DataLakeResourcesView(APIView):
    """
    Endpoint for: Get the list of all the ressources available in my data lake
    Scans the Data Lake directory to list available topics.
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated] # Any authenticated user can see the list

    def get(self, request):
        # Check for 'view_datalake' permission
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='view_datalake').exists():
            return Response({'error': 'Permission denied to access Data Lake.'}, status=403)

        # Get the Data Lake root path from settings.py
        data_lake_root = settings.DATA_LAKE_ROOT
        
        try:
            # 1. Scan the "streams" folder
            streams_path = os.path.join(data_lake_root, 'streams')
            streams_list = [d for d in os.listdir(streams_path) if os.path.isdir(os.path.join(streams_path, d))]
            
            # 2. Scan the "tables" folder
            tables_path = os.path.join(data_lake_root, 'tables')
            tables_list = [d for d in os.listdir(tables_path) if os.path.isdir(os.path.join(tables_path, d))]
            
            # 3. Build the response
            response = {
                'data_lake_root': data_lake_root,
                'available_resources': {
                    'streams': streams_list,
                    'tables': tables_list
                }
            }
            return Response(response)
        
        except FileNotFoundError as e:
            return Response({'error': f"Data Lake path not found: {e}"}, status=404)
        except Exception as e:
            return Response({'error': str(e)}, status=500)

# -----------------------------------------------------------------
# --- API 2: Part V Views (Advanced Capabilities)
# -----------------------------------------------------------------
        
# Task V.1: Full-Text Search
class FullTextSearchView(APIView):
    """
    Endpoint for: Full-text search across data.
    Scans the Data Lake for a search term.
    """
    authentication_classes = [TokenAuthentication]
    
    def post(self, request):
        # 1. Check permission
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='search_datalake').exists():
            return Response({'error': 'Permission denied for search.'}, status=403)

        # 2. Get parameters
        search_term = request.data.get('search_term')
        start_date_str = request.data.get('start_date') # e.g., "2025-10-01"
        
        if not search_term or not start_date_str:
            return Response({'error': '"search_term" and "start_date" (YYYY-MM-DD) are required.'}, status=400)
        
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            return Response({'error': 'Invalid start_date format. Use YYYY-MM-DD.'}, status=400)

        # 3. Brute-force search logic (intentionally slow)
        found_in_files = []
        data_lake_root = settings.DATA_LAKE_ROOT
        
        for root, dirs, files in os.walk(data_lake_root):
            # 4. Check if the directory date is after the start_date
            try:
                parts = root.split(os.sep)
                if len(parts) >= 3 and parts[-1].startswith('day='):
                    day = int(parts[-1].split('=')[-1])
                    month = int(parts[-2].split('=')[-1])
                    year = int(parts[-3].split('=')[-1])
                    current_date = datetime(year, month, day).date()
                    
                    if current_date < start_date:
                        continue # Skip this directory, it's too old
            except:
                continue # Not a date-partitioned directory

            # 5. Search inside the files
            for file in files:
                if file.endswith('.json'):
                    filepath = os.path.join(root, file)
                    try:
                        with open(filepath, 'r') as f:
                            data_str = f.read()
                            if search_term in data_str:
                                # Found!
                                resource = root.replace(data_lake_root, '')
                                found_in_files.append({
                                    'resource': resource,
                                    'file': file
                                })
                    except Exception as e:
                        pass # Ignore read errors

        # 6. Propose a better technology
        recommendation = ("Warning: This search was performed by scanning raw JSON files, "
                          "which is extremely slow and inefficient. For production-ready search, "
                          "it is recommended to index this data in a dedicated search engine "
                          "like Elasticsearch or OpenSearch.")

        return Response({
            'search_term': search_term,
            'start_date': start_date_str,
            'recommendation': recommendation,
            'results_found': len(found_in_files),
            'files': found_in_files
        })

# Task V.2: RPC (ML Model Training)
class TrainModelView(APIView):
    """
    RPC Endpoint for: Trigger the training of a machine learning model.
    This is an asynchronous endpoint (it responds immediately).
    """
    authentication_classes = [TokenAuthentication]
    
    def post(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='trigger_ml_model').exists():
            return Response({'error': 'Permission denied to trigger training.'}, status=403)

        model_type = request.data.get('model_type', 'default_model')
        
        # In a real project, this would dispatch a Celery task.
        # Here, we simulate it by logging to the server console.
        logging.warning(f"--- RPC TRIGGERED ---")
        logging.warning(f"Training request for model '{model_type}' received from user {request.user.username}.")
        logging.warning(f"--- SIMULATED TASK RUNNING IN BACKGROUND ---")

        # 202 Accepted: "I've received your request and am starting the job."
        return Response(
            {'status': 'success', 'message': f'Training for model {model_type} started in the background.'},
            status=202
        )

# Task V.3: Re-push a single transaction
class RepushTransactionView(APIView):
    """
    Endpoint for: Re-push a corrupted transaction to Kafka.
    """
    authentication_classes = [TokenAuthentication]

    def post(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='repush_transaction').exists():
            return Response({'error': 'Permission denied to re-process transactions.'}, status=403)
        
        transaction_id = request.data.get('transaction_id')
        if not transaction_id:
            return Response({'error': '"transaction_id" is required.'}, status=400)
            
        # 1. Find the transaction file in the Data Lake
        found_data = None
        data_lake_root = settings.DATA_LAKE_ROOT
        
        for root, dirs, files in os.walk(data_lake_root):
            for file in files:
                if file.endswith('.json'):
                    filepath = os.path.join(root, file)
                    try:
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                            # Check both UPPER (from ksqlDB) and lower (from source)
                            if data.get('TRANSACTION_ID') == transaction_id or data.get('transaction_id') == transaction_id:
                                found_data = data
                                break
                    except:
                        pass # Ignore corrupted files
            if found_data:
                break
        
        if not found_data:
            return Response({'error': f'Transaction {transaction_id} not found in Data Lake.'}, status=404)
        
        # 2. Modify the timestamp "on the fly"
        found_data['timestamp'] = timezone.now().isoformat() + "Z"
        found_data['re-pushed_from_api'] = True # Add a tag for traceability
        
        # 3. Push to the beginning of the Kafka pipeline
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            # Send to the raw 'transaction_log' topic
            producer.send('transaction_log', value=found_data)
            producer.flush()
            producer.close()
            
            return Response({
                'status': 'success', 
                'message': f'Transaction {transaction_id} re-pushed to transaction_log.',
                'new_data': found_data
            })
            
        except Exception as e:
            return Response({'error': f"Error connecting to Kafka: {e}"}, status=500)

# Task V.4: Repush all
class RepushAllView(APIView):
    """
    Endpoint for: Repush all historical product to the beginning.
    Uses threading to avoid blocking the API server.
    """
    authentication_classes = [TokenAuthentication]
    
    def _repush_all_thread(self, user):
        """This function runs in a separate thread."""
        logging.warning(f"STARTING total re-push requested by {user.username}")
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            data_lake_root = settings.DATA_LAKE_ROOT
            
            count = 0
            # Walk the entire data lake
            for root, dirs, files in os.walk(data_lake_root):
                # We only want to re-push from the 'TRANSACTIONS_PROCESSED' stream
                if 'TRANSACTIONS_PROCESSED' in root:
                    for file in files:
                        if file.endswith('.json'):
                            filepath = os.path.join(root, file)
                            try:
                                with open(filepath, 'r') as f:
                                    data = json.load(f)
                                    # Update timestamp and add tag
                                    data['timestamp'] = timezone.now().isoformat() + "Z"
                                    data['re-pushed_from_api'] = 'all'
                                    producer.send('transaction_log', value=data)
                                    count += 1
                            except:
                                pass # Ignore corrupted files

            producer.flush()
            producer.close()
            logging.warning(f"FINISHED total re-push. {count} messages were re-pushed.")
        except Exception as e:
            logging.error(f"ERROR during total re-push: {e}")

    def post(self, request):
        if not request.user.is_superuser and \
           not UserPermission.objects.filter(user=request.user, permission='repush_all_transactions').exists():
            return Response({'error': 'Permission denied (DANGEROUS).'}, status=403)
        
        # Start the re-push function in a background thread
        thread = threading.Thread(target=self._repush_all_thread, args=(request.user,))
        thread.start()
        
        # Immediately return a 202 Accepted response
        return Response(
            {'status': 'Job started', 'message': 'The re-push of all historical data has started in the background.'},
            status=202 
        )