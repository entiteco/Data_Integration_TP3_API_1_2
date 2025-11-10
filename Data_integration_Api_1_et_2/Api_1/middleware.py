from .models import ApiAuditLog
import json

class AuditLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Code to be executed for each request before the view is called
        
        # Only log API calls, not /admin/ or other paths
        if request.path.startswith('/api/'):
            
            body = ""
            if request.body:
                try:
                    # Attempt to pretty-print JSON for readability
                    body = json.dumps(json.loads(request.body), indent=4)
                except:
                    # If not JSON, save the raw body
                    body = request.body.decode('utf-8', 'ignore')
            
            # Create the audit log entry
            ApiAuditLog.objects.create(
                # request.user is populated by TokenAuthentication middleware
                user=request.user if request.user.is_authenticated else None,
                path=request.path,
                method=request.method,
                request_body=body
            )
        
        # Let the view execute
        response = self.get_response(request)
        
        # Code to be executed for each request after the view is called
        
        return response