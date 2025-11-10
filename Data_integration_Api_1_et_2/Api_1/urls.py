from django.urls import path
from . import views

urlpatterns = [
    # API 1 Lab Routes (all are prefixed with 'api/' in the main urls.py)
    path("test_json_view", views.test_json_view, name="test_json_view"),
    path("post_test", views.post_json_view, name="post_test"),
    path("products/", views.product_list_create, name="product-list-create"),
    path("products/most-expensive", views.get_most_expensive_product, name="product-most-expensive"),
    path("products/<int:pk>/", views.product_detail_view, name="product-detail"),

    # API 2 Lab Routes (Part I - Security)
    path("admin/permissions/", views.PermissionView.as_view(), name="admin-permissions"),
    
    # API 2 Lab Routes (Part II & IV - Data Lake)
    path("datalake/streams/<str:topic_name>/", views.DataLakeStreamView.as_view(), name="datalake-stream-view"),
    path("datalake/resources/", views.DataLakeResourcesView.as_view(), name="datalake-resources"),
    
    # API 2 Lab Routes (Part III - Metrics)
    path("metrics/total-by-user/", views.MetricsTotalByUserView.as_view(), name="metrics-total-by-user"),
    path("metrics/top-products/", views.MetricsTopProductsView.as_view(), name="metrics-top-products"),
    path("metrics/last-5-min/", views.MetricsLast5MinView.as_view(), name="metrics-last-5-min"),

    # API 2 Lab Routes (Part IV - Audit)
    path("audit-logs/", views.ApiAuditLogView.as_view(), name="audit-logs"),

    # API 2 Lab Routes (Part V - Advanced Capabilities)
    path("search/", views.FullTextSearchView.as_view(), name="full-text-search"),
    path("rpc/train-model/", views.TrainModelView.as_view(), name="rpc-train-model"),
    path("data/repush-transaction/", views.RepushTransactionView.as_view(), name="repush-transaction"),
    path("data/repush-all/", views.RepushAllView.as_view(), name="repush-all"),
]