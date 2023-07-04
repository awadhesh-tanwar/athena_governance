from django.urls import include, path
from . import views

urlpatterns = [
    # Other URL patterns
    path('update-db-permissions/', views.db_view, name='db_view'),
    path('update-table-permissions/', views.table_view, name='table_view'),
    path('update-lftags-db-permissions/', views.lftags_db_view, name='lftags_db_view'),
]
