from django.urls import include, path
from . import views

urlpatterns = [
    # Other URL patterns
    path('get-users/', views.get_users, name='get_users'),
    path('get-dbs/', views.get_dbs, name='get_dbs'),
    path('get-tables/', views.get_tables, name='get_tables'),
    path('get-users/', views.get_users, name='get_users')
]
