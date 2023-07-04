from django.urls import include, path
from . import views

urlpatterns = [
    # Other URL patterns
    path('grant-permission/', views.grant_permission, name='grant_permission'),
    path('revoke-permission/', views.revoke_permission, name='revoke_permission'),
]
