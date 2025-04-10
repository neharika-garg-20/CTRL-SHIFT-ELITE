from django.urls import path
from . import views

urlpatterns = [
    path('login/', views.login_view, name='login'),
    # Add other URLs like signup, home, etc. here
    path('signup/', views.signup_view, name='signup'),
    path('job/<uuid:job_id>/', views.job_result, name='job_result'),
     path('add/', views.add_job, name='add_job'),
]