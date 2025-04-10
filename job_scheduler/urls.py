"""
URL configuration for job_scheduler project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin

from django.urls import path, include
from scheduler import views

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # path('submit/', views.submit_job, name='submit_job'),
    
    path('jobs/', views.submit_job, name='submit_job'),
    # URL pattern for getting job results
    path('jobs/<uuid:job_id>/results/', views.get_job_results, name='get_job_results'),
    
        # path('signup/', views.signup_view, name='signup'),
    # path('', views.submit_job_form, name='submit_job'),
    # path('job/<uuid:job_id>/', views.job_result, name='job_result'),
     path('', include('scheduler.urls')),
]