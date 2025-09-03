'''
    Should contain only Task methods.
    avoid having any utility functions inside this file.
    Please do not corrupt/pollute this file with unncessary imports, or functions
'''

import os
from .models import Notification, CustomToken, FoodEvent, STATUS, Donation, Request
from celery import Celery, shared_task
from celery.utils.log import get_task_logger
from exponent_server_sdk import PushClient, PushMessage 
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from django.utils import timezone
from django.db.models import Q
import logging
from bs4 import BeautifulSoup

logger = get_task_logger(__name__)
app = Celery()

def send_push_message(user, title, message, notification_type, is_email_notification):
    try:

        if is_email_notification:
            # Email notification Code
            subject = title
            email_from = settings.DEFAULT_SENDER
            recipient_list = [user.email]
            html = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/NotificationEmail.html')).read()
            email_text = html.replace('{{name}}', user.name).replace('{{title}}', title).replace('{{message}}', message)
            
            try:
                msg = EmailMultiAlternatives(subject=subject, from_email=email_from, to=recipient_list)
                msg.attach_alternative(email_text, "text/html")
                msg.send()
                print({'success': True, 'message': 'Email Notification sent'})

                soup = BeautifulSoup(message, 'html.parser')
                details = soup.get_text()
                lines = details.split('\n')
                cleaned_lines = [line.split(":")[0].strip() + ":" + line.split(":")[1].strip() if ':' in line else line for line in lines]
                message = '\n'.join(cleaned_lines)
                
            except Exception as e:
                print({'success': False, 'message': 'Failed to send email notification', 'error': str(e)})
                return({'success': False, 'message': 'Failed to send email notification', 'error': str(e)})

        Notification.objects.create(user=user, title=title, message=message, notificationType=notification_type)
        
        # Expo Notification Code
        if CustomToken.objects.filter(user=user).exists():
            custom_token = CustomToken.objects.get(user=user)

            try:
                PushClient().publish(
                    PushMessage(
                        to=custom_token.expoPushToken,
                        title=title,
                        body=message,
                    )
                )
                print({'success': True, 'message': 'Expo Notification sent'})
                return({'success': True, 'message': 'Expo Notification sent'})
            
            except Exception as e:
                print({'success': False, 'message': 'Failed to send Expo notification', 'error':str(e)})
                return({'success': False, 'message': 'Failed to send Expo notification', 'error':str(e)})
        else:
            print({'success': False, 'message': 'Custom Token for user not exists', 'error': 'Custom Token for user not exists'})
            return({'success': False, 'message': 'Custom Token for user not exists', 'error': 'Custom Token for user not exists'})  
        
    except Exception as e:
        print({'success':False, 'message':'Cant Send MSG', 'error':str(e)})
        return({'success':False, 'message':'Cant Send MSG', 'error':str(e)})

@shared_task(name='checking_event_status')
def event_status_check():
    try:
        today_date = timezone.now()
        # active_food_events_list = FoodEvent.objects.filter(Q(eventStartDate__lte=today_date) & Q(eventEndDate__gte=today_date), active=False)
        # for food_events in active_food_events_list:
        #     food_events.active = True
        #     food_events.save()
        # expired_food_events_list = FoodEvent.objects.filter(Q(eventStartDate__gt=today_date) | Q(eventEndDate__lt=today_date), active=True)
        # for food_events in expired_food_events_list:
        #     food_events.active = False
        #     food_events.save()
        
        # Mark all ongoing + upcoming events as active
        FoodEvent.objects.filter(
            eventEndDate__gte=today_date,
            active=False
        ).update(active=True)

        # Mark expired events as inactive
        FoodEvent.objects.filter(
            eventEndDate__lt=today_date,
            active=True
        ).update(active=False)
        
        return ({'success': True, 'message': 'Status updated Successfully. Works'})
    except Exception as e:
        return ({'success': False, 'message': str(e)})

@shared_task(name='pending_events_email')
def pending_events_reminder():
    try:
        if FoodEvent.objects.filter(status=STATUS[2][0], active=True).exists():
            pending_event_list = FoodEvent.objects.filter(status=STATUS[2][0], active=True).order_by('-eventStartDate')[:5]
            detailslist = []
            for pending_events in pending_event_list:
                jobdetailscard = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/EventDetailsCard.txt')).read()
                details = jobdetailscard.replace('{{event_name}}',str(pending_events.name)).replace('{{event_food}}',str(pending_events.additionalInfo)).replace('{{event_location}}', str(pending_events.address)).replace('{{event_date}}',str(pending_events.eventStartDate.date())).replace('{{event_time}}',str(pending_events.eventStartDate.time())).replace('{{event_organizer}}', str(pending_events.createdBy.name))
                detailslist.append(details) 
            emaildetailstext = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/emaildetails.html'), mode = 'w')
            emaildetailstext.writelines(detailslist)
            emaildetailstext.close()            
            htmlstring = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/emaildetails.html')).read()
            subject = f'Approval Pending for New Food Events'

            email_from = settings.DEFAULT_SENDER
            recipient_list = ['srao@climatehealers.org','climatehealers@climatehealers.org','padma.chinram@alamanceinc.com', 'raiyan.firaz@alamanceinc.com', 'aravind.muniraj@alamanceinc.com']

            finalhtmlcontent = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/PendingEventsNotification.html')).read()
            email_text = finalhtmlcontent.replace('{{base_url}}', settings.PRODUCTION_URL).replace('{{details}}', htmlstring)
            try:
                msg = EmailMultiAlternatives(subject=subject, body=email_text, from_email=email_from, to=recipient_list)
                msg.attach_alternative(email_text, "text/html")
                msg.send()
                print({'success': True, 'message': 'Message is sent'})
                return ({'success': True, 'message': 'Message is sent'})
            except Exception as e:
                print({'success': False,  'error': str(e)})
                return ({'success': False,  'error': str(e)})

    except Exception as e:
        return ({'success': False, 'error': str(e)})

@shared_task(name='pending_donations_email')
def pending_donations_reminder():
    try:
        if Donation.objects.filter(status=STATUS[2][0], fullfilled=False).exists() or Request.objects.filter(Q(status=STATUS[2][0], fullfilled=False) & Q(type__name = "Food") | Q(type__name="Supplies") ).exists():
            pending_donations_list = Donation.objects.filter(status=STATUS[2][0], fullfilled=False).order_by('-createdAt')[:5]
            detailslist = []
            for pending_donations in pending_donations_list:
                donation_details_card = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/DonationDetailsCard.txt')).read()
                details = donation_details_card.replace('{{food_item}}',str(pending_donations.foodItem.itemName)).replace('{{quantity}}',str(pending_donations.quantity)).replace('{{pickup_address}}', str(pending_donations.delivery.pickupAddress)).replace('{{donation_date}}',str(pending_donations.delivery.pickupDate.date())).replace('{{donation_time}}',str(pending_donations.delivery.pickupDate.time())).replace('{{donated_by}}', str(pending_donations.donatedBy.name))
                detailslist.append(details) 
            emaildetailstext = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/donationEmaildetails.html'), mode = 'w')
            emaildetailstext.writelines(detailslist)
            emaildetailstext.close()            
            htmlstring = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/donationEmaildetails.html')).read()

            pending_request_list = Request.objects.filter(Q(status=STATUS[2][0], fullfilled=False) & Q(type__name = "Food") | Q(type__name="Supplies") ).order_by('-createdAt')[:5]
            request_detailslist = []
            for pending_requests in pending_request_list:
                request_details_card = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/RequestDetailsCard.txt')).read()
                request_details = request_details_card.replace('{{food_item}}',str(pending_requests.foodItem.itemName)).replace('{{quantity}}',str(pending_requests.quantity)).replace('{{pickup_address}}', str(pending_requests.deliver.dropAddress)).replace('{{donation_date}}',str(pending_requests.deliver.dropDate.date())).replace('{{donation_time}}',str(pending_requests.deliver.dropDate.time())).replace('{{donated_by}}', str(pending_requests.createdBy.name))
                request_detailslist.append(request_details) 
            request_emaildetailstext = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/requestEmaildetails.html'), mode = 'w')
            request_emaildetailstext.writelines(request_detailslist)
            request_emaildetailstext.close()            
            request_htmlstring = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/requestEmaildetails.html')).read()

            subject = f'Approval Pending for New Food Donations and Requests'

            email_from = settings.DEFAULT_SENDER
            recipient_list = ['srao@climatehealers.org','climatehealers@climatehealers.org', 'padma.chinram@alamanceinc.com', 'raiyan.firaz@alamanceinc.com', 'aravind.muniraj@alamanceinc.com']

            finalhtmlcontent = open(os.path.join(settings.PROJECT_DIR,'emailTemplates/PendingDonationsNotification.html')).read()
            email_text = finalhtmlcontent.replace('{{base_url}}', settings.PRODUCTION_URL).replace('{{details}}', htmlstring).replace('{{request_details}}', request_htmlstring)
            try:
                msg = EmailMultiAlternatives(subject=subject, body=email_text, from_email=email_from, to=recipient_list)
                msg.attach_alternative(email_text, "text/html")
                msg.send()
                print({'success': True, 'message': 'Message is sent'})
                return ({'success': True, 'message': 'Message is sent'})
            except Exception as e:
                print({'success': False,  'error': str(e)})
                return ({'success': False,  'error': str(e)})

    except Exception as e:
        return ({'success': False, 'error': str(e)})
    
@shared_task(name='checking_active_status')
def active_status_check():
    try:
        today_date = timezone.now()
        
        expired_donations_list = Donation.objects.filter(delivery__pickupDate__lt=today_date, active=True)
        for expired_donation in expired_donations_list:
            expired_donation.active = False
            expired_donation.save()

        expired_request_list = Request.objects.filter( Q(deliver__dropDate__lt=today_date, active=True) & Q( Q(type__name = "Food") | Q(type__name = "Supplies") | Q(type__name = "Pickup")) )
        for expired_requests in expired_request_list:
            expired_requests.active = False
            expired_requests.save()

        expired_volunteer_list = Request.objects.filter( Q(foodEvent__eventEndDate__lt=today_date, active=True) & Q(type__name = "Volunteer"))
        for expired_volunteer in expired_volunteer_list:
            expired_volunteer.active = False
            expired_volunteer.save()

        return ({'success': True, 'message': 'Status Updated Succesfully'})
    except Exception as e:
        return ({'success': False, 'message': str(e)})

