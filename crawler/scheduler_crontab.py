from crontab import CronTab
cron = CronTab(user=True)
job = cron.new(command='cd /Users/jungyulyang/programming/hell-news/crawler && /opt/anaconda3/envs/projects/bin/python articlecrawler_final.py')
job.minute.every(30)
cron.write()
