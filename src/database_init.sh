mysqldump --user=twitterdbuser --password=asdfmin twitter
mysql --user=twitterdbuser --password=asdfmin twitter < database_init.sql
mysql --user=twitterdbuser --password=asdfmin twitter < database_creator.sql
