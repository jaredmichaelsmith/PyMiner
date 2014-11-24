PyMiner
=======

#####Description:
Python 2.7.x Client for collecting data from the Twitter Stream, processing the data, and storing in a MySQL database.
#####Version: 
0.0.1

#####Setup Instructions:
- First, clone the repository onto your local machine.
- Setup a MySQL Database with a specific user for that database; this will be used to store the data.
- Modify <i>database_creator.sh</i>, <i>database_init.sh</i>, and <i>start.sh</i> to be executable. Then run <i>database_creator.sh</i> like:

  
      <code>chmod +x database_creator.sh database_init.sh start.sh  </code>
      
      <code> ./database_creator.sh</code>


- Note: You may need to provide <i>sudo</i> before changing the permission of those files, depending on your system.
- Edit the sample_config.config file with your database hostname, username, and password specific to the database created earlier. Also, enter your Twitter API access information, which you can get at <a href="https://dev.twitter.com">the Twitter Developers site</a>.
- Enter either the location boundaries you would like to track tweets within, or specific keywords to track from the stream in the filters parameter of the config file, and specify if you are tracking keywords or locations by entering either <i>keyword</i> or <i>location</i> for the filter_type parameter.
- Run <i>start.sh</i>, which will create a screen for the process, and switch to the screen to see the tweets coming in like:

      <code>./start.sh</code>
  
      <code>screen -R TwitterMiner</code>

- From here, sit back and watch as the tweets come in and are stored in the database. For visualization purposes, I have phpMyAdmin set up on my machine, and I keep track of the database from there.
