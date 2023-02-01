# Constants

PRE_INSTALL_PROMISE_TEXT = ''' 
This script will need to be run by an `accountadmin` in your account before 
this tool can run. I promise to maintain a minimal presence on your account 
and to use as few compute resources as possible. If you decide to stop 
using this tool, you can drop the database and warehouse created in this 
script. You may decide to revoke the import privileges as well, but sysadmin 
can make good use of the `SNOWFLAKE` database even without my help.

I hope you enjoy using the Tagging Assistant :)
'''

APP_INFO_TEXT = ''' 
Try these steps:
1. Copy the _**Pre-installation Scripts**_ and run them in your Snowflake 
   account. (Requires `ACCOUNTADMIN` privileges)
2. Log in to Snowflake using the app here
  - The Account URL may require special formatting for regions outside of 
    AWS us-west-2
  - For Accounts in AWS US-East-1, try xyz12345.us-east-1
3. Go to the Warehouses tab, select an existing warehouse and choose "Enable 
   Assistant"
  - There is also a space here to create a new warehouse.
4. Modify the warehouse with whatever settings might be best.
5. Go to the Tags tab, and create some tags. Useful tags might include 
   `department` or `consumer`.
6. Now that tags are created, go to the Apply Tags tab and apply tag 
   values to a warehouse. You might set a warehouse `department` tag 
   value to "Accounting" or something along those lines.
'''

APP_ABOUT_TEXT = ''' 
Welcome to the Warehouse Tagging Assitant! Check out our 
[GitHub Repository](https://github.com/jnschurig/snowflake-assistant).

This tool was designed to help manage Snowflake warehouses. This involves 
creating and modifying existing warehouses, and it also involves creating tags 
and applying those tags to those warehouses. In turn, this will help 
administrators to monitor warehouse credit usage and reduce overall expenditure. 
In addition, this tool can help attribute costs that might otherwise be absorbed 
by a provider who did not incur that cost in the first place.

If there are issues running this app, try doing a hard refresh or using 
the _**Reset Session**_ button under _**Additional Options**_. If issues 
persist when it may have worked in the past, try running the Preinstallation 
script on the Authentication tab. Something may have changed since the last 
time you ran the app!
'''

DEFAULT_ROLE = 'sysadmin'

DEFAULT_WAREHOUSE = 'tagging_assist_wh'

DEFAULT_DATABASE = 'tagging_assist_db'

DEFAULT_WAREHOUSE_SIZE = 'X-Small'

DEFAULT_TIMEZONE = 'America/Los_Angeles'

DEFAULT_CRON = '0 0 * * *' # Daily at midnight

MEMORY_CACHE_MAX_AGE_SECONDS = 600

# 1 second, 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 2 hours, 4 hours, 8 hours, 12 hours, never suspend
WAREHOUSE_AUTO_SUSPEND_STEPS = [1, 60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 0]

WAREHOUSE_SIZES = {
    'X-Small'  : {'code': 'xsmall', 'credit_rate': 1}, 
    'Small'   : {'code': 'small', 'credit_rate': 2}, 
    'Medium'  : {'code': 'medium', 'credit_rate': 4}, 
    'Large'   : {'code': 'large', 'credit_rate': 8}, 
    'X-Large'  : {'code': 'xlarge', 'credit_rate': 16}, 
    '2X-Large' : {'code': 'xxlarge', 'credit_rate': 32}, 
    '3X-Large': {'code': 'xxxlarge', 'credit_rate': 64}, 
    '4X-Large' : {'code': 'x4large', 'credit_rate': 128}, 
    '5X-Large' : {'code': 'x5large', 'credit_rate': 256}, 
    '6X-Large' : {'code': 'x6large', 'credit_rate': 512}, 
    }

def GET_WAREHOUSE_CODE_LIST():
  return_list = []
  for key in WAREHOUSE_SIZES.keys():
    return_list.append(WAREHOUSE_SIZES[key]['code'])
  return return_list

def REVERSE_WAREHOUSE_SIZES():
  return_dict = {}
  for key in WAREHOUSE_SIZES.keys():
    return_dict[WAREHOUSE_SIZES[key]['code']] = key
  return return_dict

COMMENT_MAX_LENGTH = 500

SUGGESTED_VALUE_SQL = '''-- Get a list of existing values for this tag
select nvl(listagg(distinct tag_value, ', ') within group (order by tag_value), '') as suggested_values
  from snowflake.account_usage.tag_references
'''

CRON_HELP_TEXT = ''' 
Cron strings are formatted as 5 parts separated by a space. 
In order, each of the 5 parts refers to: minute, hour, day 
(of the month), month, day (of the week)

For detailed help writing cron strings, try visiting 
[**crontab guru**](https://crontab.guru/)

Strings generated at contab guru can be copied and pasted 
into cron fields here.
'''

TIMEZONE_INPUT_HELP = 'Valid tz string. See [**Valid Timezones**](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)'

DEFAULT_RERUN_WAIT_TIME_SECONDS = 3

MAX_SCHEDULE_COUNT = 10

CONTEXT_FORMAT_COLUMN_COUNT = {
    'standard': 2,
    'wide': 4,
    'narrow': 1,
}

VALID_CONTEXT_TYPES = ['Role', 'Warehouse', 'Database', 'Schema']
