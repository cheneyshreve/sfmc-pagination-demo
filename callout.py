import csv
from datetime import date, datetime
import ET_Client
import json
from suds.sudsobject import asdict


def suds_to_dict(obj):
    """ 
        Description: method formats the suds object into a dictionary to easily extract the Property array
        Parms: suds object
        Returns: dictionary
    """
    if not hasattr(obj, '__keylist__'):
        return obj
    data = {}
    fields = obj.__keylist__
    for field in fields:
        val = getattr(obj, field)
        if isinstance(val, list):
            data[field] = []
            # extract the values for the properties, which will be of type list
            for item in val:
                data[field].append(item['Value'])
        else:
            data[field] = suds_to_dict(val)
    return data


def write_to_csv(batch):
    """
        Description: method to format data into rows and write to csv file
        Params: batch: dictionary
        Returns: None
    """
    data_rows = []
    for obj in batch:
        dictionary = suds_to_dict(obj)
        data_rows.append(dictionary["Properties"]["Property"])
    print('data_rows[0]', data_rows[0])

    now = datetime.now()
    filename = 'sfmc_aggregates_' + \
        str(now.strftime("%d%m%Y%H:%M:%S")) + '.csv'
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(properties)
        for row in data_rows:
            csvwriter.writerow(row)


# Create an instance of the ET_Client class
myClient = ET_Client.ET_Client()
# Crete an instance fo the DataExtension class
de = ET_Client.ET_DataExtension()
# Define the data extension name to be retrieved
NameOfDE = "SubscriberEngagement_AllAggregates"
# Define the properties (fields) to retrieve
properties = [
    'SubscriberKey',
    'SentCount_7Days',
    'SentCount_31Days',
    'SentCount_6Mos',
    'SentCount_12Mos',
    'SentCount_Life',
    'LastSentDateTimeUTC',
    'OpenCount_7Days',
    'OpenCount_31Days',
    'OpenCount_6Mos',
    'OpenCount_12Mos',
    'OpenCount_Life',
    'LastOpenDateTimeUTC',
    'ClickCount_7Days',
    'ClickCount_31Days',
    'ClickCount_6Mos',
    'ClickCount_12Mos',
    'ClickCount_Life',
    'LastClickDateTimeUTC',
    'BounceCount_7Days',
    'BounceCount_31Days',
    'BounceCount_6Mos',
    'BounceCount_12Mos',
    'BounceCount_Life',
    'LastBounceDateTimeUTC',
    'LastUnsubDateTimeUTC',
    'LastModifiedDateTimeUTC'
]
# Retrieve first batch of up to 2500 rows
row = ET_Client.ET_DataExtension_Row()
row.auth_stub = myClient
row.CustomerKey = NameOfDE
row.props = properties
getResponse = row.get()
batch = getResponse.results
write_to_csv(batch)
# Paginate if there are more results
while getResponse.more_results:
    getResponse = row.getMoreResults()
    batch = getResponse.results
    write_to_csv(batch)
