import pandas
import datetime
import math
import json

from pymongo import MongoClient, errors
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier

with open('config.json', 'r') as f:
    config = json.load(f)

client = MongoClient(config['host'], config['port'])
db = client[config['database']]


def add_day(start_date, nb_days):
    start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_date = start_date + datetime.timedelta(days=nb_days)
    return end_date.strftime('%Y%m%d')

def torad(degrees):
    return degrees*math.pi/180

def distance(point1, point2):
    lon1 = point1[0]
    lat1 = point1[1]
    lon2 = point2[0]
    lat2 = point2[1]

    R = 6371
    d_lat = torad((lat2 - lat1))
    d_lon = torad((lon2 - lon1))
    lat1 = torad(lat1)
    lat2 = torad(lat2)

    a = math.sin(d_lat/2) * math.sin(d_lat/2) + \
       math.sin(d_lon/2) * math.sin(d_lon/2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def import_collection(collection, **kwargs):
    col = db[collection].find()
    col_df = pandas.DataFrame(list(col))


    if 'remove' in kwargs.keys():
        col_df.drop(kwargs['remove'],
                    axis=1,
                    inplace=True)

    if 'rename' in kwargs.keys():
        col_df.rename(columns = kwargs['rename'],
                        inplace = True)

    return col_df

def import_snow_condtiions(collection_name, to_delete):
    snow_df = import_collection(collection_name,
                                remove=to_delete)

    snow_df['day'] = snow_df['date'].apply(lambda x: x[0:8])
    snow_df['hour'] = snow_df['date'].apply(lambda x: int(x[8:10]))
    snow_df['station_id'] = snow_df['numer_sta'].apply(lambda x: int(str(x)))
    snow_df.drop('numer_sta', axis=1, inplace=True)
    #Il faut un decalage dun jour entre la date de la neige et la sortie car
    #on prevoit a j pour j+1
    snow_df['tomorrow'] = snow_df['day'].apply(lambda x: add_day(x, 1))
    return snow_df

def import_meteo_conditions(collection, to_remove, to_delete):
    meteo_df = import_collection('station',
                                  remove=station_to_remove,
                                  rename=station_rename)

def create_dataset(snow_collection, snow_to_delete,
                  trip_collection, trip_to_delete,
                  meteo_collection, meteo_to_delete,
                  meteo_to_rename, distance_to_station,
                  skiability_to_delete ,date_to_predict):
    snow = import_snow_condtiions(snow_collection, snow_to_delete)
    meteo = import_collection(meteo_collection,
                              remove=meteo_to_delete,
                              rename=meteo_to_rename)
    trips = import_collection(trip_collection,
                              remove=trip_to_delete)

    skiability = import_collection('condition',
                                    remove=skiability_to_delete)

    #assign a station to each trip
    distance_max = distance_to_station
    closest_station = []
    for x in range(len(trips)):
        if (trips.ix[x]['dep_geojson'] == None )or \
        (trips.ix[x]['dep_geojson'] != trips.ix[x]['dep_geojson']):
            closest_station.append(None)
        else:
            trip_gps = trips.ix[x]['dep_geojson']['coordinates'][0:2]
            closest_station_id = None
            dist = distance_max
            for ix in range(len(meteo)):
                station = meteo.ix[ix]
                station_gps = station['station_geojson']['coordinates'][0:2]
                new_distance = distance(trip_gps, station_gps)
                if new_distance < dist:
                    dist = new_distance
                    closest_station_id = station['station_id']

            closest_station.append(closest_station_id)

    trips['closest_station'] = closest_station

    #Join between trip and meteo
    trip_condition = pandas.merge(trips,
                                  meteo,
                                  how='left',
                                  left_on='closest_station',
                                  right_on='station_id')

    trip_condition['daltitude_dep_station'] = trip_condition['dep_altitude'].astype(float) \
                                        - trip_condition['station_altitude'].astype(float)
    trip_condition['daltitude_summit_station'] = trip_condition['altitude'].astype(float) \
                                        - trip_condition['station_altitude'].astype(float)

    #supression des sorties qui n'ont pas de station
    trip_condition = trip_condition[trip_condition.closest_station.notnull()]
    trip_condition['closest_station'] = trip_condition['closest_station'].astype(int)
    trip_condition = trip_condition.drop('station_id', axis=1)

    #merge trip_condition and snow
    train = pandas.merge(trip_condition,
                          snow,
                          left_on='closest_station',
                          right_on='station_id',
                          how='left').reset_index(drop=True)

    training_dataset = train[train.day < date_to_predict ]
    test_dataset = train[train.day == date_to_predict]
    #merge train and skiability
    skiability['date'] = skiability.date.apply(lambda x: x.replace('-',''))
    skiability['nom'] = skiability.nom.apply(lambda x: x.replace('\t', ''))
    print skiability.info()
    print training_dataset.info()
    training_dataset = pandas.merge(skiability, training_dataset,
                    left_on=['nom', 'date'],
                    right_on=['nom', 'tomorrow'],
                    how='inner',
                    suffixes=('_',''))

    return training_dataset, test_dataset

def encode_data(dataframe):
    #encode string columns with label encoder
    label_encoder = {}
    for i in range(len(dataframe.dtypes)):
        column_label = dataframe.dtypes.index[i]
        column_type = dataframe.dtypes[i]
        if column_type == 'object':
            col_value = dataframe.loc[:,column_label]
            label_encoder[column_label] = preprocessing.LabelEncoder().fit(col_value)
            dataframe.loc[:, column_label] = label_encoder[column_label].transform(dataframe.loc[:, column_label])
    return dataframe, label_encoder

def train_rf_classifier(X_train, y_train):
    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)
    return clf

def predict(clf, X_pred, index):
    y_pred = clf.predict(X_pred)
    prediction = [{'trip_id':x,'skiabiliy': y} for x,y in zip(index, y_pred)]
    return pandas.DataFrame.from_dict(prediction)

def send_data(prediction, date, collection):
    trips = import_collection('trip')
    skiablility_db = pandas.merge(trips, prediction,
                                  left_on= 'trip_id',
                                  right_on = 'trip_id')
    date_col = [date for x in range(len(skiablility_db))]
    skiablility_db['date'] = date

    skiability_json = []
    n = len(skiablility_db)
    for ix in range(0,n):
        skiability_json.append(skiablility_db.ix[ix,:].to_dict())
        try:
            db[collection].insert_one(skiablility_db.ix[ix,:].to_dict())
            print 'WARNING: ' + skiablility_db.ix[ix,:].to_dict()['nom'] + 'inserted'
        except errors.DuplicateKeyError:
            print 'WARNING: ' + skiablility_db.ix[ix,:].to_dict()['nom'] + 'already in database for that day'

    print 'INFO'+str(n)
def main():
    snow_to_delete = ['aval_depart', 'aval_descr', 'aval_expo',
                  'aval_genre', 'etat_neige', 'nnuage1',
                  'teneur_eau', '_id']
    meteo_rename = {'Altitude':'station_altitude',
                    'ID':'station_id',
                    'Nom':'station_name'}
    meteo_to_remove = ['_id']
    trip_to_delete = ['_id','dep_name', 'dep_gps',
                       'dep_latlon', 'dep_url',
                       'nb_jours', 'pente',
                       'trip_url']

    skiability_to_delete = ['_id', 'snow_quality_id']

    #distance max to assign a trip to a meteostation
    trip_to_station_dist = 5

    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d')
    train, test = create_dataset('snow', snow_to_delete,
                                 'trip', trip_to_delete,
                                 'station', meteo_to_remove,
                                 meteo_rename, trip_to_station_dist,
                                 skiability_to_delete, yesterday)

    trip_id = test['trip_id']

    train.drop(['nom', 'date', 'day', 'trip_id'],
                        axis=1,
                        inplace=True)
    test.drop(['nom', 'date', 'day', 'trip_id'],
                        axis=1,
                        inplace=True)

    train = train[train.snow_quality.notnull()]
    y = train['snow_quality']
    train = train.drop(['snow_quality','date_'], axis=1 )

    for col in train.columns:
        if train[col].isnull().values.any():
            median = train[col].median()
            if math.isnan(median):
                median = 0
            train[col].fillna(median, inplace=True)

    for col in test.columns:
        if test[col].isnull().values.any():
            median = test[col].median()
            if math.isnan(median):
                median = 0
            test[col].fillna(median, inplace=True)

    X_train, _ = encode_data(train)
    X_train = X_train.as_matrix()
    X_test, _ = encode_data(test)
    X_test = X_test.as_matrix()

    clf = train_rf_classifier(X_train, y)
    pred = predict(clf, X_test, trip_id)
    send_data(pred, datetime.datetime.today().strftime('%Y%m%d'), 'prediction')


if __name__ == '__main__':
    main()
