from json import JSONDecoder


NUM_PARTITIONS = 10


class MonitoringRecord:
    """
    This class embodies both raw monitoring record constructed from a line of comma-separated values and an enriched
    monitoring record, constructed from the json reply of the htm network.
    """
    def __init__(self, line):
        values = line.split(',')
        self.state_code = values[0]
        self.country_code = values[1]
        self.site_num = values[2]
        self.parameter_code = values[3]
        self.poc = values[4]
        self.lat = values[5]
        self.lon = values[6]
        self.datum = values[7]
        self.parameter_name = values[8]
        self.date_local = values[9]
        self.time_local = values[10]
        self.date_gmt = values[11]
        self.time_gmt = values[12]
        self.sample_measurement = values[13]
        self.units_of_measure = values[14]
        self.mdl = values[15]
        self.uncertainty = values[16]
        self.qualifier = values[17]
        self.method_type = values[18]
        self.method_code = values[19]
        self.method_name = values[20]
        self.state_name = values[21]
        self.county_name = values[22]
        self.date_of_last_change = values[23]
        try:
            self.prediction = float(values[24])
            self.error = float(values[25])
            self.anomaly = float(values[26])
            self.prediction_next = float(values[27])
        except IndexError:
            self.prediction = 0.0
            self.error = 0.0
            self.anomaly = 0.0
            self.prediction_next = 0.0

    def to_list(self):
        """This method returns the list representation of the monitoring record.

        :return: list.
        """
        return [self.state_code, self.country_code, self.site_num, self.parameter_code, self.poc, self.lat, self.lon,
                self.datum, self.parameter_name, self.date_local, self.time_local, self.date_gmt, self.time_gmt,
                self.sample_measurement, self.units_of_measure, self.mdl, self.uncertainty, self.qualifier,
                self.method_type, self.method_code, self.method_name, self.state_name, self.county_name,
                self.date_of_last_change, self.prediction, self.error, self.anomaly, self.prediction_next]

    @staticmethod
    def from_json(json_record):
        """This method is used to construct a new MonitoringRecord from some json input.

        :param json_record: Input monitoring record in json format.
        :type json_record: json.
        :return MonitoringRecord.
        """
        decoder = JSONDecoder()
        values = decoder.decode(json_record)
        line = ','.join(map(str, [values['stateCode'], values['countryCode'], values['siteNum'], values['parameterCode'],
                                  values['poc'], values['latitude'], values['longitude'], values['datum'],
                                  values['parameterName'], values['dateLocal'], values['timeLocal'], values['dateGMT'],
                                  values['timeGMT'], values['sampleMeasurement'], values['unitsOfMeasure'], values['mdl'],
                                  values['uncertainty'], values['qualifier'], values['methodType'], values['methodCode'],
                                  values['methodName'], values['stateName'], values['countyName'],
                                  values['dateOfLastChange'], values['prediction'], values['error'], values['anomaly'],
                                  values['predictionNext']]))
        return MonitoringRecord(line)

    def __repr__(self):
        return ','.join(map(str, self.to_list()))

    def get_key(self):
        """This method is used to create a key for the monitoring record, which will define to which of the
        NUM_PARTITIONS partitions this record will go.

        :return: int -- the int number in 0..NUM_PARTITIONS-1
        """
        return '-'.join([self.state_code, self.country_code, self.site_num, self.parameter_code, self.poc])

